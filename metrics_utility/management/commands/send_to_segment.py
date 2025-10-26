import csv
import datetime
import json
import os

from argparse import RawDescriptionHelpFormatter
from pathlib import Path

import segment.analytics as analytics

from django.core.management.base import BaseCommand, CommandError

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.logger import debug, logger


class Command(BaseCommand):
    """
    Send data to Segment.com
    """

    help = 'Send CSV or data file to Segment.com as a custom event'
    help_texts = {
        'file': 'Path to the CSV or data file to send to Segment',
        'app': ('Application identifier (e.g., ansible-automation-platform, awx)'),
        'user-id': 'User ID for the Segment event (defaults to "system")',
        'verbose': 'Print debug information to console.',
    }

    def create_parser(self, prog_name, subcommand, **kwargs):
        return super().create_parser(
            prog_name,
            subcommand,
            # ensure newlines are preserved in descriptions and epilog
            formatter_class=RawDescriptionHelpFormatter,
            epilog='\n'.join(
                [
                    'ENVIRONMENT',
                    '',
                    '  Required Configuration:',
                    '    SEGMENT_WRITE_KEY (required): Segment write key for authentication',
                    '',
                    'EXAMPLES',
                    '',
                    '  Send a CSV file:',
                    '    $ metrics-utility send_to_segment --file data.csv --app ansible-automation-platform',
                    '',
                    '  Send with custom user ID:',
                    '    $ metrics-utility send_to_segment --file metrics.csv --app awx --user-id user123',
                    '',
                ]
            ),
            **kwargs,
        )

    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            dest='file',
            action='store',
            required=True,
            help=self.help_texts.get('file'),
        )
        parser.add_argument(
            '--app',
            dest='app',
            action='store',
            required=True,
            help=self.help_texts.get('app'),
        )
        parser.add_argument(
            '--user-id',
            dest='user_id',
            action='store',
            default='system',
            help=self.help_texts.get('user-id'),
        )
        parser.add_argument(
            '--verbose',
            dest='verbose',
            action='store_true',
            help=self.help_texts.get('verbose'),
        )

    def handle(self, *args, **options):
        if options.get('verbose'):
            debug()

        # Validate environment variables
        segment_write_key = os.getenv('SEGMENT_WRITE_KEY')
        if not segment_write_key:
            raise MissingRequiredEnvVar('Missing required env variable SEGMENT_WRITE_KEY.')

        # Get command options
        file_path = options.get('file')
        app_name = options.get('app')
        user_id = options.get('user_id')

        # Validate file exists
        if not file_path:
            raise CommandError('--file argument is required')
        if not app_name:
            raise CommandError('--app argument is required')

        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise CommandError(f'File not found: {file_path}')
        if not file_path_obj.is_file():
            raise CommandError(f'Path is not a file: {file_path}')

        logger.info(f'Reading file: {file_path}')

        # Read and parse file
        try:
            file_data = self._read_file(file_path_obj)
        except Exception as e:
            raise CommandError(f'Error reading file: {str(e)}')

        # Configure Segment SDK
        analytics.write_key = segment_write_key
        analytics.debug = options.get('verbose', False)

        # Prepare event data
        event_name = f'{app_name}_data_upload'
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

        properties = {
            'filename': file_path_obj.name,
            'file_path': str(file_path),
            'data': file_data['data'],
            'row_count': file_data['row_count'],
            'file_type': file_data['file_type'],
            'timestamp': timestamp,
        }

        context = {'app': {'name': app_name}}

        # Send to Segment
        logger.info(f'Sending data to Segment: event={event_name}, user_id={user_id}, rows={file_data["row_count"]}')

        try:
            analytics.track(
                user_id=user_id,
                event=event_name,
                properties=properties,
                context=context,
            )

            # Flush to ensure the event is sent before the script exits
            analytics.flush()

            logger.info(f'Successfully sent data to Segment: {file_data["row_count"]} rows from {file_path_obj.name} as event "{event_name}"')

        except Exception as e:
            raise CommandError(f'Error sending data to Segment: {str(e)}')

    def _read_file(self, file_path: Path) -> dict:
        """
        Read and parse file content. Supports CSV, JSON, and text files.

        Returns a dictionary with:
        - data: parsed file content
        - row_count: number of rows/items
        - file_type: detected file type
        """
        file_extension = file_path.suffix.lower()

        if file_extension == '.csv':
            return self._read_csv(file_path)
        elif file_extension == '.json':
            return self._read_json(file_path)
        else:
            # Attempt to parse as CSV first, fall back to text
            try:
                return self._read_csv(file_path)
            except (csv.Error, UnicodeDecodeError):
                return self._read_text(file_path)

    def _read_csv(self, file_path: Path) -> dict:
        """Read and parse CSV file into a list of dictionaries."""
        data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(dict(row))

        return {'data': data, 'row_count': len(data), 'file_type': 'csv'}

    def _read_json(self, file_path: Path) -> dict:
        """Read and parse JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Handle both list and single object
        if isinstance(data, list):
            row_count = len(data)
        elif isinstance(data, dict):
            row_count = 1
            data = [data]
        else:
            row_count = 1
            data = [{'value': data}]

        return {'data': data, 'row_count': row_count, 'file_type': 'json'}

    def _read_text(self, file_path: Path) -> dict:
        """Read plain text file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        lines = content.strip().split('\n')

        return {
            'data': {'content': content, 'lines': lines},
            'row_count': len(lines),
            'file_type': 'text',
        }
