import datetime
import logging
import os

from dateutil import parser
from django.core.management.base import BaseCommand
from django.utils import timezone

from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.exceptions import (
    BadRequiredEnvVar,
    BadShipTarget,
    FailedToUploadPayload,
    MissingRequiredEnvVar,
    NoAnalyticsCollected,
    UnparsableParameter,
)
from metrics_utility.management.validation import (
    handle_crc_ship_target,
    handle_directory_ship_target,
    handle_env_validation,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    handle_validate_date_param,
)


class Command(BaseCommand):
    """
    Gather Automation Controller billing data
    """

    def __init__(self):
        super().__init__()
        self.help = 'Gather Automation Controller billing data.'
        self.help_texts = {
            'since': (
                'Start date for collection including (e.g. --since=2023-12-20), a number of days ago (--since=5d), '
                'or a number of months (--since=2m).'
            ),
            'until': (
                'End date for collection including (e.g. --until=2023-12-21), a number of days ago (--until=5d), or a number of months (--until=2m).'
            ),
        }

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', dest='dry-run', action='store_true', help='Gather billing metrics without shipping.')
        parser.add_argument('--ship', dest='ship', action='store_true', help='Enable shipping of billing metrics to the console.redhat.com')

        parser.add_argument(
            '--since',
            dest='since',
            action='store',
            help=self.help_texts.get('since'),
        )
        parser.add_argument(
            '--until',
            dest='until',
            action='store',
            help=self.help_texts.get('until'),
        )

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def handle(self, *args, **options):
        try:
            self._handle(self, *args, **options)
            exit(0)
        except (BadShipTarget, MissingRequiredEnvVar, BadRequiredEnvVar, FailedToUploadPayload, UnparsableParameter) as e:
            self.logger.error(e.name)
            exit(1)
        except Exception as e:
            self.logger.exception(e)
            exit(1)

    def _handle(self, *args, **options):
        self.init_logging()
        handle_env_validation('gather')

        handle_validate_date_param(options.get('since', None), self.help_texts.get('since'), 'gather')
        handle_validate_date_param(options.get('until', None), self.help_texts.get('until'), 'gather')

        opt_ship = options.get('ship')
        opt_dry_run = options.get('dry-run')
        opt_since = options.get('since') or None
        opt_until = options.get('until') or None

        since, until = self._handle_interval(opt_since, opt_until)

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        billing_provider_params = self._handle_ship_target(ship_target)

        if opt_ship and opt_dry_run:
            self.logger.error('Arguments --ship and --dry-run cannot be processed at the same time, set only one of these.')
            return

        collector = Collector(
            collection_type=Collector.MANUAL_COLLECTION if opt_ship else Collector.DRY_RUN,
            ship_target=ship_target,
            billing_provider_params=billing_provider_params,
        )

        tgzfiles = collector.gather(since=since, until=until, billing_provider_params=billing_provider_params)
        if tgzfiles:
            for tgz in tgzfiles:
                self.logger.info(tgz)
        else:
            self.logger.error('No analytics collected')
            raise NoAnalyticsCollected('No analytics collected')

    def _handle_ship_target(self, ship_target):
        if ship_target == 'crc':
            handle_not_s3()
            return handle_crc_ship_target()
        elif ship_target == 'directory':
            handle_not_crc()
            handle_not_s3()
            return handle_directory_ship_target()
        elif ship_target == 's3':
            handle_not_crc()
            return handle_s3_ship_target()
        else:
            allowed = ', '.join(['crc', 'directory', 's3'])
            raise BadShipTarget(f'Unexpected value for METRICS_UTILITY_SHIP_TARGET env var ({ship_target}), allowed values: {allowed}')

    def _handle_datelike(self, value, help=''):
        if not value:
            return None
        # # Process ret argument
        if value.endswith('d'):
            days_ago = int(value[0:-1])
            ret = (datetime.datetime.now() - datetime.timedelta(days=days_ago - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif value.endswith('m'):
            minutes_ago = int(value[0:-1])
            ret = datetime.datetime.now() - datetime.timedelta(minutes=minutes_ago)
        else:
            ret = parser.parse(value)

        # Add default utc timezone
        if ret and ret.tzinfo is None:
            ret = ret.replace(tzinfo=timezone.utc)

        return ret

    def _handle_interval(self, opt_since, opt_until):
        # Process since argument
        since = self._handle_datelike(opt_since, help=self.help_texts.get('since'))

        # Process until argument
        until = self._handle_datelike(opt_until, help=self.help_texts.get('until'))
        return since, until
