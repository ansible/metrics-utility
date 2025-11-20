import datetime
import json
import random
import sys
import uuid

from metrics_utility.logger import logger


try:
    import segment.analytics as analytics

    SEGMENT_AVAILABLE = True
except ImportError:
    analytics = None
    SEGMENT_AVAILABLE = False


class StorageSegment:
    # Size limits for Segment messages
    # 32KB for regular messages, but leave some room for the metadata
    REGULAR_MESSAGE_LIMIT = 28 * 1024
    # 512MB for bulk messages, but leave some room for the metadata
    BULK_MESSAGE_LIMIT = 500 * 1024 * 1024

    def __init__(self, **settings):
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')
        self.use_bulk = settings.get('use_bulk', False)

        if not SEGMENT_AVAILABLE:
            logger.info('StorageSegment: segment module not installed. Analytics will be disabled.')

        if not self.write_key:
            logger.info('StorageSegment: write_key not set. Analytics will be disabled.')

    def _calculate_size(self, data):
        """Calculate the size of data in bytes."""
        return len(json.dumps(data).encode('utf-8'))

    def _split_into_chunks(self, data, max_size):
        """
        Split data into chunks based on max_size.

        Args:
            data: Dictionary to split, dictionary contains key : value pairs
            Those key value pairs are either dicts or list
            only lists are split into chunks, dicts are not split, thus dicts can not
            be larger than max_size
            max_size: Maximum size in bytes for each chunk

        Returns:
            List of data chunks
        """
        chunks = []

        if data is not None and not isinstance(data, dict):
            msg = f'Data is not a dictionary, got {type(data).__name__}'
            raise Exception(msg)

        for key, value in data.items():
            if isinstance(value, dict):
                # always add to chunks, each key in main dict is a separate chunk
                chunks.append({key: value})

            elif isinstance(value, list):
                active_chunk = {key: []}

                for item in value:
                    active_chunk_size = self._calculate_size(active_chunk)
                    item_size = self._calculate_size(item)
                    if active_chunk_size + item_size > max_size:
                        chunks.append(active_chunk)
                        active_chunk = {key: [item]}
                    else:
                        active_chunk[key].append(item)

                if len(active_chunk[key]) > 0:
                    chunks.append(active_chunk)

        return chunks if chunks else [data]

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None, event_name=None):
        """
        Send data to Segment, splitting into chunks if necessary.

        Args:
            artifact_name: Name of the artifact being sent
            filename: Not supported (raises exception)
            fileobj: Not supported (raises exception)
            dict: Dictionary or list of data to send
            event_name: Name of the event to track
                       (defaults to 'Metrics Artifact Upload')

        This method supports sending anonymized analytics from
        multiple apps. Data will be automatically split into chunks
        based on Segment's size limits:
        - 32KB for regular messages
        - 512MB for bulk messages (when use_bulk=True)
        """
        chunks = []
        if filename or fileobj or dict is None:
            msg = 'StorageSegment: filename= & fileobj= not supported, use dict='
            raise Exception(msg)

        # Check if segment is available and configured
        if not SEGMENT_AVAILABLE:
            if self.debug:
                logger.debug('Segment not available, skipping analytics upload for: %s', artifact_name)
            return

        if not self.write_key:
            if self.debug:
                logger.debug('Segment write_key not set, skipping analytics upload for: %s', artifact_name)
            return

        # Default event name
        if event_name is None:
            event_name = 'Metrics Artifact Upload'

        # Generate a random anonymous ID for this send
        anonymous_id = str(uuid.uuid4())

        # Configure Segment client
        analytics.write_key = self.write_key
        analytics.debug = self.debug

        # Determine size limit based on bulk mode
        max_size = self.BULK_MESSAGE_LIMIT if self.use_bulk else self.REGULAR_MESSAGE_LIMIT

        chunks = self._split_into_chunks(dict, max_size)
        total_chunks = len(chunks)

        print(f'Total chunks: {total_chunks}', file=sys.stderr)

        if self.debug:
            msg = f'Split data into {total_chunks} chunks'
            print(msg, file=sys.stderr)

        # generate random readable name
        choice1 = [
            'amazing',
            'wonderful',
            'fantastic',
            'incredible',
            'magnificent',
            'spectacular',
            'awesome',
            'terrific',
            'magnificent',
            'spectacular',
            'awesome',
            'terrific',
            'brilliant',
            'remarkable',
            'extraordinary',
            'marvelous',
            'stellar',
            'exceptional',
            'impressive',
            'phenomenal',
            'outstanding',
            'radiant',
            'glorious',
            'superb',
        ]

        choice2 = [
            'quiet',
            'anonymized',
            'secret',
            'hidden',
            'private',
            'secure',
            'confidential',
            'hidden',
            'secret',
            'confidential',
            'stealthy',
            'discreet',
            'low-profile',
            'shielded',
            'veiled',
            'masked',
            'encrypted',
            'obscured',
            'camouflaged',
            'protected',
            'secluded',
            'isolated',
        ]

        choice3 = [
            'apple',
            'banana',
            'cherry',
            'date',
            'elderberry',
            'fig',
            'grape',
            'honeydew',
            'kiwi',
            'lemon',
            'mango',
            'nectarine',
            'orange',
            'papaya',
            'quince',
            'raspberry',
            'strawberry',
            'tangerine',
            'watermelon',
            'blueberry',
            'blackberry',
            'pineapple',
        ]

        choice4 = [
            'analysis',
            'report',
            'summary',
            'metrics',
            'data',
            'information',
            'insights',
            'reports',
            'analytics',
            'stats',
            'overview',
            'breakdown',
            'dashboard',
            'evaluation',
            'assessment',
            'briefing',
            'log',
            'record',
            'dataset',
            'profile',
            'observations',
            'review',
        ]

        random_name = f'{random.choice(choice1)}_{random.choice(choice2)}_{random.choice(choice3)}_{random.choice(choice4)}'

        # Send each chunk
        for i, chunk in enumerate(chunks, 1):
            chunk_size = self._calculate_size(chunk)
            if self.debug:
                msg = f'Sending chunk {i}/{total_chunks} (size: {chunk_size} bytes)'
                print(msg, file=sys.stderr)

            analytics.track(
                anonymous_id=anonymous_id,
                event=event_name,
                properties={
                    'artifact_name': artifact_name,
                    'collection_id': random_name,
                    'data': chunk,
                    'upload_timestamp': (datetime.datetime.now(tz=datetime.timezone.utc).isoformat()),
                    'chunk_info': {
                        'chunk_number': i,
                        'total_chunks': total_chunks,
                        'chunk_size': chunk_size,
                    },
                },
            )

        # Flush to ensure all events are sent
        analytics.flush()

        return chunks
