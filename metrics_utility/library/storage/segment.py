import datetime
import hashlib
import json
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
    # Max JSON size of each `data` chunk. Segment enforces ~32KB per `track` message
    # including `properties` wrapper, event name, and segment_meta; keep this conservative.
    REGULAR_MESSAGE_LIMIT = 24 * 1024

    def __init__(self, **settings):
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')

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

        Always splits by top-level keys - each top-level key gets its own chunk(s).
        If a top-level key's value is a list, it is split in order: the next item is
        considered appended to the current chunk; if ``json.dumps`` of that chunk
        would exceed max_size, the current chunk is finalized and a new one is started
        (or a single oversize item is emitted alone).

        Args:
            data: Dictionary to split, dictionary contains key : value pairs
            Those key value pairs are either dicts or list
            only lists are split into chunks, dicts are not split, thus dicts can not
            be larger than max_size
            max_size: Maximum size in bytes for each chunk (JSON of top-level {key: ...})

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
                    trial = {key: active_chunk[key] + [item]}
                    if self._calculate_size(trial) > max_size:
                        if len(active_chunk[key]) > 0:
                            chunks.append(active_chunk)
                            active_chunk = {key: [item]}
                        else:
                            # single item does not fit max_size; emit it alone
                            chunks.append({key: [item]})
                    else:
                        active_chunk[key].append(item)

                if len(active_chunk[key]) > 0:
                    chunks.append(active_chunk)

        return chunks if chunks else [data]

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None, event_name=None, segment_meta=None):
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
        multiple apps. Data is split so each `data` chunk is under
        :attr:`REGULAR_MESSAGE_LIMIT` (JSON bytes), with headroom for Segment's
        per-message size limit.
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

        max_size = self.REGULAR_MESSAGE_LIMIT
        chunks = self._split_into_chunks(dict, max_size)

        total_chunks = len(chunks)

        if self.debug:
            msg = f'Split data into {total_chunks} chunks'
            print(msg, file=sys.stderr)

        if not segment_meta:
            segment_meta = {}
        message_id = segment_meta.get('message_id', None)

        # Send each chunk
        for i, chunk in enumerate(chunks, 1):
            chunk_size = self._calculate_size(chunk)

            # chunk hash = sha256(message hash + chunk index)
            if message_id:
                segment_meta['message_id'] = hashlib.sha256(f'{message_id}_{i}'.encode('utf-8', errors='replace')).hexdigest()

            if self.debug:
                msg = f'Sending chunk {i}/{total_chunks} (size: {chunk_size} bytes)'
                if message_id:
                    msg += f'; message_id={segment_meta["message_id"]}'
                print(msg, file=sys.stderr)

            analytics.track(
                anonymous_id=f'{anonymous_id}_{i}',
                event=event_name,
                properties={
                    'artifact_name': artifact_name,
                    'data': chunk,
                    'upload_timestamp': (datetime.datetime.now(tz=datetime.timezone.utc).isoformat()),
                    'chunk_info': {
                        'chunk_number': i,
                        'total_chunks': total_chunks,
                        'chunk_size': chunk_size,
                    },
                },
                **segment_meta,
            )
            analytics.flush()

            # time.sleep(3) - uncomment as backup plan

        return chunks
