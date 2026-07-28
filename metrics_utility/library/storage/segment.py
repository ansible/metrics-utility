"""Storage backend that ships anonymized analytics to Segment."""

import datetime
import hashlib
import json
import sys
import uuid

from metrics_utility.logger import logger


try:
    from segment import analytics

    SEGMENT_AVAILABLE = True
except ImportError:
    analytics = None
    SEGMENT_AVAILABLE = False


class StorageSegment:
    """Segment analytics storage backend.

    Sends anonymized artifact data as ``track`` events, automatically
    splitting large payloads into multiple messages that stay under Segment's
    per-message size limit.
    """

    # Total budget for each Segment track message (JSON bytes). The SDK enforces a
    # hard 32KB limit; in the `put` in this file, we subtract the header (and all
    # other properties in the packet) from this number, and chunk accordingly
    REGULAR_MESSAGE_LIMIT = 32 * 1024

    def __init__(self, **settings):
        """Initialise the Segment storage backend.

        Args:
            **settings: Accepts ``'debug'`` (bool), ``'user_id'`` (str), and
                ``'write_key'`` (str, required for actual uploads).
        """
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')
        self.host = settings.get('host')

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
        (or a single oversize item is emitted alone with a warning).

        Args:
            data: Dictionary to split, dictionary contains key : value pairs
            Those key value pairs are either dicts or list
            only lists are split into chunks, dicts are not split, thus dicts can not
            be larger than max_size
            max_size: Maximum size in bytes for each chunk (JSON of top-level {key: ...})

        Returns:
            List of data chunks

        Raises:
            ValueError: If max_size is not positive.
        """
        if max_size <= 0:
            msg = f'max_size must be positive, got {max_size}'
            raise ValueError(msg)

        chunks = []

        if data is not None and not isinstance(data, dict):
            msg = f'Data is not a dictionary, got {type(data).__name__}'
            raise Exception(msg)

        for key, value in data.items():
            if isinstance(value, dict):
                chunk = {key: value}
                chunk_size = self._calculate_size(chunk)
                if chunk_size > max_size:
                    logger.warning('Oversized dict chunk for key %r: %d bytes exceeds %d limit', key, chunk_size, max_size)
                chunks.append(chunk)

            elif isinstance(value, list):
                active_chunk = {key: []}

                for item in value:
                    trial = {key: active_chunk[key] + [item]}
                    if self._calculate_size(trial) > max_size:
                        if len(active_chunk[key]) > 0:
                            chunks.append(active_chunk)
                            active_chunk = {key: [item]}
                        else:
                            logger.warning('Single list item in key %r exceeds %d byte limit', key, max_size)
                            chunks.append({key: [item]})
                    else:
                        active_chunk[key].append(item)

                if len(active_chunk[key]) > 0:
                    chunks.append(active_chunk)

        return chunks or [data]

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
        # sync_mode makes each track() a blocking HTTP request instead of queuing to a
        # background thread. Without it the SDK batches all chunks into one POST which
        # can silently exceed Segment's 500 KB batch limit and drop events, returning
        # HTTP 200 with no error callback fired.
        analytics.sync_mode = True
        # Allow redirecting to a mock server via the host= kwarg.
        # Setting to None restores the SDK default (https://api.segment.io).
        analytics.host = self.host or None

        if not segment_meta:
            segment_meta = {}
        message_id = segment_meta.get('message_id')

        header = {
            'anonymousId': anonymous_id,
            'type': 'track',
            'event': event_name,
            'messageId': 'a' * 64 if message_id else str(uuid.uuid4()),
            'timestamp': datetime.datetime.now(tz=datetime.UTC).isoformat(),
            'integrations': segment_meta.get('integrations', {}),
            'context': segment_meta.get('context', {}),
            'properties': {
                'artifact_name': artifact_name,
                'data': {},
                'upload_timestamp': datetime.datetime.now(tz=datetime.UTC).isoformat(),
                'chunk_info': {'chunk_number': 0, 'total_chunks': 0, 'chunk_size': 0},
            },
        }
        overhead = self._calculate_size(header)
        max_size = self.REGULAR_MESSAGE_LIMIT - overhead
        chunks = self._split_into_chunks(dict, max_size)

        total_chunks = len(chunks)

        if self.debug:
            msg = f'Split data into {total_chunks} chunks'
            print(msg, file=sys.stderr)

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
                anonymous_id=anonymous_id,
                event=event_name,
                properties={
                    'artifact_name': artifact_name,
                    'data': chunk,
                    'upload_timestamp': (datetime.datetime.now(tz=datetime.UTC).isoformat()),
                    'chunk_info': {
                        'chunk_number': i,
                        'total_chunks': total_chunks,
                        'chunk_size': chunk_size,
                    },
                },
                **segment_meta,
            )

        # Flush to ensure all events are sent
        analytics.flush()

        return chunks
