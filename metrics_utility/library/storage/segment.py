"""Storage backend that ships anonymized analytics to Segment."""

import datetime
import gzip
import hashlib
import json
import sys
import uuid

import requests

from metrics_utility.logger import logger


# Kept as a compatibility marker for metrics-service callers. Segment support
# now uses the direct HTTP transport and no longer depends on the SDK.
SEGMENT_AVAILABLE = True


class StorageSegment:
    """Segment analytics storage backend.

    Sends anonymized artifact data as ``track`` events, automatically
    splitting large payloads into multiple messages that stay under Segment's
    per-message size limit.
    """

    # Total budget for each Segment track message (JSON bytes).
    REGULAR_MESSAGE_LIMIT = 32 * 1024
    # Segment's batch limit is below 500 KB. Leave room for the request envelope.
    BATCH_SIZE_LIMIT = 475_000
    REQUEST_TIMEOUT = 30
    SEGMENT_BATCH_PATH = '/v1/batch'

    def __init__(self, **settings):
        """Initialise the Segment storage backend.

        Args:
            **settings: Accepts ``'debug'`` (bool), ``'user_id'`` (str),
                ``'write_key'`` (str, required for actual uploads),
                ``'host'`` (str, optional base URL override),
                and ``'gzip'`` (bool, default True).
        """
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')
        self.host = settings.get('host')
        self.gzip = settings.get('gzip', True)

        if not self.write_key:
            logger.info('StorageSegment: write_key not set. Analytics will be disabled.')

    def _build_properties(self, artifact_name, data, chunk_number, total_chunks, chunk_size):
        return {
            'artifact_name': artifact_name,
            'data': data,
            'upload_timestamp': datetime.datetime.now(tz=datetime.UTC).isoformat(),
            'chunk_info': {
                'chunk_number': chunk_number,
                'total_chunks': total_chunks,
                'chunk_size': chunk_size,
            },
        }

    def _calculate_size(self, data):
        """Calculate the size of data in bytes."""
        return len(json.dumps(data).encode('utf-8'))

    @staticmethod
    def _json_bytes(data):
        return json.dumps(data, separators=(',', ':')).encode('utf-8')

    @staticmethod
    def _serialize_value(value):
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        return value

    def _build_event(self, artifact_name, event_name, anonymous_id, chunk, chunk_number, total_chunks, segment_meta):
        chunk_size = self._calculate_size(chunk)
        base_message_id = segment_meta.get('message_id')
        if base_message_id:
            message_id = hashlib.sha256(f'{base_message_id}_{chunk_number}'.encode('utf-8', errors='replace')).hexdigest()
        else:
            message_id = str(uuid.uuid4())

        event = {
            'type': 'track',
            'anonymousId': anonymous_id,
            'messageId': message_id,
            'event': event_name,
            'timestamp': self._serialize_value(segment_meta.get('timestamp', datetime.datetime.now(tz=datetime.UTC))),
            'context': segment_meta.get('context', {}),
            'integrations': segment_meta.get('integrations', {}),
            'properties': self._build_properties(artifact_name, chunk, chunk_number, total_chunks, chunk_size),
        }
        if 'user_id' in segment_meta:
            event['userId'] = segment_meta['user_id']
        return event

    def _split_into_batches(self, events, sent_at):
        """Group events into request bodies below Segment's batch-size limit."""
        batches = []
        active_batch = []

        for event in events:
            candidate = [*active_batch, event]
            payload = {'batch': candidate, 'sentAt': sent_at}
            if len(self._json_bytes(payload)) > self.BATCH_SIZE_LIMIT:
                if active_batch:
                    batches.append(active_batch)
                    active_batch = []

                    singleton_payload = {'batch': [event], 'sentAt': sent_at}
                    if len(self._json_bytes(singleton_payload)) > self.BATCH_SIZE_LIMIT:
                        msg = f'Single Segment event exceeds the {self.BATCH_SIZE_LIMIT}-byte batch limit'
                        raise ValueError(msg)
                    active_batch = [event]
                else:
                    msg = f'Single Segment event exceeds the {self.BATCH_SIZE_LIMIT}-byte batch limit'
                    raise ValueError(msg)
            else:
                active_batch = candidate

        if active_batch:
            batches.append(active_batch)
        return batches

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

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None, event_name=None, segment_meta=None, anonymous_id=None):
        """
        Send data to Segment, splitting into chunks if necessary.

        Args:
            artifact_name: Name of the artifact being sent
            filename: Not supported (raises exception)
            fileobj: Not supported (raises exception)
            dict: Dictionary or list of data to send
            event_name: Name of the event to track
                       (defaults to 'Metrics Artifact Upload')
            anonymous_id: Optional anonymized ID to reuse across related sends.
                          A random UUID is generated when omitted.

        This method supports sending anonymized analytics from
        multiple apps. Data is split so each `data` chunk is under
        :attr:`REGULAR_MESSAGE_LIMIT` (JSON bytes), with headroom for Segment's
        per-message size limit.
        """
        chunks = []
        if filename or fileobj or dict is None:
            msg = 'StorageSegment: filename= & fileobj= not supported, use dict='
            raise Exception(msg)

        if not self.write_key:
            if self.debug:
                logger.debug('Segment write_key not set, skipping analytics upload for: %s', artifact_name)
            return

        # Default event name
        if event_name is None:
            event_name = 'Metrics Artifact Upload'

        if not segment_meta:
            segment_meta = {}

        if anonymous_id is None:
            anonymous_id = str(uuid.uuid4())
        chunks = self._split_into_chunks(dict, self.REGULAR_MESSAGE_LIMIT)

        total_chunks = len(chunks)

        if self.debug:
            msg = f'Split data into {total_chunks} chunks'
            print(msg, file=sys.stderr)

        events = [
            self._build_event(artifact_name, event_name, anonymous_id, chunk, i, total_chunks, segment_meta) for i, chunk in enumerate(chunks, 1)
        ]
        sent_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
        batches = self._split_into_batches(events, sent_at)
        endpoint = f'{(self.host or "https://api.segment.io").rstrip("/")}{self.SEGMENT_BATCH_PATH}'

        for batch_number, batch in enumerate(batches, 1):
            payload = {'batch': batch, 'sentAt': sent_at}
            body = self._json_bytes(payload)
            wire_body = gzip.compress(body) if self.gzip else body
            headers = {'Content-Type': 'application/json'}
            if self.gzip:
                headers['Content-Encoding'] = 'gzip'

            response = None
            try:
                response = requests.post(
                    endpoint,
                    data=wire_body,
                    headers=headers,
                    auth=(self.write_key, ''),
                    timeout=self.REQUEST_TIMEOUT,
                )
                response.raise_for_status()
            except requests.RequestException as error:
                if self.debug:
                    logger.debug(
                        'Segment batch %d/%d failed: status=%s, response=%s, error=%s',
                        batch_number,
                        len(batches),
                        response.status_code if response is not None else None,
                        response.text if response is not None else None,
                        error,
                    )
                raise

            if self.debug:
                logger.debug(
                    'Segment batch %d/%d: events=%d, json_bytes=%d, wire_bytes=%d, status=%d, response=%s',
                    batch_number,
                    len(batches),
                    len(batch),
                    len(body),
                    len(wire_body),
                    response.status_code,
                    response.text,
                )

        return chunks
