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
        """Build the Segment properties object for one artifact chunk."""
        data = self._serialize_value(data)
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
        return len(json.dumps(self._serialize_value(data)).encode('utf-8'))

    @staticmethod
    def _json_bytes(data):
        """Serialize a payload using compact JSON and UTF-8 bytes."""
        return json.dumps(data, separators=(',', ':')).encode('utf-8')

    @staticmethod
    def _serialize_value(value):
        """Convert date and datetime values to Segment-compatible strings."""
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {key: StorageSegment._serialize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [StorageSegment._serialize_value(item) for item in value]
        if isinstance(value, tuple):
            return tuple(StorageSegment._serialize_value(item) for item in value)
        return value

    def _build_event(self, artifact_name, event_name, anonymous_id, chunk, chunk_number, total_chunks, segment_meta):
        """Build one deterministic Segment track event from an artifact chunk."""
        chunk = self._serialize_value(chunk)
        chunk_size = self._calculate_size(chunk)
        timestamp = segment_meta.get('timestamp')
        base_message_id = segment_meta.get('message_id') or f'{artifact_name}:{event_name}:{anonymous_id}:{timestamp}'
        message_id = hashlib.sha256(f'{base_message_id}_{chunk_number}'.encode('utf-8', errors='replace')).hexdigest()

        event = {
            'type': 'track',
            'anonymousId': anonymous_id,
            'messageId': message_id,
            'event': event_name,
            'timestamp': self._serialize_value(segment_meta.get('timestamp', datetime.datetime.now(tz=datetime.UTC))),
            'context': self._serialize_value(segment_meta.get('context', {})),
            'integrations': self._serialize_value(segment_meta.get('integrations', {})),
            'properties': self._build_properties(artifact_name, chunk, chunk_number, total_chunks, chunk_size),
        }
        if 'user_id' in segment_meta:
            event['userId'] = segment_meta['user_id']
        return event

    def _split_into_batches(self, events, sent_at):
        """Group events into request bodies below Segment's batch-size limit."""
        batches = []
        active_batch = []
        batch_base_size = len(self._json_bytes({'batch': [], 'sentAt': sent_at})) - 2
        active_size = batch_base_size

        for event in events:
            event_size = len(self._json_bytes(event))
            candidate_size = active_size + event_size + bool(active_batch)
            if candidate_size > self.BATCH_SIZE_LIMIT:
                if active_batch:
                    batches.append(active_batch)
                    active_batch = [event]
                    active_size = batch_base_size + event_size
                    if active_size > self.BATCH_SIZE_LIMIT:
                        msg = f'Single Segment event exceeds the {self.BATCH_SIZE_LIMIT}-byte batch limit'
                        raise ValueError(msg)
                else:
                    msg = f'Single Segment event exceeds the {self.BATCH_SIZE_LIMIT}-byte batch limit'
                    raise ValueError(msg)
            else:
                active_batch.append(event)
                active_size = candidate_size

        if active_batch:
            batches.append(active_batch)
        return batches

    def _split_into_chunks(self, data, max_size):
        """Split an artifact into top-level chunks below the requested size.

        Lists are split in order when appending another item would exceed the
        limit. Dictionaries and individual oversized list items are preserved
        as single chunks and reported through the logger.

        Args:
            data: Dictionary containing the artifact data to split.
            max_size: Maximum JSON size in bytes for a regular chunk.

        Returns:
            The ordered list of artifact chunks.

        Raises:
            ValueError: If ``max_size`` is not positive.
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

    def _validate_put_args(self, artifact_name, filename, fileobj, data):
        """Validate supported input arguments and whether uploads are enabled."""
        if data is None:
            raise ValueError('StorageSegment requires dict= for analytics uploads')
        if filename or fileobj:
            raise ValueError('StorageSegment: filename= and fileobj= are not supported; use dict=')

        if self.write_key:
            return True
        if self.debug:
            logger.debug('Segment write_key not set, skipping analytics upload for: %s', artifact_name)
        return False

    def _prepare_batches(self, artifact_name, data, event_name, segment_meta, anonymous_id):
        """Build bounded Segment batches and return them with their chunks."""
        if event_name is None:
            event_name = 'Metrics Artifact Upload'
        segment_meta = {**(segment_meta or {})}
        has_message_id = bool(segment_meta.get('message_id'))
        if has_message_id and anonymous_id is None:
            raise ValueError("segment_meta['message_id'] requires anonymous_id for retry-safe sends")
        if 'timestamp' not in segment_meta:
            segment_meta['timestamp'] = datetime.datetime.now(tz=datetime.UTC)
        if anonymous_id is None:
            anonymous_id = str(uuid.uuid4())
        chunks = self._split_into_chunks(data, self.REGULAR_MESSAGE_LIMIT)
        total_chunks = len(chunks)

        if self.debug:
            print(f'Split data into {total_chunks} chunks', file=sys.stderr)

        events = [
            self._build_event(artifact_name, event_name, anonymous_id, chunk, i, total_chunks, segment_meta) for i, chunk in enumerate(chunks, 1)
        ]
        sent_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
        return chunks, self._split_into_batches(events, sent_at), sent_at

    def _send_batch(self, endpoint, batch, batch_number, total_batches, sent_at):
        """Send one batch and log or re-raise transport failures."""
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
                    total_batches,
                    response.status_code if response is not None else None,
                    response.text if response is not None else None,
                    error,
                )
            raise

        if self.debug:
            logger.debug(
                'Segment batch %d/%d: events=%d, json_bytes=%d, wire_bytes=%d, status=%d, response=%s',
                batch_number,
                total_batches,
                len(batch),
                len(body),
                len(wire_body),
                response.status_code,
                response.text,
            )

    def _send_batches(self, batches, sent_at):
        """Send all prepared batches to Segment's batch endpoint."""
        endpoint = f'{(self.host or "https://api.segment.io").rstrip("/")}{self.SEGMENT_BATCH_PATH}'
        for batch_number, batch in enumerate(batches, 1):
            self._send_batch(endpoint, batch, batch_number, len(batches), sent_at)

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
            segment_meta: Optional metadata. For retry-safe sends, provide both
                          ``message_id`` and ``anonymous_id``.
            anonymous_id: Optional anonymized ID to reuse across related sends.
                          It must be paired with ``segment_meta['message_id']``.

        This method supports sending anonymized analytics from
        multiple apps. Data is split so each `data` chunk is under
        :attr:`REGULAR_MESSAGE_LIMIT` (JSON bytes), with headroom for Segment's
        per-message size limit.
        """
        if not self._validate_put_args(artifact_name, filename, fileobj, dict):
            return
        chunks, batches, sent_at = self._prepare_batches(artifact_name, dict, event_name, segment_meta, anonymous_id)
        self._send_batches(batches, sent_at)
        return chunks
