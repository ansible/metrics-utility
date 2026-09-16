"""Storage backend that ships anonymized analytics to Segment."""

import datetime
import gzip
import hashlib
import json
import sys
import uuid

from decimal import Decimal
from enum import Enum
from urllib.parse import urlsplit


try:
    import requests
except ImportError:  # pragma: no cover - exercised by downstream import checks
    requests = None
    SEGMENT_AVAILABLE = False
else:
    SEGMENT_AVAILABLE = True

from metrics_utility.logger import logger


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
    # Reserve room for variable-width chunk metadata after the data limit is
    # calculated from a minimal event envelope.
    EVENT_SIZE_SAFETY_MARGIN = 128
    REQUEST_TIMEOUT = 30
    SEGMENT_BATCH_PATH = '/v1/batch'

    def __init__(self, **settings):
        """Initialise the Segment storage backend.

        Args:
            **settings: Accepts ``'debug'`` (bool), ``'user_id'`` (str),
                ``'write_key'`` (str, required for actual uploads),
                ``'host'`` (str, optional base URL override),
                ``'gzip'`` (bool, default True), and ``'allow_insecure_host'``
                (bool, test-only opt-in for internal HTTP hosts).
        """
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')
        self.host = settings.get('host')
        self.gzip = settings.get('gzip', True)
        self.allow_insecure_host = settings.get('allow_insecure_host', self.debug)

        if not SEGMENT_AVAILABLE:
            logger.info('StorageSegment: requests module not installed. Analytics will be disabled.')

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
        return len(self._json_bytes(self._serialize_value(data)))

    @staticmethod
    def _json_bytes(data):
        """Serialize a payload using compact JSON and UTF-8 bytes."""
        return json.dumps(data, separators=(',', ':')).encode('utf-8')

    @staticmethod
    def _serialize_value(value):
        """Convert values to the JSON-compatible forms used by the SDK."""
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, Enum):
            return StorageSegment._serialize_value(value.value)
        if isinstance(value, dict):
            return {key: StorageSegment._serialize_value(item) for key, item in value.items()}
        if isinstance(value, (set, frozenset, list, tuple)):
            return [StorageSegment._serialize_value(item) for item in value]
        return value

    @staticmethod
    def _canonicalize_timestamp(value):
        """Return one stable string representation for a timestamp value."""
        if value is None:
            value = datetime.datetime.now(tz=datetime.UTC)

        if isinstance(value, datetime.datetime):
            timestamp = value
        elif isinstance(value, str):
            candidate = value[:-1] + '+00:00' if value.endswith(('Z', 'z')) else value
            try:
                timestamp = datetime.datetime.fromisoformat(candidate)
            except ValueError:
                return value
        elif isinstance(value, datetime.date):
            return value.isoformat()
        else:
            return str(value)

        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(datetime.UTC)
        return timestamp.isoformat()

    def _build_event(self, artifact_name, event_name, anonymous_id, chunk, chunk_number, total_chunks, segment_meta):
        """Build one deterministic Segment track event from an artifact chunk."""
        chunk = self._serialize_value(chunk)
        chunk_size = self._calculate_size(chunk)
        timestamp = self._canonicalize_timestamp(segment_meta['timestamp'])
        base_message_id = segment_meta.get('message_id') or f'{artifact_name}:{event_name}:{anonymous_id}:{timestamp}'
        message_id = hashlib.sha256(f'{base_message_id}_{chunk_number}'.encode('utf-8', errors='replace')).hexdigest()

        event = {
            'type': 'track',
            'anonymousId': anonymous_id,
            'messageId': message_id,
            'event': event_name,
            'timestamp': timestamp,
            'context': self._serialize_value(segment_meta.get('context', {})),
            'integrations': self._serialize_value(segment_meta.get('integrations', {})),
            'properties': self._build_properties(artifact_name, chunk, chunk_number, total_chunks, chunk_size),
        }
        user_id = segment_meta.get('user_id') or self.user_id
        if user_id and user_id != 'unknown':
            event['userId'] = user_id
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

    def _calculate_event_overhead(self, artifact_name, event_name, anonymous_id, segment_meta):
        """Measure the JSON byte overhead of one Segment track event envelope."""
        dummy = self._build_event(artifact_name, event_name, anonymous_id, {}, 1, 1, segment_meta)
        return len(self._json_bytes(dummy))

    def _validate_event_sizes(self, events):
        """Reject oversized complete events before any request is sent."""
        for event_number, event in enumerate(events, 1):
            event_size = len(self._json_bytes(event))
            if event_size > self.REGULAR_MESSAGE_LIMIT:
                msg = f'Segment event {event_number} is {event_size} bytes, exceeding the {self.REGULAR_MESSAGE_LIMIT}-byte message limit'
                raise ValueError(msg)

    def _validate_put_args(self, artifact_name, filename, fileobj, data):
        """Validate supported input arguments and whether uploads are enabled."""
        if data is None:
            raise ValueError('StorageSegment requires dict= for analytics uploads')
        if filename or fileobj:
            raise ValueError('StorageSegment: filename= and fileobj= are not supported; use dict=')

        if not SEGMENT_AVAILABLE:
            if self.debug:
                logger.debug('Segment transport unavailable, skipping analytics upload for: %s', artifact_name)
            return False
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
        segment_meta['timestamp'] = self._canonicalize_timestamp(segment_meta.get('timestamp'))
        if anonymous_id is None:
            anonymous_id = str(uuid.uuid4())
        overhead = self._calculate_event_overhead(artifact_name, event_name, anonymous_id, segment_meta)
        chunk_limit = self.REGULAR_MESSAGE_LIMIT - overhead - self.EVENT_SIZE_SAFETY_MARGIN
        if chunk_limit <= 0:
            msg = 'Segment event metadata leaves no room for event data under the message limit'
            raise ValueError(msg)
        chunks = self._split_into_chunks(data, chunk_limit)
        total_chunks = len(chunks)

        if self.debug:
            print(f'Split data into {total_chunks} chunks', file=sys.stderr)

        events = [
            self._build_event(artifact_name, event_name, anonymous_id, chunk, i, total_chunks, segment_meta) for i, chunk in enumerate(chunks, 1)
        ]
        self._validate_event_sizes(events)
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
        base_url = (self.host or 'https://api.segment.io').rstrip('/')
        parsed_url = urlsplit(base_url)
        if parsed_url.scheme != 'https':
            is_loopback = parsed_url.hostname in {'localhost', '127.0.0.1', '::1'}
            is_internal_hostname = parsed_url.hostname is not None and '.' not in parsed_url.hostname
            explicit_test_host = self.host is not None and self.allow_insecure_host and (is_loopback or is_internal_hostname)
            if parsed_url.scheme != 'http' or not (is_loopback or explicit_test_host):
                raise ValueError('Segment host must use HTTPS; HTTP is restricted to loopback and internal test hosts')
        if not parsed_url.netloc:
            raise ValueError('Segment host must include a valid hostname')
        endpoint = f'{base_url}{self.SEGMENT_BATCH_PATH}'
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
            segment_meta: Optional metadata. Provide a stable ``message_id``
                          for retry-safe sends.
            anonymous_id: Optional anonymized ID to reuse across related sends.
                          It controls anonymous identity correlation independently
                          of Segment's message deduplication.

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
