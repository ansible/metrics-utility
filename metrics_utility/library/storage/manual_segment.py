"""Direct HTTP storage backend for Segment analytics.

Sends to Segment's batch API via plain ``requests.post`` without the
segment-analytics-python library.  Avoids the library's background-thread
batching, which can silently merge all chunks into a single oversized POST
that exceeds Segment's 500 KB batch limit and is dropped with no error.

Key differences from StorageSegment:
- No dependency on segment.analytics
- All chunks sent in one POST to /v1/batch
- Each chunk gets a unique UUID messageId and a unique timestamp (spaced
  100 ms apart) to prevent Segment-side deduplication
- HTTP errors surface as exceptions rather than silent callbacks
"""

import datetime
import uuid

import requests

from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.logger import logger


class ManualStorageSegment(StorageSegment):
    """Segment storage backend using direct HTTP calls to /v1/batch.

    Inherits ``_calculate_size`` and ``_split_into_chunks`` from
    ``StorageSegment`` so chunking behaviour stays identical.
    """

    SEGMENT_BATCH_URL = 'https://api.segment.io/v1/batch'
    # Space chunk timestamps apart so Segment does not deduplicate events
    # that share the same anonymousId and timestamp.
    CHUNK_TIMESTAMP_GAP_MS = 100

    def __init__(self, **settings):
        self.debug = settings.get('debug', False)
        self.user_id = settings.get('user_id', 'unknown')
        self.write_key = settings.get('write_key')

        if not self.write_key:
            logger.info('ManualStorageSegment: write_key not set. Analytics will be disabled.')

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None, event_name=None, segment_meta=None):
        """Send data to Segment via direct HTTP batch POST.

        Args:
            artifact_name: Name of the artifact being sent.
            filename: Not supported (raises).
            fileobj: Not supported (raises).
            dict: Dictionary of data to send.
            event_name: Segment event name (defaults to 'Metrics Artifact Upload').
            segment_meta: Optional dict; any keys other than 'message_id' and
                'timestamp' are forwarded onto every event in the batch.

        Returns:
            List of chunk dicts sent, or None if not configured.

        Raises:
            requests.HTTPError: If Segment returns a non-2xx status.
        """
        if filename or fileobj or dict is None:
            raise Exception('ManualStorageSegment: filename= & fileobj= not supported, use dict=')

        if not self.write_key:
            if self.debug:
                logger.debug('ManualStorageSegment: write_key not set, skipping upload for: %s', artifact_name)
            return None

        if event_name is None:
            event_name = 'Metrics Artifact Upload'

        if not segment_meta:
            segment_meta = {}

        chunks = self._split_into_chunks(dict, self.REGULAR_MESSAGE_LIMIT)
        total_chunks = len(chunks)
        anonymous_id = str(uuid.uuid4())
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        if self.debug:
            logger.debug('ManualStorageSegment: %d chunks for %s', total_chunks, artifact_name)

        # Extra fields from segment_meta to forward (skip internal keys)
        extra_fields = {k: v for k, v in segment_meta.items() if k not in ('message_id', 'timestamp')}

        batch_events = []
        for i, chunk in enumerate(chunks, 1):
            chunk_size = self._calculate_size(chunk)
            chunk_ts = (now + datetime.timedelta(milliseconds=i * self.CHUNK_TIMESTAMP_GAP_MS)).isoformat()

            event = {
                'type': 'track',
                'anonymousId': anonymous_id,
                'messageId': str(uuid.uuid4()),
                'event': event_name,
                'timestamp': chunk_ts,
                'properties': {
                    'artifact_name': artifact_name,
                    'data': chunk,
                    'upload_timestamp': chunk_ts,
                    'chunk_info': {
                        'chunk_number': i,
                        'total_chunks': total_chunks,
                        'chunk_size': chunk_size,
                    },
                },
                **extra_fields,
            }
            batch_events.append(event)

            if self.debug:
                logger.debug(
                    'ManualStorageSegment: chunk %d/%d prepared (%d bytes)',
                    i,
                    total_chunks,
                    chunk_size,
                )

        resp = requests.post(
            self.SEGMENT_BATCH_URL,
            json={'batch': batch_events, 'sentAt': now.isoformat()},
            auth=(self.write_key, ''),
            timeout=30,
        )

        if self.debug:
            logger.debug('ManualStorageSegment: %s → %d %s', self.SEGMENT_BATCH_URL, resp.status_code, resp.text)

        resp.raise_for_status()

        logger.info(
            'ManualStorageSegment: sent %d/%d chunks to Segment (%s)',
            total_chunks,
            total_chunks,
            artifact_name,
        )

        return chunks
