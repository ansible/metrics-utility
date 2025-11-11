import datetime
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
    # Size limits for Segment messages
    REGULAR_MESSAGE_LIMIT = 32 * 1024  # 32KB for regular messages
    BULK_MESSAGE_LIMIT = 512 * 1024 * 1024  # 512MB for bulk messages

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
            data: Dictionary or list of data to split
            max_size: Maximum size in bytes for each chunk

        Returns:
            List of data chunks
        """
        chunks = []

        # If data is a list, split the list items
        if isinstance(data, list):
            current_chunk = []
            current_size = 0

            for item in data:
                item_size = self._calculate_size(item)

                # If single item exceeds max_size, handle specially
                if item_size > max_size:
                    # Save current chunk if it has items
                    if current_chunk:
                        chunks.append(current_chunk)
                        current_chunk = []
                        current_size = 0

                    # If it's a dict, try to split its keys
                    if isinstance(item, dict):
                        sub_chunks = self._split_dict_into_chunks(item, max_size)
                        chunks.extend(sub_chunks)
                    else:
                        # Item is too large and can't be split
                        msg = f'Warning: Single item exceeds max size ({item_size} > {max_size}), sending anyway'
                        print(msg, file=sys.stderr)
                        chunks.append([item])
                elif current_size + item_size > max_size:
                    # Current chunk is full, start a new one
                    chunks.append(current_chunk)
                    current_chunk = [item]
                    current_size = item_size
                else:
                    # Add item to current chunk
                    current_chunk.append(item)
                    current_size += item_size

            # Add remaining items
            if current_chunk:
                chunks.append(current_chunk)

        # If data is a dict, split by keys
        elif isinstance(data, dict):
            chunks = self._split_dict_into_chunks(data, max_size)

        else:
            # Single value, return as is
            chunks.append(data)

        return chunks if chunks else [data]

    def _split_dict_into_chunks(self, data_dict, max_size):
        """Split a dictionary into chunks by grouping keys."""
        chunks = []
        current_chunk = {}
        current_size = 0

        for key, value in data_dict.items():
            item = {key: value}
            item_size = self._calculate_size(item)

            if item_size > max_size:
                # Single key-value pair is too large
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = {}
                    current_size = 0
                msg = f"Warning: Single key-value pair '{key}' exceeds max size ({item_size} > {max_size}), sending anyway"
                print(msg, file=sys.stderr)
                chunks.append(item)
            elif current_size + item_size > max_size:
                # Current chunk is full
                chunks.append(current_chunk)
                current_chunk = {key: value}
                current_size = item_size
            else:
                # Add to current chunk
                current_chunk[key] = value
                current_size += item_size

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

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

        # Calculate data size
        data_size = self._calculate_size(dict)

        # Split into chunks if necessary
        if data_size > max_size:
            if self.debug:
                msg = f'Data size ({data_size} bytes) exceeds limit ({max_size} bytes), splitting into chunks'
                print(msg, file=sys.stderr)

            chunks = self._split_into_chunks(dict, max_size)
            total_chunks = len(chunks)

            if self.debug:
                msg = f'Split data into {total_chunks} chunks'
                print(msg, file=sys.stderr)

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
        else:
            # Data fits in a single message
            if self.debug:
                msg = f'Sending data in single message (size: {data_size} bytes)'
                print(msg, file=sys.stderr)

            analytics.track(
                anonymous_id=anonymous_id,
                event=event_name,
                properties={
                    'artifact_name': artifact_name,
                    'data': dict,
                    'upload_timestamp': (datetime.datetime.now(tz=datetime.timezone.utc).isoformat()),
                    'data_size': data_size,
                },
            )

            # Flush to ensure event is sent
            analytics.flush()
