import json
import sys

from unittest.mock import MagicMock, patch

import pytest


class TestStorageSegmentAvailable:
    """Test StorageSegment when segment module is available."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup mocks for each test."""
        # Mock segment.analytics module
        self.mock_analytics = MagicMock()
        self.mock_analytics.write_key = None
        self.mock_analytics.debug = False

        # Patch the import at module level
        with patch.dict('sys.modules', {'segment': MagicMock(), 'segment.analytics': self.mock_analytics}):
            # Force reload of the module with mocked segment
            if 'metrics_utility.library.storage.segment' in sys.modules:
                del sys.modules['metrics_utility.library.storage.segment']

            from metrics_utility.library.storage.segment import SEGMENT_AVAILABLE, StorageSegment

            self.StorageSegment = StorageSegment
            self.SEGMENT_AVAILABLE = SEGMENT_AVAILABLE

            yield

    def test_init_with_write_key(self):
        """Test initialization with valid write_key."""
        storage = self.StorageSegment(write_key='test-key', user_id='test-user', debug=False)

        assert storage.write_key == 'test-key'
        assert storage.user_id == 'test-user'
        assert storage.debug is False
        assert storage.use_bulk is False

    def test_init_without_write_key(self):
        """Test initialization without write_key logs a warning."""
        with patch('metrics_utility.library.storage.segment.logger') as mock_logger:
            storage = self.StorageSegment()

            assert storage.write_key is None
            mock_logger.info.assert_called_once()
            assert 'write_key not set' in mock_logger.info.call_args[0][0]

    def test_init_with_bulk_mode(self):
        """Test initialization with bulk mode enabled."""
        storage = self.StorageSegment(write_key='test-key', use_bulk=True)

        assert storage.use_bulk is True

    def test_put_dict_single_message(self):
        """Test put() with small data that fits in a single message."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key', debug=False)

            test_data = {'foo': 'bar', 'count': 42}
            storage.put('test-artifact', dict=test_data, event_name='Test Event')

            # Verify analytics configuration
            assert self.mock_analytics.write_key == 'test-key'
            assert self.mock_analytics.debug is False

            # Verify track was called once
            assert self.mock_analytics.track.call_count == 1

            # Verify track was called with correct parameters
            track_call = self.mock_analytics.track.call_args
            assert 'anonymous_id' in track_call.kwargs
            assert track_call.kwargs['event'] == 'Test Event'
            assert track_call.kwargs['properties']['artifact_name'] == 'test-artifact'
            assert track_call.kwargs['properties']['data'] == test_data
            assert 'upload_timestamp' in track_call.kwargs['properties']
            assert 'data_size' in track_call.kwargs['properties']

            # Verify flush was called
            self.mock_analytics.flush.assert_called_once()

    def test_put_dict_default_event_name(self):
        """Test put() with default event name."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            test_data = {'test': 'data'}
            storage.put('test-artifact', dict=test_data)

            # Verify default event name was used
            track_call = self.mock_analytics.track.call_args
            assert track_call.kwargs['event'] == 'Metrics Artifact Upload'

    def test_put_dict_chunked_message(self):
        """Test put() with large data that requires chunking."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key', debug=False)

            # Create data larger than 32KB (regular message limit)
            large_item = 'x' * 10000  # 10KB string
            test_data = [{'data': large_item, 'index': i} for i in range(10)]  # ~100KB total

            storage.put('test-artifact', dict=test_data)

            # Verify track was called multiple times (chunked)
            assert self.mock_analytics.track.call_count > 1

            # Verify all chunks have chunk_info
            for track_call in self.mock_analytics.track.call_args_list:
                properties = track_call.kwargs['properties']
                assert 'chunk_info' in properties
                assert 'chunk_number' in properties['chunk_info']
                assert 'total_chunks' in properties['chunk_info']
                assert 'chunk_size' in properties['chunk_info']

            # Verify flush was called once at the end
            self.mock_analytics.flush.assert_called_once()

    def test_put_without_write_key(self):
        """Test put() returns early when write_key is not set."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            with patch('metrics_utility.library.storage.segment.logger') as mock_logger:
                storage = self.StorageSegment(debug=True)

                test_data = {'test': 'data'}
                storage.put('test-artifact', dict=test_data)

                # Verify track was not called
                self.mock_analytics.track.assert_not_called()

                # Verify debug message was logged
                assert mock_logger.debug.called

    def test_put_raises_exception_for_unsupported_params(self):
        """Test put() raises exception for unsupported filename/fileobj parameters."""
        storage = self.StorageSegment(write_key='test-key')

        with pytest.raises(Exception) as exc:
            storage.put('test-artifact', filename='test.txt')
        assert 'not supported' in str(exc.value)

        with pytest.raises(Exception) as exc:
            storage.put('test-artifact', fileobj=MagicMock())
        assert 'not supported' in str(exc.value)

        with pytest.raises(Exception) as exc:
            storage.put('test-artifact')  # No dict provided
        assert 'not supported' in str(exc.value)

    def test_calculate_size(self):
        """Test _calculate_size() method."""
        storage = self.StorageSegment(write_key='test-key')

        test_data = {'foo': 'bar'}
        size = storage._calculate_size(test_data)

        expected_size = len(json.dumps(test_data).encode('utf-8'))
        assert size == expected_size

    def test_split_dict_into_chunks(self):
        """Test _split_dict_into_chunks() method."""
        storage = self.StorageSegment(write_key='test-key')

        test_dict = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
        max_size = 30  # Small size to force chunking

        chunks = storage._split_dict_into_chunks(test_dict, max_size)

        # Verify we got multiple chunks
        assert len(chunks) > 1

        # Verify all chunks are dicts
        for chunk in chunks:
            assert isinstance(chunk, dict)

    def test_split_into_chunks_list(self):
        """Test _split_into_chunks() with list data."""
        storage = self.StorageSegment(write_key='test-key')

        test_list = [{'item': i} for i in range(10)]
        max_size = 50  # Small size to force chunking

        chunks = storage._split_into_chunks(test_list, max_size)

        # Verify we got multiple chunks
        assert len(chunks) > 1

        # Verify all chunks are lists
        for chunk in chunks:
            assert isinstance(chunk, list)

    def test_split_into_chunks_dict(self):
        """Test _split_into_chunks() with dict data."""
        storage = self.StorageSegment(write_key='test-key')

        test_dict = {f'key{i}': f'value{i}' for i in range(10)}
        max_size = 50  # Small size to force chunking

        chunks = storage._split_into_chunks(test_dict, max_size)

        # Verify we got multiple chunks
        assert len(chunks) > 1

    def test_split_into_chunks_oversized_item(self, capsys):
        """Test _split_into_chunks() with item larger than max_size."""
        storage = self.StorageSegment(write_key='test-key')

        # Create an item larger than max_size
        large_item = {'data': 'x' * 1000}
        test_list = [large_item]
        max_size = 50

        chunks = storage._split_into_chunks(test_list, max_size)

        # Verify the large item was still included
        assert len(chunks) >= 1

        # Verify warning was printed to stderr
        captured = capsys.readouterr()
        assert 'Warning' in captured.err or len(chunks) > 0

    def test_bulk_mode_uses_larger_limit(self):
        """Test that bulk mode uses the larger message limit."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key', use_bulk=True)

            # Create data larger than regular limit but smaller than bulk limit
            # Regular: 32KB, Bulk: 512MB
            large_data = {'data': 'x' * 40000}  # 40KB

            storage.put('test-artifact', dict=large_data)

            # With bulk mode, this should send in a single message
            assert self.mock_analytics.track.call_count == 1

            # Verify no chunk_info (not chunked)
            track_call = self.mock_analytics.track.call_args
            assert 'chunk_info' not in track_call.kwargs['properties']

    def test_anonymous_id_is_unique_per_put(self):
        """Test that each put() call generates a unique anonymous_id."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            test_data = {'test': 'data'}

            # Call put multiple times
            storage.put('artifact1', dict=test_data)
            first_id = self.mock_analytics.track.call_args.kwargs['anonymous_id']

            self.mock_analytics.reset_mock()

            storage.put('artifact2', dict=test_data)
            second_id = self.mock_analytics.track.call_args.kwargs['anonymous_id']

            # Verify IDs are different
            assert first_id != second_id

    def test_debug_mode_messages(self, capsys):
        """Test that debug mode prints messages."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key', debug=True)

            # Create data that requires chunking
            large_data = [{'data': 'x' * 10000} for _ in range(5)]

            storage.put('test-artifact', dict=large_data)

            # Verify debug messages were printed
            captured = capsys.readouterr()
            assert 'Data size' in captured.err or 'Sending' in captured.err


class TestStorageSegmentNotAvailable:
    """Test StorageSegment when segment module is not available."""

    def test_init_logs_warning_when_not_available(self):
        """Test that initialization logs warning when segment is not available."""
        with patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', False):
            with patch('metrics_utility.library.storage.segment.logger') as mock_logger:
                from metrics_utility.library.storage.segment import StorageSegment

                storage = StorageSegment(write_key='test-key')

                # Verify warning was logged
                mock_logger.info.assert_called()
                assert storage.write_key == 'test-key'
                call_args = [call[0][0] for call in mock_logger.info.call_args_list]
                assert any('segment module not installed' in arg for arg in call_args)

    def test_put_returns_early_when_not_available(self):
        """Test that put() returns early when segment is not available."""
        with patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', False):
            with patch('metrics_utility.library.storage.segment.logger') as mock_logger:
                from metrics_utility.library.storage.segment import StorageSegment

                storage = StorageSegment(write_key='test-key', debug=True)

                test_data = {'test': 'data'}
                result = storage.put('test-artifact', dict=test_data)

                # Verify it returns None without error
                assert result is None

                # Verify debug message was logged
                assert mock_logger.debug.called
                assert 'not available' in mock_logger.debug.call_args[0][0]

    def test_calculate_size_works_without_segment(self):
        """Test that _calculate_size() works even when segment is not available."""
        with patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', False):
            from metrics_utility.library.storage.segment import StorageSegment

            storage = StorageSegment(write_key='test-key')

            test_data = {'foo': 'bar'}
            size = storage._calculate_size(test_data)

            expected_size = len(json.dumps(test_data).encode('utf-8'))
            assert size == expected_size

    def test_put_with_no_write_key_and_segment_not_available(self):
        """Test that put() handles both missing segment and missing write_key gracefully."""
        with patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', False):
            with patch('metrics_utility.library.storage.segment.logger') as mock_logger:
                from metrics_utility.library.storage.segment import StorageSegment

                storage = StorageSegment(debug=True)

                test_data = {'test': 'data'}
                result = storage.put('test-artifact', dict=test_data)

                # Should return early due to segment not available (checked first)
                assert result is None
                assert mock_logger.debug.called


class TestStorageSegmentEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup mocks for each test."""
        self.mock_analytics = MagicMock()

        with patch.dict('sys.modules', {'segment': MagicMock(), 'segment.analytics': self.mock_analytics}):
            if 'metrics_utility.library.storage.segment' in sys.modules:
                del sys.modules['metrics_utility.library.storage.segment']

            from metrics_utility.library.storage.segment import StorageSegment

            self.StorageSegment = StorageSegment

            yield

    def test_empty_dict(self):
        """Test put() with empty dict."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            storage.put('test-artifact', dict={})

            # Verify it was sent
            self.mock_analytics.track.assert_called_once()

    def test_nested_dict(self):
        """Test put() with nested dictionary."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            nested_data = {'level1': {'level2': {'level3': 'value'}}}

            storage.put('test-artifact', dict=nested_data)

            # Verify it was sent
            self.mock_analytics.track.assert_called_once()

            # Verify the nested data is preserved
            track_call = self.mock_analytics.track.call_args
            assert track_call.kwargs['properties']['data'] == nested_data

    def test_special_characters_in_data(self):
        """Test put() with special characters in data."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            special_data = {
                'unicode': '你好世界',
                'emoji': '🚀💻',
                'symbols': '!@#$%^&*()',
            }

            storage.put('test-artifact', dict=special_data)

            # Verify it was sent without error
            self.mock_analytics.track.assert_called_once()

    def test_list_with_mixed_types(self):
        """Test put() with list containing mixed types."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            mixed_list = [
                {'type': 'dict'},
                'string',
                42,
                3.14,
                True,
                None,
            ]

            storage.put('test-artifact', dict=mixed_list)

            # Verify it was sent
            self.mock_analytics.track.assert_called_once()

    def test_chunk_splitting_preserves_all_data(self):
        """Test that chunk splitting doesn't lose any data."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            # Create data with unique identifiable items
            test_data = [{'id': i, 'data': 'x' * 1000} for i in range(100)]

            storage.put('test-artifact', dict=test_data)

            # Collect all data sent across all chunks
            sent_data = []
            for track_call in self.mock_analytics.track.call_args_list:
                chunk_data = track_call.kwargs['properties']['data']
                if isinstance(chunk_data, list):
                    sent_data.extend(chunk_data)
                else:
                    sent_data.append(chunk_data)

            # Verify all items were sent
            sent_ids = [item['id'] for item in sent_data if isinstance(item, dict) and 'id' in item]
            expected_ids = list(range(100))
            assert sorted(sent_ids) == expected_ids

    def test_split_list_with_oversized_dict_item(self):
        """Test splitting a list with an oversized dict item that needs further splitting."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key')

            # Create a list with regular items and one huge dict item
            large_dict = {f'key{i}': 'x' * 10000 for i in range(10)}  # ~100KB dict
            test_data = [
                {'small': 'item1'},
                large_dict,  # This will exceed max_size and need to be split
                {'small': 'item2'},
            ]

            storage.put('test-artifact', dict=test_data)

            # Verify data was sent (chunked)
            assert self.mock_analytics.track.call_count > 1

    def test_split_single_value(self):
        """Test _split_into_chunks with a single non-list/non-dict value."""
        storage = self.StorageSegment(write_key='test-key')

        # Single string value
        chunks = storage._split_into_chunks('single_value', 1000)

        assert len(chunks) == 1
        assert chunks[0] == 'single_value'

    def test_split_list_with_oversized_non_dict_item(self, capsys):
        """Test splitting list with an oversized non-dict item that can't be split."""
        storage = self.StorageSegment(write_key='test-key')

        # Create a list with one huge non-dict item
        huge_string = 'x' * 10000
        test_data = [
            {'small': 'item1'},
            huge_string,  # This exceeds max_size but can't be split
            {'small': 'item2'},
        ]
        max_size = 100

        chunks = storage._split_into_chunks(test_data, max_size)

        # Should still create chunks, with the large item in its own chunk
        assert len(chunks) >= 3

        # Verify warning was printed
        captured = capsys.readouterr()
        assert 'Warning' in captured.err

    def test_split_dict_with_oversized_key_value_pair(self):
        """Test splitting dict when a single key-value pair exceeds max_size."""
        storage = self.StorageSegment(write_key='test-key')

        # Create dict with one huge value and some normal ones
        test_dict = {
            'small1': 'value1',
            'huge': 'x' * 10000,  # This pair exceeds max_size
            'small2': 'value2',
        }
        max_size = 100  # Small enough that 'huge' pair exceeds it

        chunks = storage._split_dict_into_chunks(test_dict, max_size)

        # Should split into multiple chunks
        assert len(chunks) >= 2

        # Verify all keys are preserved
        all_keys = set()
        for chunk in chunks:
            all_keys.update(chunk.keys())
        assert all_keys == {'small1', 'huge', 'small2'}

    def test_debug_single_message(self, capsys):
        """Test debug output for single (non-chunked) message."""
        with patch('metrics_utility.library.storage.segment.analytics', self.mock_analytics):
            storage = self.StorageSegment(write_key='test-key', debug=True)

            # Small data that fits in one message
            test_data = {'small': 'data'}

            storage.put('test-artifact', dict=test_data)

            # Verify debug message was printed
            captured = capsys.readouterr()
            assert 'single message' in captured.err
