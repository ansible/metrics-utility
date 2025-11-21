# import segment data from testing_data_for_segment

# import storage segment
from unittest.mock import Mock, patch

from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.test.library.testing_data_for_segment import segment_data, segment_data_large


class TestStorageSegmentAvailable:
    """Test StorageSegment when segment module is available."""

    def test_correct_splitting_for_small_data(self):
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(segment_data, storage_segment.REGULAR_MESSAGE_LIMIT)

        assert len(chunks) == 1

    def test_correct_splitting_for_large_data(self):
        storage_segment = StorageSegment()

        chunks = storage_segment._split_into_chunks(segment_data_large, storage_segment.REGULAR_MESSAGE_LIMIT)

        # assertions based on result
        assert len(chunks) == 9

        # statistics is first key of first chunk
        assert 'statistics' in chunks[0]
        assert 'modules_used_per_playbook' in chunks[1]
        assert 'module_stats' in chunks[2]
        assert 'module_stats' in chunks[3]
        assert 'module_stats' in chunks[4]
        assert 'module_stats' in chunks[5]
        assert 'collection_name_stats' in chunks[6]
        assert 'jobs_by_template' in chunks[7]
        assert 'job_host_summary' in chunks[8]

        assert len(chunks[2]['module_stats']) == 37
        assert len(chunks[3]['module_stats']) == 37
        assert len(chunks[4]['module_stats']) == 37
        assert len(chunks[5]['module_stats']) == 1

    def test_correct_splitting_for_large_data_with_bulk(self):
        storage_segment = StorageSegment(use_bulk=True)
        chunks = storage_segment._split_into_chunks(segment_data_large, storage_segment.BULK_MESSAGE_LIMIT)
        assert len(chunks) == 1
        assert 'module_stats' in chunks[0]
        assert 'collection_name_stats' in chunks[0]

    def test_simple_list_data(self):
        data = {'test_list': ['item1', 'item2']}
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(data, storage_segment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 1
        assert 'test_list' in chunks[0]
        assert len(chunks[0]['test_list']) == 2

    def test_simple_list_large_data(self):
        data = {'test_list': []}
        for i in range(3000):
            data['test_list'].append(f'item{i}')

        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(data, storage_segment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 2
        assert 'test_list' in chunks[0]
        assert 'test_list' in chunks[1]
        assert len(chunks[0]['test_list']) == 2139
        assert len(chunks[1]['test_list']) == 861

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_sends_data_to_segment(self, mock_analytics):
        """Test that put method sends data to segment.com with proper mocking."""
        # Setup
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', user_id='test_user', debug=True)

        # Act
        chunks = storage_segment.put(artifact_name='test_artifact', dict=segment_data, event_name='Test Event')

        # Assert
        # Verify analytics.track was called
        assert mock_analytics.track.called
        assert mock_analytics.track.call_count == len(chunks)

        # Verify flush was called
        assert mock_analytics.flush.called
        assert mock_analytics.flush.call_count == 1

        # Verify the call arguments
        call_args = mock_analytics.track.call_args[1]
        assert 'anonymous_id' in call_args
        assert call_args['event'] == 'Test Event'
        assert call_args['properties']['artifact_name'] == 'test_artifact'
        assert 'data' in call_args['properties']
        assert 'upload_timestamp' in call_args['properties']
        assert 'chunk_info' in call_args['properties']

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_sends_multiple_chunks_for_large_data(self, mock_analytics):
        """Test that put method splits large data and sends multiple chunks."""
        # Setup
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', user_id='test_user', debug=True)

        # Act
        chunks = storage_segment.put(artifact_name='test_large_artifact', dict=segment_data_large, event_name='Test Large Event')

        # Assert
        # Should split into 9 chunks as tested earlier
        assert len(chunks) == 9
        assert mock_analytics.track.call_count == 9
        assert mock_analytics.flush.call_count == 1

        # Verify chunk numbering in the calls
        for i, call in enumerate(mock_analytics.track.call_args_list, 1):
            call_kwargs = call[1]
            chunk_info = call_kwargs['properties']['chunk_info']
            assert chunk_info['chunk_number'] == i
            assert chunk_info['total_chunks'] == 9

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_sends_single_chunk_for_large_data_with_bulk(self, mock_analytics):
        """Test that put method with use_bulk=True sends large data in a single chunk."""
        # Setup
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', user_id='test_user', debug=True, use_bulk=True)

        # Act
        chunks = storage_segment.put(artifact_name='test_bulk_artifact', dict=segment_data_large, event_name='Test Bulk Event')

        # Assert
        # Should NOT split - only 1 chunk because bulk limit is 500MB
        assert len(chunks) == 1
        assert mock_analytics.track.call_count == 1
        assert mock_analytics.flush.call_count == 1

        # Verify the single call
        call_args = mock_analytics.track.call_args[1]
        assert call_args['event'] == 'Test Bulk Event'
        assert call_args['properties']['artifact_name'] == 'test_bulk_artifact'

        # Verify chunk info shows it's 1 of 1
        chunk_info = call_args['properties']['chunk_info']
        assert chunk_info['chunk_number'] == 1
        assert chunk_info['total_chunks'] == 1

        # Verify the data contains all expected keys (not split)
        data = call_args['properties']['data']
        assert 'module_stats' in data
        assert 'collection_name_stats' in data
