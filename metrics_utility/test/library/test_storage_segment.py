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

        # Each top-level key gets its own chunk, even for small data
        assert len(chunks) == 5
        assert 'statistics' in chunks[0]
        assert 'module_stats' in chunks[1]
        assert 'collection_stats' in chunks[2]
        assert 'jobs_by_job_type' in chunks[3]
        assert 'job_host_summary' in chunks[4]

    def test_correct_splitting_for_large_data(self):
        storage_segment = StorageSegment()

        chunks = storage_segment._split_into_chunks(segment_data_large, storage_segment.REGULAR_MESSAGE_LIMIT)

        # assertions based on result
        assert len(chunks) == 7

        # statistics is first key of first chunk
        assert 'statistics' in chunks[0]
        assert 'module_stats' in chunks[1]
        assert 'module_stats' in chunks[2]
        assert 'module_stats' in chunks[3]
        assert 'collection_stats' in chunks[4]
        assert 'jobs_by_job_type' in chunks[5]
        assert 'job_host_summary' in chunks[6]

        assert len(chunks[1]['module_stats']) == 38
        assert len(chunks[2]['module_stats']) == 38
        assert len(chunks[3]['module_stats']) == 36

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

    def test_rollup_period_string_arrays(self):
        """Test that arrays of strings (like rollup_period_controller_versions) are split correctly."""
        data = {
            'rollup_period_controller_versions': ['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0'],
            'rollup_period_scm_types': ['git', 'manual'],
            'rollup_period_credential_types': ['Amazon Web Services', 'Container Registry', 'Machine', 'Network', 'Source Control', 'Vault'],
        }
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(data, storage_segment.REGULAR_MESSAGE_LIMIT)

        # Each top-level key should get its own chunk
        assert len(chunks) == 3
        assert 'rollup_period_controller_versions' in chunks[0]
        assert 'rollup_period_scm_types' in chunks[1]
        assert 'rollup_period_credential_types' in chunks[2]

        # Verify the data is preserved correctly
        assert chunks[0]['rollup_period_controller_versions'] == data['rollup_period_controller_versions']
        assert chunks[1]['rollup_period_scm_types'] == data['rollup_period_scm_types']
        assert chunks[2]['rollup_period_credential_types'] == data['rollup_period_credential_types']

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
        # Should split into 7 chunks as tested earlier
        assert len(chunks) == 7
        assert mock_analytics.track.call_count == 7
        # 7 chunks well under 450 KB batch limit — only the final flush should fire
        assert mock_analytics.flush.call_count == 1

        # Verify chunk numbering in the calls
        for i, call in enumerate(mock_analytics.track.call_args_list, 1):
            call_kwargs = call[1]
            chunk_info = call_kwargs['properties']['chunk_info']
            assert chunk_info['chunk_number'] == i
            assert chunk_info['total_chunks'] == 7

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_single_flush_when_chunks_fit_in_one_batch(self, mock_analytics):
        """All chunks are sent in one batch when total size is below BATCH_SIZE_LIMIT."""
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', debug=False)
        chunks = storage_segment.put(
            artifact_name='test_artifact',
            dict=segment_data_large,
            event_name='Test Event',
        )

        assert mock_analytics.track.call_count == len(chunks)
        assert mock_analytics.flush.call_count == 1

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_flushes_mid_loop_when_batch_limit_reached(self, mock_analytics):
        """flush() is called before queuing a chunk that would push the batch over BATCH_SIZE_LIMIT.

        Segment silently drops events from batch POSTs that exceed 500 KB and returns
        HTTP 200, so we flush proactively before reaching that threshold.
        """
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', debug=False)
        # Lower the limit so each large (~24 KB) chunk forces its own batch
        storage_segment.BATCH_SIZE_LIMIT = storage_segment.REGULAR_MESSAGE_LIMIT + storage_segment.EVENT_OVERHEAD

        chunks = storage_segment.put(
            artifact_name='test_artifact',
            dict=segment_data_large,
            event_name='Test Event',
        )

        # Every chunk must still be tracked — none dropped
        assert mock_analytics.track.call_count == len(chunks)
        # Multiple flushes confirm that mid-loop splitting occurred
        assert mock_analytics.flush.call_count > 1
