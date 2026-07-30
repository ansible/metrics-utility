from unittest.mock import Mock, patch

import pytest

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

        assert len(chunks[1]['module_stats']) == 50
        assert len(chunks[2]['module_stats']) == 50
        assert len(chunks[3]['module_stats']) == 12

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
        assert len(chunks[0]['test_list']) == 2821
        assert len(chunks[1]['test_list']) == 179

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
        """put() tracks every chunk, flushes once, and enables sync_mode."""
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', user_id='test_user', debug=True)
        chunks = storage_segment.put(artifact_name='test_artifact', dict=segment_data, event_name='Test Event')

        assert mock_analytics.sync_mode is True
        assert mock_analytics.track.call_count == len(chunks)
        assert mock_analytics.flush.call_count == 1

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
        assert mock_analytics.flush.call_count == 1

        # Verify chunk numbering in the calls
        for i, call in enumerate(mock_analytics.track.call_args_list, 1):
            call_kwargs = call[1]
            chunk_info = call_kwargs['properties']['chunk_info']
            assert chunk_info['chunk_number'] == i
            assert chunk_info['total_chunks'] == 7

    def test_split_into_chunks_rejects_non_positive_max_size(self):
        storage_segment = StorageSegment()
        with pytest.raises(ValueError, match='max_size must be positive'):
            storage_segment._split_into_chunks({'key': [1, 2, 3]}, 0)
        with pytest.raises(ValueError, match='max_size must be positive'):
            storage_segment._split_into_chunks({'key': [1, 2, 3]}, -100)

    def test_split_into_chunks_warns_on_oversized_dict(self, caplog):
        storage_segment = StorageSegment()
        data = {'big': {'a': 'x' * 500}}
        chunks = storage_segment._split_into_chunks(data, 50)
        assert len(chunks) == 1
        assert chunks[0] == data
        assert 'Oversized dict chunk' in caplog.text

    def test_split_into_chunks_warns_on_oversized_single_list_item(self, caplog):
        storage_segment = StorageSegment()
        data = {'items': ['x' * 500]}
        chunks = storage_segment._split_into_chunks(data, 50)
        assert len(chunks) == 1
        assert chunks[0]['items'] == ['x' * 500]
        assert 'Single list item' in caplog.text

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_overhead_includes_segment_meta(self, mock_analytics):
        """put() accounts for segment_meta (including hashed message_id) in overhead."""
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key')
        meta = {'message_id': 'original-id-value'}
        chunks_with_meta = storage_segment.put(
            artifact_name='test',
            dict={'items': [f'item{i}' for i in range(3000)]},
            event_name='Test',
            segment_meta=meta,
        )

        chunks_without_meta = storage_segment.put(
            artifact_name='test',
            dict={'items': [f'item{i}' for i in range(3000)]},
            event_name='Test',
        )

        # A 64-char hashed message_id is larger than a 36-char UUID,
        # so with meta the overhead is higher and chunks hold fewer items
        assert len(chunks_with_meta) >= len(chunks_without_meta)

    @patch('metrics_utility.library.storage.segment.analytics')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', True)
    def test_put_sync_mode_no_batch_drops(self, mock_analytics):
        """sync_mode=True prevents Segment silently dropping chunks from oversized batches.

        Without sync_mode the SDK batches all track() calls into a single POST. With
        15 chunks at ~25 KB each the batch exceeds Segment's 500 KB limit and events
        are dropped server-side — Segment returns HTTP 200 with no error callback.

        sync_mode sends each track() as a separate blocking HTTP request so every
        chunk is confirmed delivered before the next is sent. End-to-end validated:
        15/15 chunks received in Segment with sync_mode=True vs 11-14 without.
        """
        mock_analytics.track = Mock()
        mock_analytics.flush = Mock()

        storage_segment = StorageSegment(write_key='test_write_key', debug=False)
        chunks = storage_segment.put(
            artifact_name='test_artifact',
            dict=segment_data_large,
            event_name='Test Event',
        )

        assert mock_analytics.sync_mode is True
        assert mock_analytics.track.call_count == len(chunks)
        assert mock_analytics.flush.call_count == 1
