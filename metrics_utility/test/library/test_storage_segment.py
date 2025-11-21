# import segment data from testing_data_for_segment

# import storage segment
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
