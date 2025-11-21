# import segment data from testing_data_for_segment
import json

# import storage segment
from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.test.library.testing_data_for_segment import segment_data


class TestStorageSegmentAvailable:
    """Test StorageSegment when segment module is available."""

    def test_correct_splitting(self):
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(segment_data, storage_segment.REGULAR_MESSAGE_LIMIT)

        # pretty print chunks
        for chunk in chunks:
            print('--------------------------------')
            print(json.dumps(chunk, indent=4))
            print('--------------------------------')
        print(len(chunks))

        assert len(chunks) == 6

        # Verify each chunk has the expected main key
        assert 'statistics' in chunks[0]
        assert 'modules_used_per_playbook' in chunks[1]
        assert 'module_stats' in chunks[2]
        assert 'collection_name_stats' in chunks[3]
        assert 'jobs_by_template' in chunks[4]
        assert 'job_host_summary' in chunks[5]

        # Verify each chunk has only one main key
        for chunk in chunks:
            assert len(chunk.keys()) == 1
