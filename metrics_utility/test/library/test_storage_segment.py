import gzip
import json

from unittest.mock import Mock, patch

import pytest
import requests

from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.test.library.testing_data_for_segment import segment_data, segment_data_large


class TestStorageSegmentAvailable:
    def test_correct_splitting_for_small_data(self):
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(segment_data, storage_segment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 5
        assert 'statistics' in chunks[0]
        assert 'module_stats' in chunks[1]
        assert 'collection_stats' in chunks[2]
        assert 'jobs_by_job_type' in chunks[3]
        assert 'job_host_summary' in chunks[4]

    def test_correct_splitting_for_large_data(self):
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(segment_data_large, storage_segment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 7
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
        chunks = StorageSegment()._split_into_chunks(data, StorageSegment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 1
        assert chunks[0]['test_list'] == data['test_list']

    def test_simple_list_large_data(self):
        data = {'test_list': [f'item{i}' for i in range(3000)]}
        chunks = StorageSegment()._split_into_chunks(data, StorageSegment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 2
        assert len(chunks[0]['test_list']) == 2821
        assert len(chunks[1]['test_list']) == 179

    def test_rollup_period_string_arrays(self):
        data = {
            'rollup_period_controller_versions': ['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0'],
            'rollup_period_scm_types': ['git', 'manual'],
            'rollup_period_credential_types': ['Amazon Web Services', 'Container Registry', 'Machine', 'Network', 'Source Control', 'Vault'],
        }
        chunks = StorageSegment()._split_into_chunks(data, StorageSegment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 3
        assert chunks[0]['rollup_period_controller_versions'] == data['rollup_period_controller_versions']
        assert chunks[1]['rollup_period_scm_types'] == data['rollup_period_scm_types']
        assert chunks[2]['rollup_period_credential_types'] == data['rollup_period_credential_types']

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_sends_gzipped_batch(self, mock_post):
        mock_post.return_value = Mock(status_code=200, text='{"success":true}')
        storage_segment = StorageSegment(write_key='test_write_key', debug=True)

        chunks = storage_segment.put(artifact_name='test_artifact', dict=segment_data, event_name='Test Event')

        assert mock_post.call_count == 1
        assert mock_post.call_args.args[0] == 'https://api.segment.io/v1/batch'
        request = mock_post.call_args.kwargs
        assert request['auth'] == ('test_write_key', '')
        assert request['headers']['Content-Encoding'] == 'gzip'
        payload = json.loads(gzip.decompress(request['data']))
        assert len(payload['batch']) == len(chunks)
        event = payload['batch'][0]
        assert event['anonymousId']
        assert event['event'] == 'Test Event'
        assert event['properties']['artifact_name'] == 'test_artifact'
        assert event['properties']['chunk_info']['chunk_number'] == 1

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_accepts_anonymous_id(self, mock_post):
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'first': {'value': 'one'}, 'second': {'value': 'two'}},
            anonymous_id='daily-anonymous-id',
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert {event['anonymousId'] for event in payload['batch']} == {'daily-anonymous-id'}

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_sends_multiple_chunks_in_one_batch(self, mock_post):
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', debug=True)

        chunks = storage_segment.put(artifact_name='test_large_artifact', dict=segment_data_large, event_name='Test Large Event')

        assert len(chunks) == 7
        assert mock_post.call_count == 1
        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert len(payload['batch']) == 7
        for i, event in enumerate(payload['batch'], 1):
            assert event['properties']['chunk_info']['chunk_number'] == i
            assert event['properties']['chunk_info']['total_chunks'] == 7

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

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_preserves_segment_meta(self, mock_post):
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test',
            dict={'first': {'value': 'one'}, 'second': {'value': 'two'}},
            event_name='Test',
            segment_meta={'message_id': 'original-id-value'},
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert payload['batch'][0]['messageId'] != payload['batch'][1]['messageId']
        assert len(payload['batch'][0]['messageId']) == 64

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_splits_oversized_batches(self, mock_post):
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', debug=False)
        storage_segment.BATCH_SIZE_LIMIT = 1000

        chunks = storage_segment.put(artifact_name='test_artifact', dict=segment_data_large, event_name='Test Event')

        assert len(chunks) == 7
        assert mock_post.call_count > 1
        total_events = 0
        for call in mock_post.call_args_list:
            payload = json.loads(gzip.decompress(call.kwargs['data']))
            total_events += len(payload['batch'])
        assert total_events == len(chunks)

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_can_disable_gzip(self, mock_post):
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', gzip=False)

        storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        request = mock_post.call_args.kwargs
        assert 'Content-Encoding' not in request['headers']
        assert json.loads(request['data'])['batch']

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_raises_http_errors(self, mock_post):
        response = Mock(status_code=400, text='bad request')
        response.raise_for_status.side_effect = requests.HTTPError('bad request')
        mock_post.return_value = response
        storage_segment = StorageSegment(write_key='test_write_key')

        with pytest.raises(requests.HTTPError):
            storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})
