import datetime
import gzip
import json

from decimal import Decimal
from enum import Enum
from unittest.mock import Mock, patch

import pytest
import requests

from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.test.library.testing_data_for_segment import segment_data, segment_data_large


class ExampleState(Enum):
    READY = 'ready'
    DONE = 'done'


def reference_split_into_chunks(storage_segment, data, max_size):
    """Naive O(K^2) list splitter kept as the behavioural reference."""
    chunks = []
    for key, value in data.items():
        if isinstance(value, list):
            active_chunk = {key: []}
            for item in value:
                trial = {key: active_chunk[key] + [item]}
                if storage_segment._calculate_size(trial) > max_size:
                    if len(active_chunk[key]) > 0:
                        chunks.append(active_chunk)
                        active_chunk = {key: [item]}
                    else:
                        chunks.append({key: [item]})
                else:
                    active_chunk[key].append(item)
            if len(active_chunk[key]) > 0:
                chunks.append(active_chunk)
        else:
            chunks.append({key: value})
    return chunks or [data]


class TestStorageSegmentAvailable:
    def test_correct_splitting_for_small_data(self):
        """Split the standard fixture into one chunk per top-level section."""
        storage_segment = StorageSegment()
        chunks = storage_segment._split_into_chunks(segment_data, storage_segment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 5
        assert 'statistics' in chunks[0]
        assert 'module_stats' in chunks[1]
        assert 'collection_stats' in chunks[2]
        assert 'jobs_by_job_type' in chunks[3]
        assert 'job_host_summary' in chunks[4]

    def test_correct_splitting_for_large_data(self):
        """Split large list data into ordered chunks at the regular limit."""
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
        assert len(chunks[1]['module_stats']) == 54
        assert len(chunks[2]['module_stats']) == 53
        assert len(chunks[3]['module_stats']) == 5

    def test_simple_list_data(self):
        """Keep a small list in a single artifact chunk."""
        data = {'test_list': ['item1', 'item2']}
        chunks = StorageSegment()._split_into_chunks(data, StorageSegment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 1
        assert chunks[0]['test_list'] == data['test_list']

    def test_simple_list_large_data(self):
        """Split a large list while preserving item counts and ordering."""
        data = {'test_list': [f'item{i}' for i in range(3000)]}
        chunks = StorageSegment()._split_into_chunks(data, StorageSegment.REGULAR_MESSAGE_LIMIT)
        assert len(chunks) == 1
        assert len(chunks[0]['test_list']) == 3000

    def test_preserves_other_top_level_values(self):
        """Keep scalar and set values when another key already made a chunk."""
        data = {'section': {'value': 1}, 'count': 2, 'labels': {'red', 'blue'}}

        chunks = StorageSegment()._split_into_chunks(data, StorageSegment.REGULAR_MESSAGE_LIMIT)

        assert chunks[0] == {'section': {'value': 1}}
        assert chunks[1] == {'count': 2}
        assert chunks[2]['labels'] == {'red', 'blue'}

    def test_rollup_period_string_arrays(self):
        """Split each rollup-period array into its own chunk."""
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
        """Send a compressed batch containing valid Segment track fields."""
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
        assert event['properties']['chunk_info']['chunk_size'] == len(storage_segment._json_bytes(event['properties']['data']))
        assert event['properties']['version'] == 'v1'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_uses_explicit_version_in_properties(self, mock_post):
        """Use an explicitly supplied schema version in Segment properties."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'first': {'value': 'one'}, 'second': {'value': 'two'}},
            event_name='Test Event',
            segment_meta={'version': 'v2'},
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert len(payload['batch']) == 2
        for event in payload['batch']:
            assert event['properties']['version'] == 'v2'
            assert 'version' not in event

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_falsy_version_falls_back_to_default(self, mock_post):
        """Treat a falsy segment_meta version as omitted."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'first': {'value': 'one'}},
            segment_meta={'version': None},
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert payload['batch'][0]['properties']['version'] == 'v1'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_accepts_anonymous_id(self, mock_post):
        """Reuse a caller-provided anonymous ID across all events."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'first': {'value': 'one'}, 'second': {'value': 'two'}},
            anonymous_id='daily-anonymous-id',
            segment_meta={'message_id': 'daily-upload-id'},
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert {event['anonymousId'] for event in payload['batch']} == {'daily-anonymous-id'}

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_sends_multiple_chunks_in_one_batch(self, mock_post):
        """Place multiple artifact chunks in one batch request when it fits."""
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
        """Reject invalid non-positive chunk size limits."""
        storage_segment = StorageSegment()
        with pytest.raises(ValueError, match='max_size must be positive'):
            storage_segment._split_into_chunks({'key': [1, 2, 3]}, 0)
        with pytest.raises(ValueError, match='max_size must be positive'):
            storage_segment._split_into_chunks({'key': [1, 2, 3]}, -100)

    def test_split_into_chunks_warns_on_oversized_dict(self, caplog):
        """Warn but preserve an oversized dictionary chunk."""
        storage_segment = StorageSegment()
        data = {'big': {'a': 'x' * 500}}
        chunks = storage_segment._split_into_chunks(data, 50)
        assert len(chunks) == 1
        assert chunks[0] == data
        assert 'Oversized dict chunk' in caplog.text

    def test_split_into_chunks_warns_on_oversized_single_list_item(self, caplog):
        """Warn but preserve an oversized individual list item."""
        storage_segment = StorageSegment()
        data = {'items': ['x' * 500]}
        chunks = storage_segment._split_into_chunks(data, 50)
        assert len(chunks) == 1
        assert chunks[0]['items'] == ['x' * 500]
        assert 'Single list item' in caplog.text

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_serializes_sdk_compatible_values(self, mock_post):
        """Serialize Decimal, Enum, and set values before sending JSON."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'metrics': {'value': Decimal('1.25'), 'labels': {'red', 'blue'}, 'state': Enum('State', {'READY': 'ready'}).READY}},
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        data = payload['batch'][0]['properties']['data']['metrics']
        assert data['value'] == 1.25
        assert set(data['labels']) == {'red', 'blue'}
        assert data['state'] == 'ready'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_canonicalizes_timestamp_for_generated_message_ids(self, mock_post):
        """Equivalent datetime and ISO timestamps produce the same fallback ID."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')
        common = {
            'artifact_name': 'test_artifact',
            'dict': {'statistics': {'count': 1}},
            'anonymous_id': 'stable-anonymous-id',
        }

        storage_segment.put(**common, segment_meta={'timestamp': datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)})
        first_payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        storage_segment.put(**common, segment_meta={'timestamp': '2026-01-01T00:00:00Z'})
        second_payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))

        assert first_payload['batch'][0]['messageId'] == second_payload['batch'][0]['messageId']
        assert first_payload['batch'][0]['timestamp'] == second_payload['batch'][0]['timestamp'] == '2026-01-01T00:00:00+00:00'

    @patch('metrics_utility.library.storage.segment.requests.post')
    @patch('metrics_utility.library.storage.segment.SEGMENT_AVAILABLE', False)
    def test_put_skips_upload_when_transport_is_unavailable(self, mock_post):
        """Preserve the service compatibility marker when requests is absent."""
        storage_segment = StorageSegment(write_key='test_write_key')

        assert storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}}) is None
        mock_post.assert_not_called()

    def test_split_into_batches_rejects_oversized_event_after_flush(self):
        """Reject an oversized event even after flushing a prior batch."""
        storage_segment = StorageSegment()
        storage_segment.BATCH_SIZE_LIMIT = 100
        small_event = {'messageId': 'small', 'properties': {'value': 'x'}}
        oversized_event = {'messageId': 'large', 'properties': {'value': 'x' * 200}}

        with pytest.raises(ValueError, match='Single Segment event exceeds'):
            storage_segment._split_into_batches([small_event, oversized_event], '2026-01-01T00:00:00+00:00')

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_preserves_segment_meta(self, mock_post):
        """Derive distinct deterministic message IDs for metadata-based sends."""
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
    def test_put_reuses_message_ids_for_retryable_metadata(self, mock_post):
        """Reuse message IDs when retry metadata and event identity are stable."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')
        metadata = {'message_id': 'stable-upload', 'timestamp': '2026-01-01T00:00:00+00:00'}
        kwargs = {'artifact_name': 'test', 'dict': {'first': {'value': 'one'}}, 'anonymous_id': 'stable-anonymous-id', 'segment_meta': metadata}

        storage_segment.put(**kwargs)
        first_message_id = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))['batch'][0]['messageId']
        storage_segment.put(**kwargs)
        second_message_id = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))['batch'][0]['messageId']

        assert first_message_id == second_message_id

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_serializes_nested_datetime_metadata(self, mock_post):
        """Serialize datetime values nested in Segment metadata dictionaries."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test',
            dict={'first': {'value': 'one'}},
            anonymous_id='stable-anonymous-id',
            segment_meta={
                'message_id': 'stable-upload',
                'context': {'nested': {'created_at': datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)}},
                'integrations': {'warehouse': {'updated_at': datetime.date(2026, 1, 2)}},
            },
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert payload['batch'][0]['context']['nested']['created_at'] == '2026-01-01T00:00:00+00:00'
        assert payload['batch'][0]['integrations']['warehouse']['updated_at'] == '2026-01-02'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_splits_oversized_batches(self, mock_post):
        """Split events across multiple requests when the batch limit is reached."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', debug=False)
        storage_segment.BATCH_SIZE_LIMIT = 40000

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
        """Allow callers to send an uncompressed JSON request body."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', gzip=False)

        storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        request = mock_post.call_args.kwargs
        assert 'Content-Encoding' not in request['headers']
        assert json.loads(request['data'])['batch']

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_raises_http_errors(self, mock_post):
        """Propagate HTTP failures so dispatcherd can retry the task."""
        response = Mock(status_code=400, text='bad request')
        response.raise_for_status.side_effect = requests.HTTPError('bad request')
        mock_post.return_value = response
        storage_segment = StorageSegment(write_key='test_write_key')

        with pytest.raises(requests.HTTPError):
            storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_rejects_insecure_remote_host(self, mock_post):
        """Reject dotted-hostname HTTP hosts even when allow_insecure_host is set."""
        storage_segment = StorageSegment(write_key='test_write_key', host='http://segment.example.test', allow_insecure_host=True)

        with pytest.raises(ValueError, match='must use HTTPS'):
            storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        mock_post.assert_not_called()

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_allows_docker_compose_test_host(self, mock_post):
        """Allow a Docker Compose service hostname when allow_insecure_host is set."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', host='http://mock-segment:5000', allow_insecure_host=True)

        storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        assert mock_post.call_args.args[0] == 'http://mock-segment:5000/v1/batch'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_allows_explicit_loopback_test_host(self, mock_post):
        """Allow an explicitly opted-in loopback HTTP mock server."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', host='http://localhost:8765', allow_insecure_host=True)

        storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        assert mock_post.call_args.args[0] == 'http://localhost:8765/v1/batch'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_allows_loopback_host_without_internal_host_opt_in(self, mock_post):
        """Keep existing loopback integration callers working without extra settings."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', host='http://localhost:8765', allow_insecure_host=False)

        storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        assert mock_post.call_args.args[0] == 'http://localhost:8765/v1/batch'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_rejects_internal_test_host_without_opt_in(self, mock_post):
        """Require the opt-in for non-loopback compose-style HTTP hosts."""
        storage_segment = StorageSegment(write_key='test_write_key', host='http://mock-segment:5000', allow_insecure_host=False)

        with pytest.raises(ValueError, match='HTTPS'):
            storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        mock_post.assert_not_called()

    def test_put_skips_upload_without_write_key(self):
        """Skip uploads when Segment credentials are not configured."""
        storage_segment = StorageSegment(debug=True)

        assert storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}}) is None

    def test_put_rejects_unsupported_file_arguments(self):
        """Reject filename and file-object inputs unsupported by this backend."""
        storage_segment = StorageSegment(write_key='test_write_key')

        with pytest.raises(Exception, match='not supported'):
            storage_segment.put(artifact_name='test_artifact', filename='artifact.json', dict={})

    def test_put_requires_dict_data(self):
        """Report a targeted error when artifact data is omitted."""
        storage_segment = StorageSegment(write_key='test_write_key')

        with pytest.raises(ValueError, match='requires dict='):
            storage_segment.put(artifact_name='test_artifact')

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_logs_and_raises_network_errors_in_debug_mode(self, mock_post):
        """Log transport failures in debug mode while preserving the exception."""
        mock_post.side_effect = requests.Timeout('request timed out')
        storage_segment = StorageSegment(write_key='test_write_key', debug=True)

        with pytest.raises(requests.Timeout):
            storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_event_stays_within_segment_message_limit(self, mock_post):
        """Each built Segment event must fit within the 32 KB per-message limit."""
        mock_post.return_value = Mock(status_code=200, text='{"success":true}')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(artifact_name='test_artifact', dict=segment_data_large, event_name='Test Event')

        for call in mock_post.call_args_list:
            payload = json.loads(gzip.decompress(call.kwargs['data']))
            for event in payload['batch']:
                event_bytes = len(json.dumps(event, separators=(',', ':')).encode('utf-8'))
                assert event_bytes <= StorageSegment.REGULAR_MESSAGE_LIMIT, (
                    f'Event exceeded limit: {event_bytes} > {StorageSegment.REGULAR_MESSAGE_LIMIT}'
                )

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_handles_separator_light_data_without_oversized_events(self, mock_post):
        """Use wire-size accounting for data with few JSON separators."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'items': ['x' * 200] * 1000},
            segment_meta={'context': {'deployment': 'c' * 1000}},
        )

        events = []
        for call in mock_post.call_args_list:
            events.extend(json.loads(gzip.decompress(call.kwargs['data']))['batch'])
        assert events
        assert all(len(storage_segment._json_bytes(event)) <= storage_segment.REGULAR_MESSAGE_LIMIT for event in events)

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_rejects_indivisible_oversized_event_before_post(self, mock_post):
        """Reject a single oversized list item before sending any batch."""
        storage_segment = StorageSegment(write_key='test_write_key')

        with pytest.raises(ValueError, match='message limit'):
            storage_segment.put(artifact_name='test_artifact', dict={'items': ['x' * 32375]})

        mock_post.assert_not_called()

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_falls_back_to_constructor_user_id(self, mock_post):
        """Include the constructor-level user_id when segment_meta omits it."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', user_id='my-user-id')

        storage_segment.put(artifact_name='test_artifact', dict={'statistics': {'count': 1}})

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert payload['batch'][0]['userId'] == 'my-user-id'

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_put_segment_meta_user_id_overrides_constructor(self, mock_post):
        """Let segment_meta user_id override the constructor-level value."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key', user_id='constructor-id')

        storage_segment.put(
            artifact_name='test_artifact',
            dict={'statistics': {'count': 1}},
            segment_meta={'user_id': 'meta-id'},
        )

        payload = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))
        assert payload['batch'][0]['userId'] == 'meta-id'


class TestSplitIntoChunksIncrementalSizing:
    """Boundary and parity coverage for the incremental list chunk sizing."""

    @staticmethod
    def _exact_size(storage_segment, key, items):
        return storage_segment._calculate_size({key: items})

    def test_exact_boundary_size_keeps_single_chunk(self):
        """Keep the whole list when max_size equals the exact serialized size."""
        storage_segment = StorageSegment()
        items = ['aa', 'bbb', 'c']
        exact = self._exact_size(storage_segment, 'items', items)

        chunks = storage_segment._split_into_chunks({'items': items}, exact)

        assert len(chunks) == 1
        assert chunks[0]['items'] == items
        assert storage_segment._calculate_size(chunks[0]) == exact

    def test_one_byte_below_exact_size_splits(self):
        """Split when max_size is one byte short of the exact serialized size."""
        storage_segment = StorageSegment()
        items = ['aa', 'bbb', 'c']
        exact = self._exact_size(storage_segment, 'items', items)

        chunks = storage_segment._split_into_chunks({'items': items}, exact - 1)

        assert len(chunks) > 1
        assert [item for chunk in chunks for item in chunk['items']] == items
        assert all(storage_segment._calculate_size(chunk) <= exact - 1 for chunk in chunks)

    def test_one_byte_above_exact_size_keeps_single_chunk(self):
        """Keep the whole list when max_size is one byte above the exact size."""
        storage_segment = StorageSegment()
        items = ['aa', 'bbb', 'c']
        exact = self._exact_size(storage_segment, 'items', items)

        chunks = storage_segment._split_into_chunks({'items': items}, exact + 1)

        assert len(chunks) == 1
        assert chunks[0]['items'] == items

    def test_boundaries_use_utf8_byte_lengths(self):
        """Measure multi-byte keys and values in UTF-8 bytes, not characters."""
        storage_segment = StorageSegment()
        key = 'clé_😀'
        items = ['héllo', '漢字', '😀😀']
        exact = self._exact_size(storage_segment, key, items)

        # Character counts are much smaller than the UTF-8 byte length.
        character_length = len(json.dumps({key: items}, separators=(',', ':'), ensure_ascii=False))
        assert exact > character_length
        assert len(storage_segment._split_into_chunks({key: items}, exact)) == 1
        assert len(storage_segment._split_into_chunks({key: items}, exact + 1)) == 1

        tighter = storage_segment._split_into_chunks({key: items}, exact - 1)
        assert len(tighter) > 1
        assert [item for chunk in tighter for item in chunk[key]] == items
        assert all(storage_segment._calculate_size(chunk) <= exact - 1 for chunk in tighter)

    def test_multi_byte_items_fill_chunks_exactly(self):
        """Pack multi-byte items right up to the limit using exact byte sizes."""
        storage_segment = StorageSegment()
        items = ['😀', 'é', '漢字', 'ok']
        key = 'items'
        base = self._exact_size(storage_segment, key, [])

        # Room for exactly the first two items (with separators).
        first_two = base + storage_segment._calculate_size('😀') + 1 + storage_segment._calculate_size('é')
        chunks = storage_segment._split_into_chunks({key: items}, first_two)

        assert chunks[0][key] == ['😀', 'é']
        assert storage_segment._calculate_size(chunks[0]) == first_two
        assert len(chunks) > 1

    def test_splits_serialized_non_json_native_items(self):
        """Account for serialized lengths of datetime, date, Decimal, Enum, and set items."""
        storage_segment = StorageSegment()
        items = [
            datetime.datetime(2026, 1, 1, 12, 30, tzinfo=datetime.UTC),
            datetime.date(2026, 1, 2),
            Decimal('1.25'),
            ExampleState.READY,
            {'label': 'é'},
        ]
        exact = self._exact_size(storage_segment, 'events', items)

        assert len(storage_segment._split_into_chunks({'events': items}, exact)) == 1
        split = storage_segment._split_into_chunks({'events': items}, exact - 1)
        assert len(split) > 1
        assert [item for chunk in split for item in chunk['events']] == items
        assert all(storage_segment._calculate_size(chunk) <= exact - 1 for chunk in split)

    def test_oversized_single_item_at_list_start(self, caplog):
        """Warn once and emit an oversized leading item as its own chunk."""
        storage_segment = StorageSegment()
        oversized = 'x' * 500
        chunks = storage_segment._split_into_chunks({'items': [oversized, 'a', 'b']}, 50)

        assert chunks[0]['items'] == [oversized]
        assert chunks[1]['items'] == ['a', 'b']
        assert caplog.text.count('Single list item') == 1

    def test_oversized_item_after_non_empty_chunk(self, caplog):
        """Flush the active chunk then emit the oversized item, without warning."""
        storage_segment = StorageSegment()
        oversized = 'x' * 500
        chunks = storage_segment._split_into_chunks({'items': ['a', 'b', oversized, 'c']}, 50)

        assert chunks == [{'items': ['a', 'b']}, {'items': [oversized]}, {'items': ['c']}]
        assert 'Single list item' not in caplog.text

    def test_oversized_shared_payload_preserved(self, caplog):
        """Emit oversized items without truncation and keep every item in order."""
        storage_segment = StorageSegment()
        data = {'items': [{'payload': 'y' * 200}], 'other': [{'payload': 'z' * 200}]}
        chunks = storage_segment._split_into_chunks(data, 60)

        assert [chunk['items'][0]['payload'] for chunk in chunks if 'items' in chunk] == ['y' * 200]
        assert [chunk['other'][0]['payload'] for chunk in chunks if 'other' in chunk] == ['z' * 200]
        assert caplog.text.count('Single list item') == 2

    @pytest.mark.parametrize(
        'items',
        [
            [f'item{i}' for i in range(40)],
            ['😀', 'é', '漢字', 'ascii', 'x' * 40] * 6,
            [{'i': i, 'label': 'é😀'} for i in range(30)],
            [datetime.date(2026, 1, (i % 28) + 1) for i in range(20)],
            [Decimal(f'{i}.5') for i in range(20)],
            [ExampleState.READY if i % 2 else ExampleState.DONE for i in range(20)],
            [1, 'two', None, True, 2.5, ['nested', '😀'], {'k': 'v'}] * 8,
            ['x' * 30, 'y' * 30] * 10,
        ],
    )
    def test_matches_reference_split_and_incremental_sizes(self, items):
        """Match the naive reference split and the tracked incremental sizes."""
        storage_segment = StorageSegment()
        key = 'items'
        base = self._exact_size(storage_segment, key, [])
        item_sizes = [storage_segment._calculate_size(item) for item in items]
        total = base + sum(item_sizes) + max(len(items) - 1, 0)

        for max_size in (total, total - 1, total // 2, total // 3, total // 5, 60, 100, 200):
            if max_size <= 0:
                continue
            chunks = storage_segment._split_into_chunks({key: items}, max_size)
            assert chunks == reference_split_into_chunks(storage_segment, {key: items}, max_size)

            # Every non-oversized chunk matches base + items + separators exactly.
            for chunk in chunks:
                chunk_items = chunk[key]
                expected = base + sum(storage_segment._calculate_size(item) for item in chunk_items)
                expected += max(len(chunk_items) - 1, 0)
                assert storage_segment._calculate_size(chunk) == expected
                if len(chunk_items) == 1:
                    continue
                assert storage_segment._calculate_size(chunk) <= max_size

    def test_incremental_size_matches_after_oversized_item(self):
        """Keep exact size accounting after an oversized item resets the chunk."""
        storage_segment = StorageSegment()
        key = 'items'
        items = ['a', 'x' * 500, 'b', 'c']
        chunks = storage_segment._split_into_chunks({key: items}, 50)

        assert [chunk[key] for chunk in chunks] == [['a'], ['x' * 500], ['b', 'c']]
        base = self._exact_size(storage_segment, key, [])
        last = chunks[-1][key]
        expected = base + sum(storage_segment._calculate_size(item) for item in last) + len(last) - 1
        assert storage_segment._calculate_size(chunks[-1]) == expected

    @patch('metrics_utility.library.storage.segment.requests.post')
    def test_build_event_reports_serialized_chunk_size(self, mock_post):
        """Report the serialized chunk size and serialize properties data once."""
        mock_post.return_value = Mock(status_code=200, text='')
        storage_segment = StorageSegment(write_key='test_write_key')
        chunk = {'metrics': {'value': Decimal('1.25'), 'labels': ['é', '😀']}}

        storage_segment.put(artifact_name='test_artifact', dict=chunk)

        event = json.loads(gzip.decompress(mock_post.call_args.kwargs['data']))['batch'][0]
        assert event['properties']['chunk_info']['chunk_size'] == len(storage_segment._json_bytes(event['properties']['data']))
        assert event['properties']['data'] == {'metrics': {'value': 1.25, 'labels': ['é', '😀']}}

    def test_build_properties_serializes_data_by_default(self):
        """Direct callers of _build_properties still get value serialization by default."""
        storage_segment = StorageSegment()
        props = storage_segment._build_properties(
            artifact_name='test_artifact',
            data={'amount': Decimal('2.50'), 'day': datetime.date(2026, 11, 26)},
            chunk_number=1,
            total_chunks=1,
            chunk_size=42,
        )

        assert props['data'] == {'amount': 2.5, 'day': '2026-11-26'}
