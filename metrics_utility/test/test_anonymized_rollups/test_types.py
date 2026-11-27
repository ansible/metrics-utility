"""Tests for the AnonymizedPayload Segment contract."""

import pytest

from pydantic import ConfigDict, TypeAdapter, ValidationError, with_config

from metrics_utility.anonymized_rollups.anonymized_rollups import flatten_json_report
from metrics_utility.anonymized_rollups.types import AnonymizedPayload


StrictPayload = with_config(ConfigDict(extra='forbid'))(AnonymizedPayload)
adapter = TypeAdapter(StrictPayload)


def _base_payload():
    return flatten_json_report({})


def _payload_with_service_telemetry():
    payload = _base_payload()
    payload['summary_metadata'] = {
        'summary_date': '2026-11-26',
        'hourly_collections_count': 23,
        'missing_hours': ['unified_jobs:5', 12],
        'aggregation_timestamp': '2026-11-27T01:02:03+00:00',
        'install_type': 'openshift',
    }
    payload['dashboard_telemetry'] = [
        {
            'task_name': 'sync_dashboard_job_records',
            'success': True,
            'collection_duration_ms': 12.5,
            'number_of_records_processed': 4,
            'database_query_time_ms': None,
            'cache_hit_rate': 0.75,
        }
    ]
    payload['leaderboard_telemetry'] = {
        'enabled': True,
        'observed_at': '2026-11-27T01:02:03+00:00',
    }
    payload['api_usage_telemetry'] = {
        'available': True,
        'total_requests': 3,
        'by_endpoint': {'analytics:v1:root': {'2xx': 3, '4xx': 0}},
    }
    return payload


def test_accepts_payload_without_service_telemetry_keys():
    payload = _base_payload()

    validated = adapter.validate_python(payload, strict=True)

    assert validated == payload
    for key in ('summary_metadata', 'dashboard_telemetry', 'leaderboard_telemetry', 'api_usage_telemetry'):
        assert key not in validated


def test_accepts_payload_with_service_telemetry_keys():
    payload = _payload_with_service_telemetry()

    validated = adapter.validate_python(payload, strict=True)

    assert validated['summary_metadata']['missing_hours'] == ['unified_jobs:5', 12]
    assert validated['dashboard_telemetry'][0]['cache_hit_rate'] == 0.75
    assert validated['leaderboard_telemetry']['enabled'] is True
    assert validated['api_usage_telemetry']['by_endpoint']['analytics:v1:root']['2xx'] == 3


def test_accepts_summary_metadata_without_install_type():
    payload = _payload_with_service_telemetry()
    del payload['summary_metadata']['install_type']

    validated = adapter.validate_python(payload, strict=True)

    assert 'install_type' not in validated['summary_metadata']


def test_rejects_unknown_keys():
    payload = {**_base_payload(), 'unknown_key': 'value'}

    with pytest.raises(ValidationError) as excinfo:
        adapter.validate_python(payload, strict=True)

    assert excinfo.value.errors()[0]['type'] == 'extra_forbidden'


def test_rejects_invalid_service_telemetry_types():
    payload = _payload_with_service_telemetry()
    payload['summary_metadata']['hourly_collections_count'] = '23'

    with pytest.raises(ValidationError) as excinfo:
        adapter.validate_python(payload, strict=True)

    assert any(err['loc'] == ('summary_metadata', 'hourly_collections_count') for err in excinfo.value.errors())
