"""Tests for helper functions in total_workers_vcpu module."""

from unittest.mock import MagicMock

import pytest

from metrics_utility.library.collectors.others.total_workers_vcpu import (
    get_cpu_timeline,
    get_total_workers_cpu,
    timestamp_format,
)
from metrics_utility.test.util import utcdt


# timestamp_format


def test_timestamp_format_basic():
    ts = utcdt('2024-01-01T12:30:45.123').timestamp()

    result = timestamp_format(ts)

    assert result == '2024-01-01T12:30:45.123Z'
    assert result.endswith('Z')


def test_timestamp_format_midnight():
    ts = utcdt('2024-01-01').timestamp()

    result = timestamp_format(ts)

    assert result == '2024-01-01T00:00:00.000Z'


def test_timestamp_format_with_milliseconds():
    ts = utcdt('2024-01-01T12:30:45.999').timestamp()

    result = timestamp_format(ts)

    assert result == '2024-01-01T12:30:45.999Z'
    assert '.999Z' in result


def test_timestamp_format_no_timezone_offset():
    ts = utcdt('2024-06-15T08:45:30.500').timestamp()

    result = timestamp_format(ts)

    assert '+00:00' not in result
    assert result.endswith('Z')


# get_total_workers_cpu


def test_get_total_workers_cpu_success():
    mock_prom = MagicMock()
    mock_prom.get_current_value.return_value = 16.0
    base_ts = 1234567890.0

    cpu_val, query = get_total_workers_cpu(mock_prom, base_ts)

    assert cpu_val == pytest.approx(16.0)
    assert 'max_over_time' in query
    assert 'sum(machine_cpu_cores)' in query
    assert '[59m59s999ms:5m]' in query
    assert f'@ {base_ts}' in query
    mock_prom.get_current_value.assert_called_once()


def test_get_total_workers_cpu_query_format():
    mock_prom = MagicMock()
    mock_prom.get_current_value.return_value = 8.0
    base_ts = 1704067200.0  # 2024-01-01 00:00:00 UTC

    _, query = get_total_workers_cpu(mock_prom, base_ts)

    expected = f'max_over_time(sum(machine_cpu_cores)[59m59s999ms:5m] @ {base_ts})'
    assert query == expected


def test_get_total_workers_cpu_returns_float():
    mock_prom = MagicMock()
    mock_prom.get_current_value.return_value = 24.5
    base_ts = 1234567890.0

    cpu_val, _ = get_total_workers_cpu(mock_prom, base_ts)

    assert isinstance(cpu_val, float)
    assert cpu_val == pytest.approx(24.5)


def test_get_total_workers_cpu_none_result():
    mock_prom = MagicMock()
    mock_prom.get_current_value.return_value = None
    base_ts = 1234567890.0

    cpu_val, query = get_total_workers_cpu(mock_prom, base_ts)

    assert cpu_val is None
    assert isinstance(query, str)


# get_cpu_timeline


def test_get_cpu_timeline_success():
    mock_prom = MagicMock()
    mock_response = {
        'data': {
            'result': [
                {
                    'values': [
                        [1704067200, '16.0'],
                        [1704067500, '16.0'],
                        [1704067800, '18.0'],
                    ]
                }
            ]
        }
    }
    mock_prom.query_range.return_value = mock_response

    start_ts = 1704067200.0
    end_ts = 1704070799.999

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert len(result) == 3
    assert result[0]['timestamp'] == '2024-01-01T00:00:00.000Z'
    assert result[0]['cpu_sum'] == pytest.approx(16.0)
    assert result[1]['cpu_sum'] == pytest.approx(16.0)
    assert result[2]['cpu_sum'] == pytest.approx(18.0)


def test_get_cpu_timeline_query_parameters():
    mock_prom = MagicMock()
    mock_prom.query_range.return_value = {'data': {'result': []}}

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    get_cpu_timeline(mock_prom, start_ts, end_ts)

    mock_prom.query_range.assert_called_once_with(query='sum(machine_cpu_cores)', start_time=start_ts, end_time=end_ts, step='5m')


def test_get_cpu_timeline_empty_result():
    mock_prom = MagicMock()
    mock_prom.query_range.return_value = {'data': {'result': []}}

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert result == []


def test_get_cpu_timeline_no_data_field():
    mock_prom = MagicMock()
    mock_prom.query_range.return_value = {}

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert result == []


def test_get_cpu_timeline_sorted_by_timestamp():
    mock_prom = MagicMock()
    mock_response = {
        'data': {
            'result': [
                {
                    'values': [
                        [1704067800, '18.0'],  # Later
                        [1704067200, '16.0'],  # Earlier
                        [1704067500, '17.0'],  # Middle
                    ]
                }
            ]
        }
    }
    mock_prom.query_range.return_value = mock_response

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert len(result) == 3
    assert result[0]['timestamp'] < result[1]['timestamp']
    assert result[1]['timestamp'] < result[2]['timestamp']
    assert result[0]['cpu_sum'] == pytest.approx(16.0)
    assert result[1]['cpu_sum'] == pytest.approx(17.0)
    assert result[2]['cpu_sum'] == pytest.approx(18.0)


def test_get_cpu_timeline_series_without_values():
    mock_prom = MagicMock()
    mock_response = {
        'data': {
            'result': [
                {'metric': {'instance': 'node1'}},
                {'values': [[1704067200, '16.0']]},
            ]
        }
    }
    mock_prom.query_range.return_value = mock_response

    result = get_cpu_timeline(mock_prom, 1704067200.0, 1704070800.0)

    assert len(result) == 1
    assert result[0]['cpu_sum'] == pytest.approx(16.0)


def test_get_cpu_timeline_multiple_series():
    mock_prom = MagicMock()
    mock_response = {
        'data': {
            'result': [
                {'values': [[1704067200, '8.0'], [1704067500, '8.0']]},
                {'values': [[1704067200, '8.0'], [1704067500, '8.0']]},
            ]
        }
    }
    mock_prom.query_range.return_value = mock_response

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert len(result) == 4


def test_get_cpu_timeline_float_conversion():
    mock_prom = MagicMock()
    mock_response = {'data': {'result': [{'values': [[1704067200, '24.5'], [1704067500, '32']]}]}}
    mock_prom.query_range.return_value = mock_response

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert isinstance(result[0]['cpu_sum'], float)
    assert result[0]['cpu_sum'] == pytest.approx(24.5)
    assert isinstance(result[1]['cpu_sum'], float)
    assert result[1]['cpu_sum'] == pytest.approx(32.0)


def test_get_cpu_timeline_timestamp_format():
    mock_prom = MagicMock()
    mock_response = {'data': {'result': [{'values': [[1704067200, '16.0']]}]}}
    mock_prom.query_range.return_value = mock_response

    start_ts = 1704067200.0
    end_ts = 1704070800.0

    result = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert result[0]['timestamp'].endswith('Z')
    assert '+00:00' not in result[0]['timestamp']
    assert 'T' in result[0]['timestamp']
