import datetime

from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.library.collectors.others.total_workers_vcpu import (
    get_cpu_timeline,
    get_hour_boundaries,
    get_total_workers_cpu,
    total_workers_vcpu,
)


def test_get_hour_boundaries():
    """Test get_hour_boundaries calculates correct hour boundaries."""
    # Test with a specific timestamp: 2024-01-15 14:30:45
    current_ts = datetime.datetime(2024, 1, 15, 14, 30, 45, tzinfo=datetime.timezone.utc).timestamp()

    prev_start, prev_end = get_hour_boundaries(current_ts)

    # Previous hour should be 13:00:00 to 13:59:59
    expected_start = datetime.datetime(2024, 1, 15, 13, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    expected_end = datetime.datetime(2024, 1, 15, 13, 59, 59, tzinfo=datetime.timezone.utc).timestamp()

    assert prev_start == expected_start
    assert prev_end == expected_end


def test_get_hour_boundaries_on_hour():
    """Test get_hour_boundaries when current time is exactly on the hour."""
    # Test at exactly 10:00:00
    current_ts = datetime.datetime(2024, 6, 20, 10, 0, 0, tzinfo=datetime.timezone.utc).timestamp()

    prev_start, prev_end = get_hour_boundaries(current_ts)

    # Previous hour should be 09:00:00 to 09:59:59
    expected_start = datetime.datetime(2024, 6, 20, 9, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    expected_end = datetime.datetime(2024, 6, 20, 9, 59, 59, tzinfo=datetime.timezone.utc).timestamp()

    assert prev_start == expected_start
    assert prev_end == expected_end


def test_get_total_workers_cpu():
    """Test get_total_workers_cpu constructs correct query and returns value."""
    mock_prom = MagicMock()
    mock_prom.get_current_value.return_value = 42.0

    base_ts = 1234567890.0

    vcpu_value, query = get_total_workers_cpu(mock_prom, base_ts)

    # Should call get_current_value with the query
    mock_prom.get_current_value.assert_called_once()

    # Should return the value and query
    assert vcpu_value == pytest.approx(42.0)
    assert isinstance(query, str)
    assert 'max_over_time' in query
    assert 'sum(machine_cpu_cores)' in query
    assert '59m59s' in query
    assert str(int(base_ts)) in query


def test_get_total_workers_cpu_returns_none():
    """Test get_total_workers_cpu when Prometheus returns None."""
    mock_prom = MagicMock()
    mock_prom.get_current_value.return_value = None

    base_ts = 1234567890.0

    vcpu_value, query = get_total_workers_cpu(mock_prom, base_ts)

    assert vcpu_value is None
    assert isinstance(query, str)


def test_get_cpu_timeline():
    """Test get_cpu_timeline retrieves and formats timeline data."""
    mock_prom = MagicMock()

    # Mock response from Prometheus
    mock_prom.query_range.return_value = {'data': {'result': [{'values': [[1234567890.0, '10.0'], [1234568190.0, '12.0'], [1234568490.0, '11.0']]}]}}

    start_ts = 1234567800.0
    end_ts = 1234571400.0

    timeline = get_cpu_timeline(mock_prom, start_ts, end_ts)

    # Should call query_range
    mock_prom.query_range.assert_called_once()
    call_args = mock_prom.query_range.call_args

    assert call_args[1]['query'] == 'sum(machine_cpu_cores)'
    assert call_args[1]['start_time'] == pytest.approx(start_ts)
    assert call_args[1]['end_time'] == pytest.approx(end_ts)
    assert call_args[1]['step'] == '5m'

    # Should return list of timestamp/cpu_sum dicts
    assert isinstance(timeline, list)
    assert len(timeline) == 3
    assert 'timestamp' in timeline[0]
    assert 'cpu_sum' in timeline[0]
    assert timeline[0]['cpu_sum'] == pytest.approx(10.0)


def test_get_cpu_timeline_empty_result():
    """Test get_cpu_timeline with empty Prometheus response."""
    mock_prom = MagicMock()
    mock_prom.query_range.return_value = {'data': {'result': []}}

    start_ts = 1234567800.0
    end_ts = 1234571400.0

    timeline = get_cpu_timeline(mock_prom, start_ts, end_ts)

    assert isinstance(timeline, list)
    assert len(timeline) == 0


def test_get_cpu_timeline_sorting():
    """Test that get_cpu_timeline sorts results by timestamp."""
    mock_prom = MagicMock()

    # Unsorted values
    mock_prom.query_range.return_value = {'data': {'result': [{'values': [[1234568490.0, '11.0'], [1234567890.0, '10.0'], [1234568190.0, '12.0']]}]}}

    start_ts = 1234567800.0
    end_ts = 1234571400.0

    timeline = get_cpu_timeline(mock_prom, start_ts, end_ts)

    # Should be sorted
    assert len(timeline) == 3
    # First timestamp should be the earliest (1234567890)
    assert '2009-02-13' in timeline[0]['timestamp']  # 1234567890 is in Feb 2009
    assert timeline[0]['cpu_sum'] == pytest.approx(10.0)


@patch('metrics_utility.library.collectors.others.total_workers_vcpu.PrometheusClient')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.datetime')
def test_total_workers_vcpu_metering_disabled(mock_datetime, mock_prom_class):
    """Test total_workers_vcpu when metering is disabled."""
    # Mock current time
    mock_now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=datetime.timezone.utc)
    mock_datetime.now.return_value = mock_now
    mock_datetime.fromtimestamp = datetime.datetime.fromtimestamp
    mock_datetime.timezone = datetime.timezone

    instance = total_workers_vcpu(cluster_name='test-cluster', metering_enabled=False, prometheus_url='http://localhost:9090')
    result = instance.gather()

    # Should return result with total_workers_vcpu = 1
    assert result is not None
    assert result['cluster_name'] == 'test-cluster'
    assert result['total_workers_vcpu'] == 1
    assert 'timestamp' in result

    # Should not create PrometheusClient
    mock_prom_class.assert_not_called()


@patch('metrics_utility.library.collectors.others.total_workers_vcpu.get_total_workers_cpu')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.get_cpu_timeline')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.PrometheusClient')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.datetime')
def test_total_workers_vcpu_metering_enabled(mock_datetime, mock_prom_class, mock_timeline, mock_total_cpu):
    """Test total_workers_vcpu when metering is enabled."""
    # Mock current time
    mock_now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=datetime.timezone.utc)
    mock_datetime.now.return_value = mock_now
    mock_datetime.fromtimestamp = datetime.datetime.fromtimestamp
    mock_datetime.timezone = datetime.timezone

    # Mock Prometheus client
    mock_prom_instance = MagicMock()
    mock_prom_class.return_value = mock_prom_instance

    # Mock helper function returns
    mock_total_cpu.return_value = (16.0, 'test_query')
    mock_timeline.return_value = [{'timestamp': '2024-01-15T13:00:00+00:00', 'cpu_sum': 16.0}]

    instance = total_workers_vcpu(
        cluster_name='test-cluster',
        metering_enabled=True,
        prometheus_url='http://localhost:9090',
        ca_cert_path='/path/to/ca.crt',
        token='test-token',
    )
    result = instance.gather()

    # Should create PrometheusClient
    mock_prom_class.assert_called_once_with(url='http://localhost:9090', ca_cert_path='/path/to/ca.crt', token='test-token')

    # Should call helper functions
    mock_total_cpu.assert_called_once()
    mock_timeline.assert_called_once()

    # Should return result with actual CPU value
    assert result is not None
    assert result['cluster_name'] == 'test-cluster'
    assert result['total_workers_vcpu'] == 16
    assert 'timestamp' in result


@patch('metrics_utility.library.collectors.others.total_workers_vcpu.get_total_workers_cpu')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.get_cpu_timeline')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.PrometheusClient')
@patch('metrics_utility.library.collectors.others.total_workers_vcpu.datetime')
def test_total_workers_vcpu_no_data_available(mock_datetime, mock_prom_class, mock_timeline, mock_total_cpu):
    """Test total_workers_vcpu when Prometheus returns no data."""
    # Mock current time
    mock_now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=datetime.timezone.utc)
    mock_datetime.now.return_value = mock_now
    mock_datetime.fromtimestamp = datetime.datetime.fromtimestamp
    mock_datetime.timezone = datetime.timezone

    # Mock Prometheus client
    mock_prom_instance = MagicMock()
    mock_prom_class.return_value = mock_prom_instance

    # Mock no data available
    mock_total_cpu.return_value = (None, 'test_query')
    mock_timeline.return_value = []

    instance = total_workers_vcpu(cluster_name='test-cluster', metering_enabled=True, prometheus_url='http://localhost:9090')
    result = instance.gather()

    # Should return None when no data
    assert result is None


def test_total_workers_vcpu_collector_decorator():
    """Test that total_workers_vcpu uses @collector decorator."""
    instance = total_workers_vcpu(cluster_name='test', metering_enabled=False)

    # Should have gather method from decorator
    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
