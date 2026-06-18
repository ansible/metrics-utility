"""Integration tests for PrometheusClient against the mock-prometheus-server container."""

import json
import os
import urllib.request

import pytest

from metrics_utility.library.collectors.others.total_workers_vcpu import (
    PrometheusClient,
    get_cpu_timeline,
    get_total_workers_cpu,
)


MOCK_PROMETHEUS_URL = os.getenv('MOCK_PROMETHEUS_URL', 'http://localhost:9090')


def reset_mock():
    req = urllib.request.Request(f'{MOCK_PROMETHEUS_URL}/reset', method='POST')
    urllib.request.urlopen(req, timeout=5)


def configure_mock(**kwargs):
    data = json.dumps(kwargs).encode()
    req = urllib.request.Request(f'{MOCK_PROMETHEUS_URL}/config', data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    urllib.request.urlopen(req, timeout=5)


def get_captured_requests():
    with urllib.request.urlopen(f'{MOCK_PROMETHEUS_URL}/requests', timeout=5) as resp:
        return json.loads(resp.read())


class TestPrometheusClientIntegration:
    def setup_method(self):
        reset_mock()
        configure_mock(cpu_value='16', empty_result=False)

    def test_instant_query(self):
        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        result = client.query('sum(machine_cpu_cores)')

        assert len(result) == 1
        assert float(result[0]['value'][1]) == pytest.approx(16.0)

        captured = get_captured_requests()
        assert len(captured) == 1
        assert captured[0]['path'] == '/api/v1/query'
        assert captured[0]['params']['query'] == 'sum(machine_cpu_cores)'

    def test_instant_query_with_time_param(self):
        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        result = client.query('up', time_param=1700000000.0)

        assert len(result) == 1
        assert float(result[0]['value'][0]) == pytest.approx(1700000000.0)

    def test_range_query(self):
        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        start = 1700000000.0
        end = 1700003600.0  # 1 hour later
        result = client.query_range('sum(machine_cpu_cores)', start_time=start, end_time=end, step='5m')

        assert result['status'] == 'success'
        values = result['data']['result'][0]['values']
        # 1 hour / 5 min = 12 intervals + 1 = 13 data points
        assert len(values) == 13
        for ts, val in values:
            assert float(val) == pytest.approx(16.0)

        captured = get_captured_requests()
        assert len(captured) == 1
        assert captured[0]['path'] == '/api/v1/query_range'
        assert captured[0]['params']['step'] == '5m'

    def test_get_current_value(self):
        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        value = client.get_current_value('sum(machine_cpu_cores)')

        assert value == pytest.approx(16.0)

    def test_get_current_value_empty_result(self):
        configure_mock(empty_result=True)

        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        value = client.get_current_value('sum(machine_cpu_cores)')

        assert value is None


class TestVcpuHelpersIntegration:
    def setup_method(self):
        reset_mock()
        configure_mock(cpu_value='16', empty_result=False)

    def test_get_total_workers_cpu(self):
        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        base_ts = 1700000000.0
        vcpu_val, query = get_total_workers_cpu(client, base_ts)

        assert vcpu_val == pytest.approx(16.0)
        assert 'max_over_time' in query
        assert 'sum(machine_cpu_cores)' in query

    def test_get_total_workers_cpu_no_data(self):
        configure_mock(empty_result=True)

        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        vcpu_val, query = get_total_workers_cpu(client, 1700000000.0)

        assert vcpu_val is None

    def test_get_cpu_timeline(self):
        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        start = 1700000000.0
        end = 1700003600.0

        timeline = get_cpu_timeline(client, start, end)

        assert len(timeline) == 13
        for entry in timeline:
            assert entry['cpu_sum'] == pytest.approx(16.0)
            assert 'timestamp' in entry

    def test_get_cpu_timeline_empty(self):
        configure_mock(empty_result=True)

        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        timeline = get_cpu_timeline(client, 1700000000.0, 1700003600.0)

        assert timeline == []

    def test_dynamic_cpu_value(self):
        configure_mock(cpu_value='32')

        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        value = client.get_current_value('sum(machine_cpu_cores)')

        assert value == pytest.approx(32.0)

    def test_range_query_varying_values(self):
        configure_mock(cpu_value=['8', '16', '24'])

        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        start = 1700000000.0
        end = 1700000600.0  # 10 minutes later
        result = client.query_range('sum(machine_cpu_cores)', start_time=start, end_time=end, step='5m')

        values = result['data']['result'][0]['values']
        assert len(values) == 3
        assert float(values[0][1]) == pytest.approx(8.0)
        assert float(values[1][1]) == pytest.approx(16.0)
        assert float(values[2][1]) == pytest.approx(24.0)

    def test_instant_query_returns_last_value_from_list(self):
        configure_mock(cpu_value=['8', '16', '24'])

        client = PrometheusClient(url=MOCK_PROMETHEUS_URL)
        value = client.get_current_value('sum(machine_cpu_cores)')

        assert value == pytest.approx(24.0)
