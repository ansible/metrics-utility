from unittest.mock import MagicMock, mock_open, patch

import pytest

from metrics_utility.exceptions import MetricsException, MissingRequiredEnvVar
from metrics_utility.gather.collectors import cli_total_workers_vcpu
from metrics_utility.library.collectors.util import DictOutput
from metrics_utility.test.util import temporary_env


class TestTotalWorkersVcpu:
    """Test suite for the cli_total_workers_vcpu collector function."""

    def test_returns_none_when_not_in_optional_collectors(self):
        """Test that the function returns None when total_workers_vcpu is not in optional collectors."""
        with patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get:
            mock_get.return_value = []
            result = cli_total_workers_vcpu(None, None, DictOutput())
            assert result is None

    def test_raises_missing_required_env_var_when_cluster_name_not_set(self):
        """Test that the function raises MissingRequiredEnvVar when METRICS_UTILITY_CLUSTER_NAME is not set."""
        with patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get:
            mock_get.return_value = ['total_workers_vcpu']
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': None}):
                with pytest.raises(MissingRequiredEnvVar) as exc_info:
                    cli_total_workers_vcpu(None, None, DictOutput())

                assert 'environment variable METRICS_UTILITY_CLUSTER_NAME is not set' in str(exc_info.value)

    def test_returns_hardcoded_value_when_usage_based_billing_disabled(self):
        """Test that the function returns hardcoded value when METRICS_UTILITY_USAGE_BASED_METERING_ENABLED is not set or false (default behavior)."""
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.total_workers_vcpu') as mock_tw_vcpu,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock the collector
            mock_collector = MagicMock()
            mock_info = {
                'cluster_name': 'test-cluster',
                'total_workers_vcpu': 1,
                'usage_based_billing_enabled': False,
                'collection_timestamp': '2024-01-01T00:00:00.000Z',
                'start_timestamp': '2024-01-01T00:00:00.000Z',
                'end_timestamp': '2024-01-01T00:59:59.999Z',
            }
            mock_collector.gather.return_value = mock_info
            mock_tw_vcpu.return_value = mock_collector

            # Test when not set (default behavior)
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster'}):
                result = cli_total_workers_vcpu(None, None, DictOutput())
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 1
                assert result['timestamp'] == '2024-01-01T00:59:59.999Z'

    def test_raises_exception_when_total_workers_vcpu_is_none(self):
        """Test that the function raises MetricsException when total_workers_vcpu is None."""
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.gather.collectors.os.path.exists', return_value=True),
            patch('builtins.open', mock_open(read_data='test-token\n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock the collector to return None for total_workers_vcpu
            mock_collector = MagicMock()
            mock_info = None
            mock_collector.gather.return_value = mock_info
            mock_tw_vcpu.return_value = mock_collector

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException) as exc_info:
                    cli_total_workers_vcpu(None, None, DictOutput())

                assert 'No data available yet' in str(exc_info.value)

    def test_successful_call_with_metering_enabled(self):
        """Test successful call when usage based metering is enabled."""
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.gather.collectors.os.path.exists', return_value=True),
            patch('builtins.open', mock_open(read_data='test-token\n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Mock the collector
            mock_collector = MagicMock()
            mock_info = {
                'cluster_name': 'test-cluster',
                'total_workers_vcpu': 16,
                'usage_based_billing_enabled': True,
                'collection_timestamp': '2024-01-01T00:00:00.000Z',
                'start_timestamp': '2024-01-01T00:00:00.000Z',
                'end_timestamp': '2024-01-01T00:59:59.999Z',
                'promql_query': 'max_over_time(...)',
                'timeline': [],
            }
            mock_collector.gather.return_value = mock_info
            mock_tw_vcpu.return_value = mock_collector

            with temporary_env(
                {
                    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
                    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
                    'METRICS_UTILITY_PROMETHEUS_URL': 'https://prometheus.example.com:9090',
                }
            ):
                result = cli_total_workers_vcpu(None, None, DictOutput())
                assert result['cluster_name'] == 'test-cluster'
                assert result['total_workers_vcpu'] == 16
