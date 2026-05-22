from unittest.mock import MagicMock, mock_open, patch

import pytest

from metrics_utility.exceptions import CollectorDisabled, MetricsException, MissingRequiredEnvVar
from metrics_utility.gather.collectors import cli_total_workers_vcpu
from metrics_utility.library.collectors.util import DictOutput
from metrics_utility.test.util import temporary_env


class TestTotalWorkersVcpu:
    """Test suite for the cli_total_workers_vcpu collector function."""

    def test_raises_disabled_when_not_in_optional_collectors(self):
        """Test that the function raises CollectorDisabled when total_workers_vcpu is not in optional collectors."""
        with patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get:
            mock_get.return_value = []
            with pytest.raises(CollectorDisabled):
                cli_total_workers_vcpu(None, None, DictOutput())

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


class TestTokenAndCertificateHandling:
    def test_missing_token_file_raises_exception(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = False

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='Service account token not found'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_missing_ca_cert_file_raises_exception(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.side_effect = lambda path: 'token' in path

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='CA_CERT not found'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_empty_token_file_raises_exception(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='Unable to retrieve the token'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_whitespace_only_token_raises_exception(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='   \n\t  \n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='Unable to retrieve the token'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_token_with_newlines_is_stripped(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='test-token-with-newlines\n\n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            mock_collector = MagicMock()
            mock_collector.gather.return_value = {
                'cluster_name': 'test-cluster',
                'total_workers_vcpu': 8,
                'end_timestamp': '2024-01-01T00:59:59.999Z',
            }
            mock_tw_vcpu.return_value = mock_collector

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                cli_total_workers_vcpu(None, None, DictOutput())
                assert mock_tw_vcpu.call_args[1]['token'] == 'test-token-with-newlines'

    def test_metering_disabled_skips_token_check(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = False

            mock_collector = MagicMock()
            mock_collector.gather.return_value = {
                'cluster_name': 'test-cluster',
                'total_workers_vcpu': 1,
                'end_timestamp': '2024-01-01T00:59:59.999Z',
            }
            mock_tw_vcpu.return_value = mock_collector

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'false'}):
                result = cli_total_workers_vcpu(None, None, DictOutput())
                assert result is not None
                assert mock_tw_vcpu.call_args[1]['token'] is None
                assert mock_tw_vcpu.call_args[1]['ca_cert_path'] is None

    def test_token_file_read_error_propagates(self):
        with (
            patch('metrics_utility.gather.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.gather.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', side_effect=IOError('Permission denied')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(IOError, match='Permission denied'):
                    cli_total_workers_vcpu(None, None, DictOutput())
