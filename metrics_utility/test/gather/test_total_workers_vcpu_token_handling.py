"""Tests for token and CA certificate handling in cli_total_workers_vcpu."""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from metrics_utility.automation_controller_billing.collectors import cli_total_workers_vcpu
from metrics_utility.exceptions import MetricsException
from metrics_utility.library.collectors.util import DictOutput
from metrics_utility.test.util import temporary_env


class TestTokenAndCertificateHandling:
    """Test token and certificate file handling."""

    def test_missing_token_file_raises_exception(self):
        """Test that missing token file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = False  # Token file doesn't exist

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='Service account token not found'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_missing_ca_cert_file_raises_exception(self):
        """Test that missing CA cert file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            # Token exists but CA cert doesn't
            mock_exists.side_effect = lambda path: 'token' in path

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='CA_CERT not found'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_empty_token_file_raises_exception(self):
        """Test that empty token file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='Unable to retrieve the token'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_whitespace_only_token_raises_exception(self):
        """Test that whitespace-only token file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='   \n\t  \n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(MetricsException, match='Unable to retrieve the token'):
                    cli_total_workers_vcpu(None, None, DictOutput())

    def test_token_with_newlines_is_stripped(self):
        """Test that token with newlines is properly stripped."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='test-token-with-newlines\n\n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            # Mock the collector
            mock_collector = MagicMock()
            mock_info = {
                'cluster_name': 'test-cluster',
                'total_workers_vcpu': 8,
                'end_timestamp': '2024-01-01T00:59:59.999Z',
            }
            mock_collector.gather.return_value = mock_info
            mock_tw_vcpu.return_value = mock_collector

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                cli_total_workers_vcpu(None, None, DictOutput())

                # Verify token was called with stripped value
                call_args = mock_tw_vcpu.call_args
                assert call_args[1]['token'] == 'test-token-with-newlines'

    def test_metering_disabled_skips_token_check(self):
        """Test that when metering is disabled, token/cert files are not checked."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            # This shouldn't be called since metering is disabled
            mock_exists.return_value = False

            # Mock the collector
            mock_collector = MagicMock()
            mock_info = {
                'cluster_name': 'test-cluster',
                'total_workers_vcpu': 1,
                'end_timestamp': '2024-01-01T00:59:59.999Z',
            }
            mock_collector.gather.return_value = mock_info
            mock_tw_vcpu.return_value = mock_collector

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'false'}):
                result = cli_total_workers_vcpu(None, None, DictOutput())

                # Should succeed without checking files
                assert result is not None
                # Verify token and ca_cert_path passed as None
                call_args = mock_tw_vcpu.call_args
                assert call_args[1]['token'] is None
                assert call_args[1]['ca_cert_path'] is None

    def test_token_file_read_error_propagates(self):
        """Test that file read errors are propagated."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', side_effect=IOError('Permission denied')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true'}):
                with pytest.raises(IOError, match='Permission denied'):
                    cli_total_workers_vcpu(None, None, DictOutput())
