"""Tests for token and CA certificate handling in cli_total_workers_vcpu."""

import re

from unittest.mock import MagicMock, mock_open, patch

import pytest

from metrics_utility.automation_controller_billing.collectors import K8S_CA_CERT_PATH, K8S_TOKEN_PATH, cli_total_workers_vcpu
from metrics_utility.exceptions import MetricsException
from metrics_utility.library.collectors.others.total_workers_vcpu import PrometheusClient
from metrics_utility.library.collectors.util import DictOutput
from metrics_utility.test.util import temporary_env


# METRICS_UTILITY_PROMETHEUS_* are unset explicitly, so an exported ./run-vcpu environment
# can't leak into the tests.
METERING_ENV = {
    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
    'METRICS_UTILITY_PROMETHEUS_TOKEN': None,
    'METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH': None,
}


def gathering_collector():
    """A total_workers_vcpu stand-in whose gather() returns a minimal valid payload."""
    collector = MagicMock()
    collector.gather.return_value = {
        'cluster_name': 'test-cluster',
        'total_workers_vcpu': 8,
        'end_timestamp': '2024-01-01T00:59:59.999Z',
    }
    return collector


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

            output = DictOutput()
            with temporary_env(METERING_ENV):
                with pytest.raises(MetricsException, match=re.escape(f'Service account token not found at {K8S_TOKEN_PATH}')):
                    cli_total_workers_vcpu(None, None, output)

    def test_missing_ca_cert_file_raises_exception(self):
        """Test that missing CA cert file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='k8s-token')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            # Token exists but CA cert doesn't
            mock_exists.side_effect = lambda path: 'token' in path

            output = DictOutput()
            with temporary_env(METERING_ENV):
                with pytest.raises(MetricsException, match=re.escape(f'CA_CERT not found at {K8S_CA_CERT_PATH}')):
                    cli_total_workers_vcpu(None, None, output)

    def test_empty_token_file_raises_exception(self):
        """Test that empty token file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            output = DictOutput()
            with temporary_env(METERING_ENV):
                with pytest.raises(MetricsException, match='Unable to retrieve the token'):
                    cli_total_workers_vcpu(None, None, output)

    def test_whitespace_only_token_raises_exception(self):
        """Test that whitespace-only token file raises MetricsException."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='   \n\t  \n')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            output = DictOutput()
            with temporary_env(METERING_ENV):
                with pytest.raises(MetricsException, match='Unable to retrieve the token'):
                    cli_total_workers_vcpu(None, None, output)

    def test_unset_ca_cert_path_uses_the_service_ca(self):
        """Without METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH, the in-cluster service CA is used."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
            patch('builtins.open', mock_open(read_data='k8s-token')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True
            mock_tw_vcpu.return_value = gathering_collector()

            with temporary_env(METERING_ENV):
                cli_total_workers_vcpu(None, None, DictOutput())

            call_args = mock_tw_vcpu.call_args
            assert call_args[1]['ca_cert_path'] == K8S_CA_CERT_PATH
            assert call_args[1]['token'] == 'k8s-token'

    def test_empty_ca_cert_path_skips_tls_verification(self):
        """An empty METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH is passed through and means verify=False."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.total_workers_vcpu') as mock_tw_vcpu,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = False  # no service account files around at all
            mock_tw_vcpu.return_value = gathering_collector()

            env = METERING_ENV | {'METRICS_UTILITY_PROMETHEUS_TOKEN': 'dev', 'METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH': ''}
            with temporary_env(env):
                cli_total_workers_vcpu(None, None, DictOutput())

            # neither the service CA nor the service account token is looked for
            mock_exists.assert_not_called()

            ca_cert_path = mock_tw_vcpu.call_args[1]['ca_cert_path']
            assert ca_cert_path == ''

            # ... and that is what turns TLS verification off
            client = PrometheusClient(url='http://localhost:9090', ca_cert_path=ca_cert_path)
            assert client.session.verify is False

    def test_missing_explicit_ca_cert_file_raises_exception(self):
        """A non-empty METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH still has to exist."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.os.path.exists') as mock_exists,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = False

            env = METERING_ENV | {'METRICS_UTILITY_PROMETHEUS_TOKEN': 'dev', 'METRICS_UTILITY_PROMETHEUS_CA_CERT_PATH': '/nope/ca.crt'}
            output = DictOutput()
            with temporary_env(env), pytest.raises(MetricsException, match=re.escape('CA_CERT not found at /nope/ca.crt')):
                cli_total_workers_vcpu(None, None, output)

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
            mock_tw_vcpu.return_value = gathering_collector()

            with temporary_env(METERING_ENV):
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
            mock_tw_vcpu.return_value = gathering_collector()

            with temporary_env(METERING_ENV | {'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'false'}):
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
            patch('builtins.open', side_effect=OSError('Permission denied')),
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_exists.return_value = True

            output = DictOutput()
            with temporary_env(METERING_ENV):
                with pytest.raises(IOError, match='Permission denied'):
                    cli_total_workers_vcpu(None, None, output)
