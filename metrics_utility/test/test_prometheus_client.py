"""
Unit tests for PrometheusClient class.

This module contains comprehensive tests for the PrometheusClient class,
including mocking of HTTP requests and Kubernetes client interactions.
"""

import json

from unittest.mock import MagicMock, patch

import pytest
import requests

from metrics_utility.automation_controller_billing.prometheus_client import PrometheusClient
from metrics_utility.exceptions import MetricsException


class TestPrometheusClient:
    """Test cases for PrometheusClient class."""

    def _setup_kubernetes_client_mock(self, mock_k8s_client, token='test-token'):
        """Helper method to set up KubernetesClient mock with common return values."""
        mock_k8s_client.get_current_token.return_value = token
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        return mock_k8s_client

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_init_success_with_token(self, mock_k8s_client_class):
        """Test successful initialization with valid token."""
        # Setup mock
        mock_k8s_client = MagicMock()
        self._setup_kubernetes_client_mock(mock_k8s_client, 'test-token-12345')
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch('os.path.exists', return_value=True):
            # Create client
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            # Assertions
            assert client.url == 'https://prometheus.example.com:9090'
            assert client.token == 'test-token-12345'
            assert client.timeout == 30  # default
            assert client.session is not None
            mock_k8s_client.get_current_token.assert_called_once()

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_init_success_with_mounted_token(self, mock_k8s_client_class):
        """Test successful initialization requesting mounted token."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'mounted-token-67890'
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch('os.path.exists', return_value=True):
            # Create client
            client = PrometheusClient(url='https://prometheus.example.com:9090', timeout=60)

            # Assertions
            assert client.url == 'https://prometheus.example.com:9090'
            assert client.token == 'mounted-token-67890'
            assert client.timeout == 60
            assert client.session is not None
            mock_k8s_client.get_current_token.assert_called_once()

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_init_failure_no_token(self, mock_k8s_client_class):
        """Test initialization failure when no token can be obtained."""
        # Setup mock to return None
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = None
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Test that MetricsException is raised
        with pytest.raises(MetricsException, match='Unable to retrieve the token for the current service account'):
            PrometheusClient(url='https://prometheus.example.com:9090')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_init_url_trailing_slash_removal(self, mock_k8s_client_class):
        """Test that trailing slash is removed from URL."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch('os.path.exists', return_value=True):
            # Create client with trailing slash
            client = PrometheusClient(url='https://prometheus.example.com:9090/')

            # Assertions
            assert client.url == 'https://prometheus.example.com:9090'

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_setup_session_with_token(self, mock_k8s_client_class):
        """Test session setup with authentication token."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token-12345'
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        mock_k8s_client_class.return_value = mock_k8s_client

        with (
            patch('urllib3.disable_warnings') as mock_disable_warnings,
            patch('os.path.exists') as mock_exists,
        ):
            # Test when CA certificate exists
            mock_exists.return_value = True
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            # Assertions
            assert client.session.headers['Authorization'] == 'Bearer test-token-12345'
            assert client.session.headers['Content-Type'] == 'application/x-www-form-urlencoded'
            assert client.session.verify == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
            assert client.ca_cert_path == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
            mock_disable_warnings.assert_not_called()

            # Test when CA certificate doesn't exist - should raise exception
            mock_exists.return_value = False
            with pytest.raises(MetricsException, match='CA_CERT not found at'):
                PrometheusClient(url='https://prometheus.example.com:9090')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_success(self, mock_k8s_client_class):
        """Test successful query execution."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response data
        mock_response_data = {'status': 'success', 'data': {'result': [{'metric': {'__name__': 'test_metric'}, 'value': [1640995200, '42.0']}]}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            result = client.query('test_metric')

            # Assertions
            assert result == mock_response_data['data']['result']
            mock_get.assert_called_once_with('https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric'}, timeout=30)

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_with_time_param(self, mock_k8s_client_class):
        """Test query execution with time parameter."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response data
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query with time
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            result = client.query('test_metric', time_param=1640995200.0)

            # Assertions
            assert result == []
            mock_get.assert_called_once_with(
                'https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric', 'time': 1640995200.0}, timeout=30
            )

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_prometheus_api_error(self, mock_k8s_client_class):
        """Test query failure with Prometheus API error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock error response
        mock_response_data = {'status': 'error', 'error': 'invalid query: parse error at position 5'}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='Prometheus API error: invalid query: parse error at position 5'):
                client.query('invalid_query')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_http_error(self, mock_k8s_client_class):
        """Test query failure with HTTP error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_response.text = 'Not Found'
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='HTTP error 404: Not Found'):
                client.query('test_metric')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_connection_error(self, mock_k8s_client_class):
        """Test query failure with connection error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch.object(requests.Session, 'get') as mock_get:
            mock_get.side_effect = requests.ConnectionError('Connection failed')

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='Query failed: Connection failed'):
                client.query('test_metric')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_timeout_error(self, mock_k8s_client_class):
        """Test query failure with timeout error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch.object(requests.Session, 'get') as mock_get:
            mock_get.side_effect = requests.Timeout('Request timed out')

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='Query failed: Request timed out'):
                client.query('test_metric')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_json_decode_error(self, mock_k8s_client_class):
        """Test query failure with JSON decode error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError('Invalid JSON', 'doc', 0)
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='Query failed:'):
                client.query('test_metric')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_get_current_value_success(self, mock_k8s_client_class):
        """Test successful get_current_value execution."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response data - using whole number float as vCPU counts are always whole numbers
        mock_response_data = {'status': 'success', 'data': {'result': [{'metric': {'__name__': 'test_metric'}, 'value': [1640995200, '42']}]}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            value = client.get_current_value('test_metric')

            # Assertions
            assert value == 42
            mock_get.assert_called_once_with('https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric'}, timeout=30)

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_get_current_value_empty_result(self, mock_k8s_client_class):
        """Test get_current_value with empty result."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock empty response data
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            value = client.get_current_value('test_metric')

            # Assertions
            assert value is None

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_get_current_value_query_failure(self, mock_k8s_client_class):
        """Test get_current_value when underlying query fails."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch.object(requests.Session, 'get') as mock_get:
            mock_get.side_effect = requests.ConnectionError('Connection failed')

            # Create client and test error propagation
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='Query failed: Connection failed'):
                client.get_current_value('test_metric')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_get_current_value_invalid_value_format(self, mock_k8s_client_class):
        """Test get_current_value with invalid value format."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response with invalid value format
        mock_response_data = {
            'status': 'success',
            'data': {'result': [{'metric': {'__name__': 'test_metric'}, 'value': [1640995200, 'invalid_number']}]},
        }

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(ValueError):
                client.get_current_value('test_metric')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_session_configuration(self, mock_k8s_client_class):
        """Test that session is configured correctly."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        mock_k8s_client_class.return_value = mock_k8s_client

        with (
            patch('urllib3.disable_warnings') as mock_disable_warnings,
            patch('os.path.exists', return_value=True),
        ):
            # Create client
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            # Check session configuration
            assert isinstance(client.session, requests.Session)
            assert client.session.headers['Authorization'] == 'Bearer test-token'
            assert client.session.headers['Content-Type'] == 'application/x-www-form-urlencoded'
            assert client.session.verify == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
            assert client.ca_cert_path == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
            mock_disable_warnings.assert_not_called()

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_init_failure_ca_cert_not_found(self, mock_k8s_client_class):
        """Test initialization failure when CA certificate file doesn't exist."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client.get_ca_cert_path.return_value = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch('os.path.exists', return_value=False):
            # Test that MetricsException is raised when CA cert doesn't exist
            with pytest.raises(MetricsException, match='CA_CERT not found at /var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'):
                PrometheusClient(url='https://prometheus.example.com:9090')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_custom_timeout(self, mock_k8s_client_class):
        """Test client with custom timeout."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client with custom timeout
            client = PrometheusClient(url='https://prometheus.example.com:9090', timeout=120)
            client.query('test_metric')

            # Verify custom timeout is used
            assert client.timeout == 120
            mock_get.assert_called_once_with('https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric'}, timeout=120)

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_unknown_error_status(self, mock_k8s_client_class):
        """Test query with unknown error status from Prometheus."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock error response without error message
        mock_response_data = {
            'status': 'error'
            # No 'error' field
        }

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException, match='Prometheus API error: Unknown error'):
                client.query('test_query')

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_missing_data_field(self, mock_k8s_client_class):
        """Test query with missing data field in response."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response without data field
        mock_response_data = {
            'status': 'success'
            # No 'data' field
        }

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            result = client.query('test_metric')

            # Should return empty list when data field is missing
            assert result == []

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_missing_result_field(self, mock_k8s_client_class):
        """Test query with missing result field in data."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response without result field
        mock_response_data = {
            'status': 'success',
            'data': {},  # No 'result' field
        }

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            result = client.query('test_metric')

            # Should return empty list when result field is missing
            assert result == []

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_range_success(self, mock_k8s_client_class):
        """Test successful query_range execution."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock response data for range query
        mock_response_data = {
            'status': 'success',
            'data': {'result': [{'metric': {'__name__': 'test_metric'}, 'values': [[1640995200, '16'], [1640995260, '18'], [1640995320, '16']]}]},
        }

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute range query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            result = client.query_range('test_metric', 1640995200, 1640995320, '1m')

            # Assertions
            assert result == mock_response_data
            mock_get.assert_called_once_with(
                'https://prometheus.example.com:9090/api/v1/query_range',
                params={'query': 'test_metric', 'start': 1640995200, 'end': 1640995320, 'step': '1m'},
                timeout=30,
            )

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_range_default_step(self, mock_k8s_client_class):
        """Test query_range with default step parameter."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute range query without step parameter
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            client.query_range('test_metric', 1640995200, 1640995320)

            # Should use default step of '5m'
            mock_get.assert_called_once_with(
                'https://prometheus.example.com:9090/api/v1/query_range',
                params={'query': 'test_metric', 'start': 1640995200, 'end': 1640995320, 'step': '5m'},
                timeout=30,
            )

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_range_prometheus_error(self, mock_k8s_client_class):
        """Test query_range with Prometheus API error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        # Mock error response
        mock_response_data = {'status': 'error', 'error': 'invalid query'}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute range query
            client = PrometheusClient(url='https://prometheus.example.com:9090')
            result = client.query_range('invalid_query', 1640995200, 1640995320)

            # Should return None for error status
            assert result is None

    @patch('metrics_utility.automation_controller_billing.prometheus_client.KubernetesClient')
    def test_query_range_http_error(self, mock_k8s_client_class):
        """Test query_range with HTTP error."""
        # Setup mock
        mock_k8s_client = MagicMock()
        mock_k8s_client.get_current_token.return_value = 'test-token'
        mock_k8s_client_class.return_value = mock_k8s_client

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError('400 Bad Request')
            mock_get.return_value = mock_response

            # Create client and execute range query
            client = PrometheusClient(url='https://prometheus.example.com:9090')

            with pytest.raises(MetricsException):
                client.query_range('test_metric', 1640995200, 1640995320)
