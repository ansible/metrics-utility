"""
Unit tests for PrometheusClient class.

This module contains comprehensive tests for the PrometheusClient class,
including mocking of HTTP requests.
"""

import json

from unittest.mock import MagicMock, patch

import pytest
import requests

from metrics_utility.library.collectors.others.prometheus_client import PrometheusClient


class TestPrometheusClient:
    """Test cases for PrometheusClient class."""

    def test_init_success_with_token(self):
        """Test successful initialization with valid token."""
        # Create client
        client = PrometheusClient(
            url='https://prometheus.example.com:9090',
            token='test-token-12345',
            ca_cert_path='/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt',
        )

        # Assertions
        assert client.url == 'https://prometheus.example.com:9090'
        assert client.timeout == 30  # default
        assert client.session is not None
        assert client.session.headers['Authorization'] == 'Bearer test-token-12345'

    def test_init_success_with_custom_timeout(self):
        """Test successful initialization with custom timeout."""
        # Create client
        client = PrometheusClient(
            url='https://prometheus.example.com:9090',
            timeout=60,
            token='mounted-token-67890',
            ca_cert_path='/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt',
        )

        # Assertions
        assert client.url == 'https://prometheus.example.com:9090'
        assert client.timeout == 60
        assert client.session is not None
        assert client.session.headers['Authorization'] == 'Bearer mounted-token-67890'

    def test_init_without_token(self):
        """Test initialization without authentication token."""
        # Create client without token
        client = PrometheusClient(url='https://prometheus.example.com:9090')

        # Assertions - should work fine without token (unauthenticated)
        assert client.url == 'https://prometheus.example.com:9090'
        assert client.timeout == 30
        assert client.session is not None
        assert 'Authorization' not in client.session.headers

    def test_init_url_trailing_slash_removal(self):
        """Test that trailing slash is removed from URL."""
        # Create client with trailing slash
        client = PrometheusClient(url='https://prometheus.example.com:9090/', token='test-token')

        # Assertions
        assert client.url == 'https://prometheus.example.com:9090'

    def test_setup_session_with_token(self):
        """Test session setup with authentication token."""
        # Create client with token and CA cert
        client = PrometheusClient(
            url='https://prometheus.example.com:9090',
            token='test-token-12345',
            ca_cert_path='/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt',
        )

        # Assertions
        assert client.session.headers['Authorization'] == 'Bearer test-token-12345'
        assert client.session.headers['Content-Type'] == 'application/x-www-form-urlencoded'
        assert client.session.verify == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'

    def test_query_success(self):
        """Test successful query execution."""
        # Mock response data
        mock_response_data = {'status': 'success', 'data': {'result': [{'metric': {'__name__': 'test_metric'}, 'value': [1640995200, '42.0']}]}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            result = client.query('test_metric')

            # Assertions
            assert result == mock_response_data['data']['result']
            mock_get.assert_called_once_with('https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric'}, timeout=30)

    def test_query_with_time_param(self):
        """Test query execution with time parameter."""
        # Mock response data
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query with time
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            result = client.query('test_metric', time_param=1640995200.0)

            # Assertions
            assert result == []
            mock_get.assert_called_once_with(
                'https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric', 'time': 1640995200.0}, timeout=30
            )

    def test_query_prometheus_api_error(self):
        """Test query failure with Prometheus API error."""
        # Mock error response
        mock_response_data = {'status': 'error', 'error': 'invalid query: parse error at position 5'}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(Exception, match='Prometheus API error: invalid query: parse error at position 5'):
                client.query('invalid_query')

    def test_query_http_error(self):
        """Test query failure with HTTP error."""
        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_response.text = 'Not Found'
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(Exception, match='HTTP error 404: Not Found'):
                client.query('test_metric')

    def test_query_connection_error(self):
        """Test query failure with connection error."""
        with patch.object(requests.Session, 'get') as mock_get:
            mock_get.side_effect = requests.ConnectionError('Connection failed')

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(requests.ConnectionError, match='Connection failed'):
                client.query('test_metric')

    def test_query_timeout_error(self):
        """Test query failure with timeout error."""
        with patch.object(requests.Session, 'get') as mock_get:
            mock_get.side_effect = requests.Timeout('Request timed out')

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(requests.Timeout, match='Request timed out'):
                client.query('test_metric')

    def test_query_json_decode_error(self):
        """Test query failure with JSON decode error."""
        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError('Invalid JSON', 'doc', 0)
            mock_get.return_value = mock_response

            # Create client and test error
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(json.JSONDecodeError):
                client.query('test_metric')

    def test_get_current_value_success(self):
        """Test successful get_current_value execution."""
        # Mock response data - using whole number float as vCPU counts are always whole numbers
        mock_response_data = {'status': 'success', 'data': {'result': [{'metric': {'__name__': 'test_metric'}, 'value': [1640995200, '42']}]}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            value = client.get_current_value('test_metric')

            # Assertions
            assert value == 42
            mock_get.assert_called_once_with('https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric'}, timeout=30)

    def test_get_current_value_empty_result(self):
        """Test get_current_value with empty result."""
        # Mock empty response data
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute query
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            value = client.get_current_value('test_metric')

            # Assertions
            assert value is None

    def test_get_current_value_query_failure(self):
        """Test get_current_value when underlying query fails."""
        with patch.object(requests.Session, 'get') as mock_get:
            mock_get.side_effect = requests.ConnectionError('Connection failed')

            # Create client and test error propagation
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(requests.ConnectionError, match='Connection failed'):
                client.get_current_value('test_metric')

    def test_get_current_value_invalid_value_format(self):
        """Test get_current_value with invalid value format."""
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
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(ValueError):
                client.get_current_value('test_metric')

    def test_session_configuration(self):
        """Test that session is configured correctly."""
        # Create client
        client = PrometheusClient(
            url='https://prometheus.example.com:9090', token='test-token', ca_cert_path='/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        )

        # Check session configuration
        assert isinstance(client.session, requests.Session)
        assert client.session.headers['Authorization'] == 'Bearer test-token'
        assert client.session.headers['Content-Type'] == 'application/x-www-form-urlencoded'
        assert client.session.verify == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'

    def test_init_with_ca_cert_path(self):
        """Test initialization with CA certificate path."""
        # Create client with CA cert path
        client = PrometheusClient(
            url='https://prometheus.example.com:9090', token='test-token', ca_cert_path='/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        )

        # Verify CA cert is configured
        assert client.session.verify == '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'

    def test_custom_timeout(self):
        """Test client with custom timeout."""
        # Mock response
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client with custom timeout
            client = PrometheusClient(url='https://prometheus.example.com:9090', timeout=120, token='test-token')
            client.query('test_metric')

            # Verify custom timeout is used
            assert client.timeout == 120
            mock_get.assert_called_once_with('https://prometheus.example.com:9090/api/v1/query', params={'query': 'test_metric'}, timeout=120)

    def test_query_unknown_error_status(self):
        """Test query with unknown error status from Prometheus."""
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
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(Exception, match='Prometheus API error: Unknown error'):
                client.query('test_query')

    def test_query_missing_data_field(self):
        """Test query with missing data field in response."""
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
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            result = client.query('test_metric')

            # Should return empty list when data field is missing
            assert result == []

    def test_query_missing_result_field(self):
        """Test query with missing result field in data."""
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
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            result = client.query('test_metric')

            # Should return empty list when result field is missing
            assert result == []

    def test_query_range_success(self):
        """Test successful query_range execution."""
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
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            result = client.query_range('test_metric', 1640995200, 1640995320, '1m')

            # Assertions
            assert result == mock_response_data
            mock_get.assert_called_once_with(
                'https://prometheus.example.com:9090/api/v1/query_range',
                params={'query': 'test_metric', 'start': 1640995200, 'end': 1640995320, 'step': '1m'},
                timeout=30,
            )

    def test_query_range_default_step(self):
        """Test query_range with default step parameter."""
        mock_response_data = {'status': 'success', 'data': {'result': []}}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute range query without step parameter
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')
            client.query_range('test_metric', 1640995200, 1640995320)

            # Should use default step of '5m'
            mock_get.assert_called_once_with(
                'https://prometheus.example.com:9090/api/v1/query_range',
                params={'query': 'test_metric', 'start': 1640995200, 'end': 1640995320, 'step': '5m'},
                timeout=30,
            )

    def test_query_range_prometheus_error(self):
        """Test query_range with Prometheus API error."""
        # Mock error response
        mock_response_data = {'status': 'error', 'error': 'invalid query'}

        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response

            # Create client and execute range query
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            # Should raise exception for error status
            with pytest.raises(Exception, match='Prometheus API error: invalid query'):
                client.query_range('invalid_query', 1640995200, 1640995320)

    def test_query_range_http_error(self):
        """Test query_range with HTTP error."""
        with patch.object(requests.Session, 'get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 400
            mock_response.text = 'Bad Request'
            mock_get.return_value = mock_response

            # Create client and execute range query
            client = PrometheusClient(url='https://prometheus.example.com:9090', token='test-token')

            with pytest.raises(Exception, match='HTTP error 400: Bad Request'):
                client.query_range('test_metric', 1640995200, 1640995320)
