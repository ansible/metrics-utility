from unittest.mock import Mock, patch

import pytest

from metrics_utility.library.collectors.others.prometheus_client import PrometheusClient


def test_prometheus_client_init_basic():
    """Test PrometheusClient initialization with basic parameters."""
    client = PrometheusClient(url='http://localhost:9090')

    assert client.url == 'http://localhost:9090'
    assert client.timeout == 30
    assert client.session is not None


def test_prometheus_client_init_with_trailing_slash():
    """Test that trailing slash is removed from URL."""
    client = PrometheusClient(url='http://localhost:9090/')

    assert client.url == 'http://localhost:9090'


def test_prometheus_client_init_with_token():
    """Test PrometheusClient initialization with authentication token."""
    client = PrometheusClient(url='http://localhost:9090', token='test-token')

    assert 'Authorization' in client.session.headers
    assert client.session.headers['Authorization'] == 'Bearer test-token'


def test_prometheus_client_init_with_ca_cert():
    """Test PrometheusClient initialization with CA certificate."""
    client = PrometheusClient(url='https://localhost:9090', ca_cert_path='/path/to/ca.crt')

    assert client.session.verify == '/path/to/ca.crt'


def test_prometheus_client_init_custom_timeout():
    """Test PrometheusClient initialization with custom timeout."""
    client = PrometheusClient(url='http://localhost:9090', timeout=60)

    assert client.timeout == 60


def test_prometheus_client_headers():
    """Test that default headers are set correctly."""
    client = PrometheusClient(url='http://localhost:9090')

    assert 'Content-Type' in client.session.headers
    assert client.session.headers['Content-Type'] == 'application/x-www-form-urlencoded'


@patch('requests.Session.get')
def test_query_success(mock_get):
    """Test successful Prometheus query."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'status': 'success',
        'data': {'result': [{'metric': {}, 'value': [1234567890, '42']}]},
    }
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    result = client.query('up')

    assert result == [{'metric': {}, 'value': [1234567890, '42']}]
    mock_get.assert_called_once()


@patch('requests.Session.get')
def test_query_with_time_param(mock_get):
    """Test Prometheus query with time parameter."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    result = client.query('up', time_param=1234567890.5)

    assert result == []
    # Verify time parameter was passed
    call_args = mock_get.call_args
    assert call_args[1]['params']['time'] == pytest.approx(1234567890.5)


@patch('requests.Session.get')
def test_query_http_error(mock_get):
    """Test query handling of HTTP errors."""
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = 'Internal Server Error'
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')

    with pytest.raises(Exception, match='HTTP error 500'):
        client.query('up')


@patch('requests.Session.get')
def test_query_prometheus_api_error(mock_get):
    """Test query handling of Prometheus API errors."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'error', 'error': 'Query timeout'}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')

    with pytest.raises(Exception, match='Prometheus API error'):
        client.query('up')


@patch('requests.Session.get')
def test_query_range_success(mock_get):
    """Test successful Prometheus range query."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'status': 'success',
        'data': {
            'resultType': 'matrix',
            'result': [{'metric': {}, 'values': [[1234567890, '10'], [1234567900, '20']]}],
        },
    }
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    result = client.query_range('up', start_time=1234567890, end_time=1234568000)

    assert 'data' in result
    assert result['status'] == 'success'
    mock_get.assert_called_once()


@patch('requests.Session.get')
def test_query_range_with_custom_step(mock_get):
    """Test range query with custom step parameter."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    client.query_range('up', start_time=1000, end_time=2000, step='1m')

    # Verify step parameter was passed
    call_args = mock_get.call_args
    assert call_args[1]['params']['step'] == '1m'


@patch('requests.Session.get')
def test_query_range_default_step(mock_get):
    """Test that range query uses default step of 5m."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    client.query_range('up', start_time=1000, end_time=2000)

    # Verify default step
    call_args = mock_get.call_args
    assert call_args[1]['params']['step'] == '5m'


@patch('requests.Session.get')
def test_get_current_value_success(mock_get):
    """Test get_current_value with successful query."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'status': 'success',
        'data': {'result': [{'metric': {}, 'value': [1234567890, '42.5']}]},
    }
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    value = client.get_current_value('up')

    assert value == pytest.approx(42.5)
    assert isinstance(value, float)


@patch('requests.Session.get')
def test_get_current_value_empty_result(mock_get):
    """Test get_current_value when query returns empty result."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    value = client.get_current_value('up')

    assert value is None


@patch('requests.Session.get')
def test_get_current_value_integer(mock_get):
    """Test get_current_value with integer value."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'status': 'success',
        'data': {'result': [{'metric': {}, 'value': [1234567890, '100']}]},
    }
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    value = client.get_current_value('metric_total')

    assert value == pytest.approx(100.0)
    assert isinstance(value, float)


@patch('requests.Session.get')
def test_query_url_construction(mock_get):
    """Test that query constructs correct URL."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    client.query('up')

    # Verify URL
    call_args = mock_get.call_args
    assert call_args[0][0] == 'http://localhost:9090/api/v1/query'


@patch('requests.Session.get')
def test_query_range_url_construction(mock_get):
    """Test that query_range constructs correct URL."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    client.query_range('up', start_time=1000, end_time=2000)

    # Verify URL
    call_args = mock_get.call_args
    assert call_args[0][0] == 'http://localhost:9090/api/v1/query_range'


@patch('requests.Session.get')
def test_query_timeout_parameter(mock_get):
    """Test that timeout is passed to requests."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090', timeout=45)
    client.query('up')

    # Verify timeout parameter
    call_args = mock_get.call_args
    assert call_args[1]['timeout'] == 45


@patch('requests.Session.get')
def test_query_with_complex_promql(mock_get):
    """Test query with complex PromQL expression."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'status': 'success', 'data': {'result': []}}
    mock_get.return_value = mock_response

    client = PrometheusClient(url='http://localhost:9090')
    complex_query = 'rate(http_requests_total{job="api"}[5m])'
    client.query(complex_query)

    # Verify query parameter
    call_args = mock_get.call_args
    assert call_args[1]['params']['query'] == complex_query
