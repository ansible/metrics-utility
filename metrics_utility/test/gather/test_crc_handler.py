from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.exceptions import FailedToUploadPayload
from metrics_utility.gather.package.crc_handler import (
    _get_rh_password,
    _get_rh_user,
    get_ingress_url,
    get_proxy_url,
    get_sso_url,
    is_shipping_configured,
    ship,
)
from metrics_utility.test.util import temporary_env


CRC_ENV = {
    'METRICS_UTILITY_CRC_SSO_URL': 'https://sso.example.com/token',
    'METRICS_UTILITY_CRC_INGRESS_URL': 'https://ingress.example.com/upload',
    'METRICS_UTILITY_SERVICE_ACCOUNT_ID': 'my-id',
    'METRICS_UTILITY_SERVICE_ACCOUNT_SECRET': 'my-secret',
    'METRICS_UTILITY_PROXY_URL': None,
}

CLEAR_ENV = {
    'METRICS_UTILITY_CRC_SSO_URL': None,
    'METRICS_UTILITY_CRC_INGRESS_URL': None,
    'METRICS_UTILITY_SERVICE_ACCOUNT_ID': None,
    'METRICS_UTILITY_SERVICE_ACCOUNT_SECRET': None,
    'METRICS_UTILITY_PROXY_URL': None,
}


# --- env helpers ---


def test_get_sso_url_default():
    with temporary_env({'METRICS_UTILITY_CRC_SSO_URL': None}):
        assert 'sso.redhat.com' in get_sso_url()


def test_get_sso_url_custom():
    with temporary_env({'METRICS_UTILITY_CRC_SSO_URL': 'https://custom.sso'}):
        assert get_sso_url() == 'https://custom.sso'


def test_get_ingress_url_default():
    with temporary_env({'METRICS_UTILITY_CRC_INGRESS_URL': None}):
        assert 'console.redhat.com' in get_ingress_url()


def test_get_proxy_url():
    with temporary_env({'METRICS_UTILITY_PROXY_URL': 'http://proxy:8080'}):
        assert get_proxy_url() == 'http://proxy:8080'


def test_get_rh_user():
    with temporary_env({'METRICS_UTILITY_SERVICE_ACCOUNT_ID': 'test-id'}):
        assert _get_rh_user() == 'test-id'


def test_get_rh_password():
    with temporary_env({'METRICS_UTILITY_SERVICE_ACCOUNT_SECRET': 'test-secret'}):
        assert _get_rh_password() == 'test-secret'


# --- is_shipping_configured ---


def test_is_shipping_configured_all_set():
    with temporary_env(CRC_ENV):
        assert is_shipping_configured() is True


def test_is_shipping_configured_no_ingress():
    with temporary_env({**CRC_ENV, 'METRICS_UTILITY_CRC_INGRESS_URL': ''}):
        assert is_shipping_configured() is False


def test_is_shipping_configured_no_sso():
    with temporary_env({**CRC_ENV, 'METRICS_UTILITY_CRC_SSO_URL': ''}):
        assert is_shipping_configured() is False


def test_is_shipping_configured_no_user():
    with temporary_env({**CRC_ENV, 'METRICS_UTILITY_SERVICE_ACCOUNT_ID': ''}):
        assert is_shipping_configured() is False


def test_is_shipping_configured_no_password():
    with temporary_env({**CRC_ENV, 'METRICS_UTILITY_SERVICE_ACCOUNT_SECRET': ''}):
        assert is_shipping_configured() is False


# --- ship ---


@patch('metrics_utility.gather.package.crc_handler.requests')
def test_ship_success(mock_requests, tmp_path):
    mock_sso_response = MagicMock()
    mock_sso_response.status_code = 200
    mock_sso_response.json.return_value = {'access_token': 'test-token'}
    mock_requests.post.return_value = mock_sso_response

    mock_session = MagicMock()
    mock_session.headers = {'Content-Type': 'application/json', 'User-Agent': 'test'}
    mock_upload_response = MagicMock()
    mock_upload_response.status_code = 200
    mock_session.post.return_value = mock_upload_response
    mock_requests.Session.return_value = mock_session

    tar_path = tmp_path / 'test.tar.gz'
    tar_path.write_bytes(b'fake tar content')

    with temporary_env(CRC_ENV):
        ship(str(tar_path))

    mock_session.post.assert_called_once()


@patch('metrics_utility.gather.package.crc_handler.requests')
def test_ship_sso_failure(mock_requests, tmp_path):
    mock_sso_response = MagicMock()
    mock_sso_response.status_code = 401
    mock_sso_response.text = 'Unauthorized'
    mock_requests.post.return_value = mock_sso_response
    mock_requests.Session.return_value = MagicMock(headers={'Content-Type': 'app/json'})

    tar_path = tmp_path / 'test.tar.gz'
    tar_path.write_bytes(b'fake tar content')

    with temporary_env(CRC_ENV):
        with pytest.raises(FailedToUploadPayload, match='SSO token request failed'):
            ship(str(tar_path))


@patch('metrics_utility.gather.package.crc_handler.requests')
def test_ship_sso_missing_token(mock_requests, tmp_path):
    mock_sso_response = MagicMock()
    mock_sso_response.status_code = 200
    mock_sso_response.json.return_value = {}
    mock_sso_response.text = 'no token'
    mock_requests.post.return_value = mock_sso_response
    mock_requests.Session.return_value = MagicMock(headers={'Content-Type': 'app/json'})

    tar_path = tmp_path / 'test.tar.gz'
    tar_path.write_bytes(b'fake tar content')

    with temporary_env(CRC_ENV):
        with pytest.raises(FailedToUploadPayload, match='missing access_token'):
            ship(str(tar_path))


@patch('metrics_utility.gather.package.crc_handler.requests')
def test_ship_upload_failure(mock_requests, tmp_path):
    mock_sso_response = MagicMock()
    mock_sso_response.status_code = 200
    mock_sso_response.json.return_value = {'access_token': 'test-token'}
    mock_requests.post.return_value = mock_sso_response

    mock_session = MagicMock()
    mock_session.headers = {'Content-Type': 'application/json'}
    mock_upload_response = MagicMock()
    mock_upload_response.status_code = 500
    mock_upload_response.text = 'Internal Server Error'
    mock_session.post.return_value = mock_upload_response
    mock_requests.Session.return_value = mock_session

    tar_path = tmp_path / 'test.tar.gz'
    tar_path.write_bytes(b'fake tar content')

    with temporary_env(CRC_ENV):
        with pytest.raises(FailedToUploadPayload, match='Upload failed'):
            ship(str(tar_path))


@patch('metrics_utility.gather.package.crc_handler.requests')
def test_ship_with_proxy(mock_requests, tmp_path):
    mock_sso_response = MagicMock()
    mock_sso_response.status_code = 200
    mock_sso_response.json.return_value = {'access_token': 'test-token'}
    mock_requests.post.return_value = mock_sso_response

    mock_session = MagicMock()
    mock_session.headers = {'Content-Type': 'application/json'}
    mock_upload_response = MagicMock()
    mock_upload_response.status_code = 200
    mock_session.post.return_value = mock_upload_response
    mock_requests.Session.return_value = mock_session

    tar_path = tmp_path / 'test.tar.gz'
    tar_path.write_bytes(b'fake tar content')

    with temporary_env({**CRC_ENV, 'METRICS_UTILITY_PROXY_URL': 'http://proxy:8080'}):
        ship(str(tar_path))

    call_kwargs = mock_session.post.call_args[1]
    assert call_kwargs['proxies'] == {'https': 'http://proxy:8080'}
