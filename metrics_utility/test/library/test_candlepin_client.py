"""
Unit tests for metrics_utility.library.candlepin.client.CandlepinClient.

All HTTP calls are mocked with unittest.mock so no real Candlepin server is
needed.  Temp-file creation and cleanup are also verified to ensure no PEM
material is leaked after each call.
"""

import datetime
import os

from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from metrics_utility.library.candlepin.client import CandlepinClient


# ---------------------------------------------------------------------------
# Test-cert helpers
# ---------------------------------------------------------------------------


def _generate_cert_and_key():
    """Return (cert_pem, key_pem) for a self-signed cert valid for 365 days."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'test-consumer')]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'test-ca')]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode('utf-8')
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode('utf-8')
    return cert_pem, key_pem


@pytest.fixture
def cert_and_key():
    return _generate_cert_and_key()


CONSUMER_UUID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'


# ---------------------------------------------------------------------------
# CandlepinClient construction
# ---------------------------------------------------------------------------


class TestCandlepinClientInit:
    def test_default_url(self):
        client = CandlepinClient()
        assert client.base_url == CandlepinClient.DEFAULT_CANDLEPIN_URL.rstrip('/')

    def test_trailing_slash_stripped(self):
        client = CandlepinClient(base_url='https://example.com/sub/')
        assert not client.base_url.endswith('/')

    def test_verify_true_by_default(self):
        client = CandlepinClient()
        assert client.verify is True

    def test_verify_set_to_ca_path(self):
        client = CandlepinClient(candlepin_ca='/etc/rhsm/ca/redhat-uep.pem')
        assert client.verify == '/etc/rhsm/ca/redhat-uep.pem'

    def test_verify_false_requires_explicit_opt_in(self):
        client = CandlepinClient(verify_tls=False)
        assert client.verify is False

    def test_ca_path_takes_precedence_over_verify_tls_false(self):
        client = CandlepinClient(candlepin_ca='/etc/rhsm/ca/redhat-uep.pem', verify_tls=False)
        assert client.verify == '/etc/rhsm/ca/redhat-uep.pem'

    def test_proxy_set(self):
        client = CandlepinClient(proxy='https://proxy:3128')
        assert client.proxies == {'https': 'https://proxy:3128', 'http': 'http://proxy:3128'}

    def test_proxy_none_gives_empty_dict(self):
        client = CandlepinClient()
        assert client.proxies == {}


# ---------------------------------------------------------------------------
# Temp cert file helper
# ---------------------------------------------------------------------------


class TestTempCertFiles:
    def test_files_written_and_cleaned_up(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        paths = {}
        with CandlepinClient._temp_cert_files(cert_pem, key_pem) as (cp, kp):
            paths['cert'] = cp
            paths['key'] = kp
            assert os.path.exists(cp)
            assert os.path.exists(kp)
            with open(cp) as f:
                assert f.read() == cert_pem
            with open(kp) as f:
                assert f.read() == key_pem
        assert not os.path.exists(paths['cert'])
        assert not os.path.exists(paths['key'])

    def test_files_have_mode_0600(self, cert_pem=None, key_pem=None):
        cert_pem, key_pem = _generate_cert_and_key()
        with CandlepinClient._temp_cert_files(cert_pem, key_pem) as (cp, kp):
            assert oct(os.stat(cp).st_mode)[-3:] == '600'
            assert oct(os.stat(kp).st_mode)[-3:] == '600'

    def test_cleanup_on_exception(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        paths = {}
        try:
            with CandlepinClient._temp_cert_files(cert_pem, key_pem) as (cp, kp):
                paths['cert'] = cp
                paths['key'] = kp
                raise RuntimeError('simulated error')
        except RuntimeError:
            pass
        assert not os.path.exists(paths['cert'])
        assert not os.path.exists(paths['key'])


# ---------------------------------------------------------------------------
# checkin()
# ---------------------------------------------------------------------------


class TestCheckin:
    def _mock_response(self, status_code):
        resp = MagicMock()
        resp.status_code = status_code
        return resp

    def test_returns_true_on_200(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient(base_url='https://candlepin.example.com/sub')
        with patch('requests.put', return_value=self._mock_response(200)) as mock_put:
            result = client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        assert result is True
        mock_put.assert_called_once()

    def test_returns_true_on_204(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient(base_url='https://candlepin.example.com/sub')
        with patch('requests.put', return_value=self._mock_response(204)):
            result = client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        assert result is True

    def test_returns_false_on_4xx(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.put', return_value=self._mock_response(401)):
            result = client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        assert result is False

    def test_returns_false_on_network_error(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.put', side_effect=requests.exceptions.ConnectionError('refused')):
            result = client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        assert result is False

    def test_never_raises(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.put', side_effect=Exception('totally unexpected')):
            result = client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        assert result is False

    def test_logs_warning_on_failure(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.put', side_effect=Exception('oops')):
            with patch('metrics_utility.library.candlepin.client.logger') as mock_log:
                client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        mock_log.warning.assert_called_once()

    def test_url_contains_consumer_uuid(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient(base_url='https://candlepin.example.com')
        with patch('requests.put', return_value=self._mock_response(204)) as mock_put:
            client.checkin(CONSUMER_UUID, cert_pem, key_pem)
        url_called = mock_put.call_args[0][0]
        assert CONSUMER_UUID in url_called

    def test_temp_files_cleaned_up_after_success(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        seen_paths = []

        def capturing_put(url, cert=None, **kwargs):
            seen_paths.extend(list(cert))
            resp = MagicMock()
            resp.status_code = 204
            return resp

        with patch('requests.put', side_effect=capturing_put):
            client.checkin(CONSUMER_UUID, cert_pem, key_pem)

        for path in seen_paths:
            assert not os.path.exists(path), f'Temp file not cleaned up: {path}'


# ---------------------------------------------------------------------------
# regenerate_cert()
# ---------------------------------------------------------------------------


class TestRegenerateCert:
    SAMPLE_NEW_CERT = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
    SAMPLE_NEW_KEY = '-----BEGIN RSA PRIVATE KEY-----\nnewkey==\n-----END RSA PRIVATE KEY-----\n'

    def _mock_success_response(self):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            'uuid': CONSUMER_UUID,
            'idCert': {
                'cert': self.SAMPLE_NEW_CERT,
                'key': self.SAMPLE_NEW_KEY,
                'serial': {'serial': 9999},
            },
        }
        return resp

    def test_returns_new_cert_and_key_on_success(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()):
            new_cert, new_key = client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)
        assert new_cert == self.SAMPLE_NEW_CERT
        assert new_key == self.SAMPLE_NEW_KEY

    def test_raises_on_http_error(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.text = 'Internal Server Error'
        with patch('requests.post', return_value=resp):
            with pytest.raises(RuntimeError, match='500'):
                client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)

    def test_raises_on_network_error(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.post', side_effect=requests.exceptions.ConnectionError('refused')):
            with pytest.raises(RuntimeError, match='network error'):
                client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)

    def test_raises_when_idcert_missing(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {'uuid': CONSUMER_UUID}
        with patch('requests.post', return_value=resp):
            with pytest.raises(RuntimeError, match='idCert'):
                client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)

    def test_url_contains_consumer_uuid(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient(base_url='https://candlepin.example.com')
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)
        url_called = mock_post.call_args[0][0]
        assert CONSUMER_UUID in url_called

    def test_temp_files_cleaned_up_after_success(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        seen_paths = []

        def capturing_post(url, cert=None, **kwargs):
            seen_paths.extend(list(cert))
            return self._mock_success_response()

        with patch('requests.post', side_effect=capturing_post):
            client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)

        for path in seen_paths:
            assert not os.path.exists(path), f'Temp file not cleaned up: {path}'

    def test_temp_files_cleaned_up_on_failure(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        seen_paths = []

        def capturing_post(url, cert=None, **kwargs):
            seen_paths.extend(list(cert))
            raise requests.exceptions.ConnectionError('refused')

        with patch('requests.post', side_effect=capturing_post):
            with pytest.raises(RuntimeError):
                client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)

        for path in seen_paths:
            assert not os.path.exists(path), f'Temp file not cleaned up: {path}'

    def test_logs_info_on_success(self, cert_and_key):
        cert_pem, key_pem = cert_and_key
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()):
            with patch('metrics_utility.library.candlepin.client.logger') as mock_log:
                client.regenerate_cert(CONSUMER_UUID, cert_pem, key_pem)
        mock_log.info.assert_called_once()


# ---------------------------------------------------------------------------
# register_consumer()
# ---------------------------------------------------------------------------


class TestRegisterConsumer:
    SAMPLE_CERT = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
    SAMPLE_KEY = '-----BEGIN RSA PRIVATE KEY-----\nnewkey==\n-----END RSA PRIVATE KEY-----\n'
    SAMPLE_UUID = 'cccccccc-dddd-eeee-ffff-000000000000'

    def _mock_success_response(self):
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {
            'uuid': self.SAMPLE_UUID,
            'idCert': {
                'cert': self.SAMPLE_CERT,
                'key': self.SAMPLE_KEY,
                'serial': {'serial': 12345},
            },
        }
        return resp

    def test_returns_cert_key_uuid_on_success(self):
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()):
            cert, key, uuid_ = client.register_consumer('user', 'pass', 'my-org')
        assert cert == self.SAMPLE_CERT
        assert key == self.SAMPLE_KEY
        assert uuid_ == self.SAMPLE_UUID

    def test_posts_to_consumers_endpoint(self):
        client = CandlepinClient(base_url='https://candlepin.example.com/sub')
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.register_consumer('user', 'pass', 'my-org')
        url = mock_post.call_args[0][0]
        assert url == 'https://candlepin.example.com/sub/consumers'

    def test_passes_org_as_query_param(self):
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.register_consumer('user', 'pass', 'test-org-123')
        kwargs = mock_post.call_args[1]
        assert kwargs.get('params', {}).get('owner') == 'test-org-123'

    def test_uses_basic_auth(self):
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.register_consumer('myuser', 'mypass', 'org')
        kwargs = mock_post.call_args[1]
        assert kwargs.get('auth') == ('myuser', 'mypass')

    def test_consumer_type_is_aap(self):
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.register_consumer('user', 'pass', 'org')
        payload = mock_post.call_args[1]['json']
        assert payload['type']['label'] == 'aap'

    def test_install_uuid_used_in_facts(self):
        client = CandlepinClient()
        install_uuid = 'aaaabbbb-cccc-dddd-eeee-ffffffffffff'
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.register_consumer('user', 'pass', 'org', install_uuid=install_uuid)
        payload = mock_post.call_args[1]['json']
        assert payload['facts']['aap.instance_uuid'] == install_uuid

    def test_random_uuid_used_when_install_uuid_not_given(self):
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()) as mock_post:
            client.register_consumer('user', 'pass', 'org')
        payload = mock_post.call_args[1]['json']
        # just verify it's set to something non-empty
        assert payload['facts']['aap.instance_uuid']

    def test_raises_on_http_error(self):
        client = CandlepinClient()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 401
        resp.text = 'Unauthorized'
        with patch('requests.post', return_value=resp):
            with pytest.raises(RuntimeError, match='401'):
                client.register_consumer('bad', 'creds', 'org')

    def test_raises_on_network_error(self):
        client = CandlepinClient()
        with patch('requests.post', side_effect=requests.exceptions.ConnectionError('refused')):
            with pytest.raises(RuntimeError, match='network error'):
                client.register_consumer('user', 'pass', 'org')

    def test_raises_when_uuid_missing_from_response(self):
        client = CandlepinClient()
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {'idCert': {'cert': self.SAMPLE_CERT, 'key': self.SAMPLE_KEY}}
        with patch('requests.post', return_value=resp):
            with pytest.raises(RuntimeError, match='uuid'):
                client.register_consumer('user', 'pass', 'org')

    def test_raises_when_idcert_missing(self):
        client = CandlepinClient()
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {'uuid': self.SAMPLE_UUID}
        with patch('requests.post', return_value=resp):
            with pytest.raises(RuntimeError, match='idCert'):
                client.register_consumer('user', 'pass', 'org')

    def test_logs_info_on_success(self):
        client = CandlepinClient()
        with patch('requests.post', return_value=self._mock_success_response()):
            with patch('metrics_utility.library.candlepin.client.logger') as mock_log:
                client.register_consumer('user', 'pass', 'org')
        mock_log.info.assert_called_once()
        assert self.SAMPLE_UUID in mock_log.info.call_args[0][0]


class TestDiscoverOrg:
    def _mock_owners_response(self, owners):
        resp = MagicMock()
        resp.json.return_value = owners
        resp.raise_for_status = MagicMock()
        return resp

    def test_returns_org_key_from_first_owner(self):
        client = CandlepinClient()
        owners = [{'key': 'my-org', 'displayName': 'My Org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)):
            org = client.discover_org('user', 'pass')
        assert org == 'my-org'

    def test_returns_first_org_when_multiple_exist(self):
        client = CandlepinClient()
        owners = [{'key': 'first-org'}, {'key': 'second-org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)):
            org = client.discover_org('user', 'pass')
        assert org == 'first-org'

    def test_logs_warning_when_multiple_orgs(self):
        client = CandlepinClient()
        owners = [{'key': 'first-org'}, {'key': 'second-org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)):
            with patch('metrics_utility.library.candlepin.client.logger') as mock_log:
                client.discover_org('user', 'pass')
        mock_log.warning.assert_called_once()
        assert 'first-org' in mock_log.warning.call_args[0][0]

    def test_returns_none_when_no_owners(self):
        client = CandlepinClient()
        with patch('requests.get', return_value=self._mock_owners_response([])):
            org = client.discover_org('user', 'pass')
        assert org is None

    def test_returns_none_when_key_missing_in_owner(self):
        client = CandlepinClient()
        owners = [{'displayName': 'No Key Org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)):
            org = client.discover_org('user', 'pass')
        assert org is None

    def test_returns_none_on_request_exception(self):
        import requests as req

        client = CandlepinClient()
        with patch('requests.get', side_effect=req.exceptions.ConnectionError('refused')):
            org = client.discover_org('user', 'pass')
        assert org is None

    def test_returns_none_on_http_error(self):
        client = CandlepinClient()
        resp = MagicMock()
        resp.raise_for_status.side_effect = Exception('401 Unauthorized')
        with patch('requests.get', return_value=resp):
            org = client.discover_org('user', 'pass')
        assert org is None

    def test_calls_correct_endpoint(self):
        client = CandlepinClient(base_url='https://sub.example.com/subscription')
        owners = [{'key': 'org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)) as mock_get:
            client.discover_org('testuser', 'testpass')
        url = mock_get.call_args[0][0]
        assert url == 'https://sub.example.com/subscription/users/testuser/owners'

    def test_uses_basic_auth(self):
        client = CandlepinClient()
        owners = [{'key': 'org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)) as mock_get:
            client.discover_org('myuser', 'mypass')
        assert mock_get.call_args[1]['auth'] == ('myuser', 'mypass')

    def test_logs_info_on_success(self):
        client = CandlepinClient()
        owners = [{'key': 'discovered-org'}]
        with patch('requests.get', return_value=self._mock_owners_response(owners)):
            with patch('metrics_utility.library.candlepin.client.logger') as mock_log:
                client.discover_org('user', 'pass')
        mock_log.info.assert_called_once()
        assert 'discovered-org' in mock_log.info.call_args[0][0]
