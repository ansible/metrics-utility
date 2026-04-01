import datetime
import os
import tempfile

from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import metrics_utility.base.package as base_package

from metrics_utility.automation_controller_billing.package.package_crc import PackageCRC, _is_cert_valid


def _generate_cert(expired=False):
    """Generate a self-signed X.509 certificate for testing."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    if expired:
        not_before = datetime.datetime(2020, 1, 1, tzinfo=timezone.utc)
        not_after = datetime.datetime(2021, 1, 1, tzinfo=timezone.utc)
    else:
        now = datetime.datetime.now(timezone.utc)
        not_before = now
        not_after = now + datetime.timedelta(days=365)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'test')]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'test')]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
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
def valid_cert_pem():
    cert_pem, _ = _generate_cert(expired=False)
    return cert_pem


@pytest.fixture
def expired_cert_pem():
    cert_pem, _ = _generate_cert(expired=True)
    return cert_pem


@pytest.fixture
def valid_cert_and_key():
    return _generate_cert(expired=False)


def _make_package(cert_pem=None, key_pem=None):
    """Create a PackageCRC with a mocked collector."""
    collector = MagicMock()
    params = {}
    if cert_pem is not None:
        params['candlepin_cert_pem'] = cert_pem
    if key_pem is not None:
        params['candlepin_key_pem'] = key_pem
    collector.billing_provider_params = params
    return PackageCRC(collector)


class TestIsCertValid:
    def test_valid_cert_returns_true(self, valid_cert_pem):
        assert _is_cert_valid(valid_cert_pem) is True

    def test_expired_cert_returns_false(self, expired_cert_pem):
        assert _is_cert_valid(expired_cert_pem) is False

    def test_invalid_pem_string_returns_false(self):
        assert _is_cert_valid('not-a-cert') is False

    def test_empty_string_returns_false(self):
        assert _is_cert_valid('') is False

    def test_expired_cert_logs_warning(self, expired_cert_pem):
        with patch('metrics_utility.library.candlepin.lifecycle.logger') as mock_logger:
            _is_cert_valid(expired_cert_pem)
        mock_logger.warning.assert_called_once()
        assert 'expired' in mock_logger.warning.call_args[0][0]

    def test_invalid_pem_logs_warning(self):
        with patch('metrics_utility.library.candlepin.lifecycle.logger') as mock_logger:
            _is_cert_valid('not-a-cert')
        mock_logger.warning.assert_called_once()
        assert 'Could not parse' in mock_logger.warning.call_args[0][0]


class TestShippingAuthMode:
    def test_returns_certificates_when_valid_cert_and_key(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        assert package.shipping_auth_mode() == PackageCRC.SHIPPING_AUTH_CERTIFICATES

    def test_returns_service_account_when_no_cert_or_key(self):
        package = _make_package()
        assert package.shipping_auth_mode() == PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT

    def test_returns_service_account_when_cert_present_but_no_key(self, valid_cert_pem):
        package = _make_package(cert_pem=valid_cert_pem)
        assert package.shipping_auth_mode() == PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT

    def test_returns_service_account_when_key_present_but_no_cert(self):
        _, key_pem = _generate_cert()
        package = _make_package(key_pem=key_pem)
        assert package.shipping_auth_mode() == PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT

    def test_returns_service_account_when_cert_is_expired(self, expired_cert_pem):
        _, key_pem = _generate_cert()
        package = _make_package(cert_pem=expired_cert_pem, key_pem=key_pem)
        assert package.shipping_auth_mode() == PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT

    def test_caches_auth_mode_after_first_call(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        with patch('metrics_utility.automation_controller_billing.package.package_crc._is_cert_valid', return_value=True) as mock_valid:
            package.shipping_auth_mode()
            package.shipping_auth_mode()
        mock_valid.assert_called_once()

    def test_pre_resolved_mode_is_returned_immediately(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        package._resolved_auth_mode = PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT
        # Even with valid cert+key, pre-resolved mode wins
        assert package.shipping_auth_mode() == PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT


class TestIsShippingConfigured:
    def test_base_tar_path_check_runs_in_cert_mode(self, valid_cert_and_key):
        """Regression: super().is_shipping_configured() must be called (not super()) so tar_path is checked."""
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        package.tar_path = None
        assert package.is_shipping_configured() is False

    def test_base_tar_path_nonexistent_file_returns_false(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        package.tar_path = '/nonexistent/path/metrics.tar.gz'
        assert package.is_shipping_configured() is False

    def test_cert_mode_returns_true_when_ingress_url_set(self, valid_cert_and_key, monkeypatch):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        monkeypatch.setenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            assert package.is_shipping_configured() is True

    def test_cert_mode_returns_false_when_ingress_url_missing(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            with patch.object(PackageCRC, 'get_ingress_url', return_value=''):
                assert package.is_shipping_configured() is False

    def test_service_account_mode_returns_true_when_all_vars_set(self, monkeypatch):
        package = _make_package()
        monkeypatch.setenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')
        monkeypatch.setenv('METRICS_UTILITY_CRC_SSO_URL', 'https://sso.redhat.com/token')
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID', 'test-client-id')
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET', 'test-secret')
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            assert package.is_shipping_configured() is True

    def test_service_account_mode_returns_false_when_ingress_url_missing(self, monkeypatch):
        package = _make_package()
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID', 'test-client-id')
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET', 'test-secret')
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            with patch.object(PackageCRC, 'get_ingress_url', return_value=''):
                assert package.is_shipping_configured() is False

    def test_service_account_mode_returns_false_when_sso_url_missing(self, monkeypatch):
        package = _make_package()
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID', 'test-client-id')
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET', 'test-secret')
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            with patch.object(PackageCRC, 'get_sso_url', return_value=''):
                assert package.is_shipping_configured() is False

    def test_service_account_mode_returns_false_when_account_id_missing(self, monkeypatch):
        package = _make_package()
        monkeypatch.setenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')
        monkeypatch.setenv('METRICS_UTILITY_CRC_SSO_URL', 'https://sso.redhat.com/token')
        monkeypatch.delenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET', 'test-secret')
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            assert package.is_shipping_configured() is False

    def test_service_account_mode_returns_false_when_secret_missing(self, monkeypatch):
        package = _make_package()
        monkeypatch.setenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')
        monkeypatch.setenv('METRICS_UTILITY_CRC_SSO_URL', 'https://sso.redhat.com/token')
        monkeypatch.setenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID', 'test-client-id')
        monkeypatch.delenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET', raising=False)
        with tempfile.NamedTemporaryFile() as f:
            package.tar_path = f.name
            assert package.is_shipping_configured() is False

    def test_base_check_fails_with_error_in_tar_path(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        package.tar_path = 'Error: something went wrong'
        assert package.is_shipping_configured() is False


class TestShip:
    def test_service_account_mode_delegates_directly_to_super(self):
        package = _make_package()
        with patch.object(base_package.Package, 'ship', return_value=True) as mock_super:
            result = package.ship()
        assert result is True
        mock_super.assert_called_once()

    def test_mtls_mode_invokes_super_ship(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        with patch.object(base_package.Package, 'ship', return_value=True) as mock_super:
            result = package.ship()
        assert result is True
        mock_super.assert_called_once()

    def test_mtls_mode_creates_temp_files_with_cert_and_key_content(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        captured = {}

        def capture():
            captured['cert_path'] = package._temp_cert_path
            captured['key_path'] = package._temp_key_path
            with open(package._temp_cert_path) as f:
                captured['cert_content'] = f.read()
            with open(package._temp_key_path) as f:
                captured['key_content'] = f.read()
            return True

        with patch.object(base_package.Package, 'ship', side_effect=capture):
            package.ship()

        assert captured['cert_content'] == cert_pem
        assert captured['key_content'] == key_pem

    def test_mtls_mode_sets_temp_file_permissions_to_0600(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        seen_modes = {}
        original_chmod = os.chmod

        def tracking_chmod(path, mode):
            seen_modes[path] = mode
            original_chmod(path, mode)

        with patch('os.chmod', side_effect=tracking_chmod):
            with patch.object(base_package.Package, 'ship', return_value=True):
                package.ship()

        assert len(seen_modes) == 2
        for mode in seen_modes.values():
            assert mode == 0o600

    def test_mtls_mode_cleans_up_temp_files_on_success(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        captured = {}

        def capture():
            captured['cert'] = package._temp_cert_path
            captured['key'] = package._temp_key_path
            return True

        with patch.object(base_package.Package, 'ship', side_effect=capture):
            package.ship()

        assert not os.path.exists(captured['cert'])
        assert not os.path.exists(captured['key'])
        assert package._temp_cert_path is None
        assert package._temp_key_path is None

    def test_ssl_error_raises_when_no_service_account_credentials(self, valid_cert_and_key):
        """If mTLS fails and no service account creds exist, raise FailedToUploadPayload instead of silently returning False."""
        from metrics_utility.exceptions import FailedToUploadPayload

        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        with patch.object(base_package.Package, 'ship', side_effect=requests.exceptions.SSLError('handshake failed')):
            with patch.object(PackageCRC, '_get_rh_user', return_value=None):
                with patch.object(PackageCRC, '_get_rh_password', return_value=None):
                    with pytest.raises(FailedToUploadPayload) as exc_info:
                        package.ship()

        assert 'mTLS upload failed' in str(exc_info.value)
        assert 'METRICS_UTILITY_SERVICE_ACCOUNT_ID' in str(exc_info.value)
        assert 'handshake failed' in str(exc_info.value)

    def test_ssl_error_raises_preserves_original_ssl_error_as_cause(self, valid_cert_and_key):
        """The raised FailedToUploadPayload must chain the original SSLError via __cause__."""
        from metrics_utility.exceptions import FailedToUploadPayload

        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        ssl_error = requests.exceptions.SSLError('handshake failed')

        with patch.object(base_package.Package, 'ship', side_effect=ssl_error):
            with patch.object(PackageCRC, '_get_rh_user', return_value=None):
                with patch.object(PackageCRC, '_get_rh_password', return_value=None):
                    with pytest.raises(FailedToUploadPayload) as exc_info:
                        package.ship()

        assert exc_info.value.__cause__ is ssl_error

    def test_ssl_error_falls_back_to_service_account_when_credentials_present(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        with patch.object(PackageCRC, '_get_rh_user', return_value='client-id'):
            with patch.object(PackageCRC, '_get_rh_password', return_value='secret'):
                with patch.object(base_package.Package, 'ship', side_effect=[requests.exceptions.SSLError('handshake failed'), True]):
                    result = package.ship()

        assert result is True
        assert package._resolved_auth_mode == PackageCRC.SHIPPING_AUTH_SERVICE_ACCOUNT

    def test_ssl_error_cleans_up_temp_files(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        captured = {}
        call_count = [0]

        def raise_first_then_succeed():
            call_count[0] += 1
            if call_count[0] == 1:
                captured['cert'] = package._temp_cert_path
                captured['key'] = package._temp_key_path
                raise requests.exceptions.SSLError('SSL error')
            return True

        with patch.object(PackageCRC, '_get_rh_user', return_value='client-id'):
            with patch.object(PackageCRC, '_get_rh_password', return_value='secret'):
                with patch.object(base_package.Package, 'ship', side_effect=raise_first_then_succeed):
                    package.ship()

        assert not os.path.exists(captured['cert'])
        assert not os.path.exists(captured['key'])

    def test_ssl_error_logs_error_message(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)

        with patch.object(PackageCRC, '_get_rh_user', return_value='client-id'):
            with patch.object(PackageCRC, '_get_rh_password', return_value='secret'):
                with patch.object(base_package.Package, 'ship', side_effect=[requests.exceptions.SSLError('handshake'), True]):
                    with patch('metrics_utility.automation_controller_billing.package.package_crc.logger') as mock_logger:
                        package.ship()

        mock_logger.error.assert_called_once()
        assert 'mTLS upload failed' in mock_logger.error.call_args[0][0]
        assert 'service account' in mock_logger.error.call_args[0][0]


class TestGetClientCertificates:
    def test_returns_temp_paths_when_set(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        package = _make_package(cert_pem=cert_pem, key_pem=key_pem)
        package._temp_cert_path = '/tmp/cert.pem'
        package._temp_key_path = '/tmp/key.pem'
        assert package._get_client_certificates() == ('/tmp/cert.pem', '/tmp/key.pem')

    def test_falls_back_to_super_when_temp_paths_not_set(self):
        package = _make_package()
        result = package._get_client_certificates()
        assert result == (base_package.Package.DEFAULT_RHSM_CERT_FILE, base_package.Package.DEFAULT_RHSM_KEY_FILE)
