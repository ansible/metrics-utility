"""
Unit tests for:
  - metrics_utility.library.candlepin.lifecycle  (parse_cert, needs_renewal, run_candlepin_lifecycle)
  - metrics_utility.management.validation         (_fetch_candlepin_lifecycle_from_db,
                                                   _save_candlepin_cert_to_db,
                                                   _run_candlepin_lifecycle,
                                                   handle_crc_ship_target lifecycle wiring)
"""

import datetime
import json

from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from metrics_utility.library.candlepin.lifecycle import needs_renewal, parse_cert, run_candlepin_lifecycle
from metrics_utility.management.validation import (
    CANDLEPIN_CERT_SETTING_KEY,
    CANDLEPIN_KEY_SETTING_KEY,
    CANDLEPIN_UUID_SETTING_KEY,
    _fetch_candlepin_lifecycle_from_db,
    _run_candlepin_lifecycle,
    _save_candlepin_cert_to_db,
    handle_crc_ship_target,
)


CONSUMER_UUID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'

SAMPLE_NEW_CERT = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
SAMPLE_NEW_KEY = '-----BEGIN RSA PRIVATE KEY-----\nnewkey==\n-----END RSA PRIVATE KEY-----\n'


# ---------------------------------------------------------------------------
# Cert generation helpers
# ---------------------------------------------------------------------------


def _generate_cert(expired=False, days_until_expiry=365):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(timezone.utc)
    if expired:
        not_before = now - datetime.timedelta(days=400)
        not_after = now - datetime.timedelta(days=1)
    else:
        not_before = now
        not_after = now + datetime.timedelta(days=days_until_expiry)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'test-consumer')]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'Candlepin CA')]))
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
def valid_cert_and_key():
    return _generate_cert(expired=False, days_until_expiry=365)


@pytest.fixture
def expiring_cert_and_key():
    return _generate_cert(expired=False, days_until_expiry=10)


@pytest.fixture
def expired_cert_and_key():
    return _generate_cert(expired=True)


# ---------------------------------------------------------------------------
# parse_cert
# ---------------------------------------------------------------------------


class TestParseCert:
    def test_returns_serial(self, valid_cert_and_key):
        cert_pem, _ = valid_cert_and_key
        info = parse_cert(cert_pem)
        assert isinstance(info['serial'], str)
        assert len(info['serial']) > 0

    def test_returns_cn(self, valid_cert_and_key):
        cert_pem, _ = valid_cert_and_key
        info = parse_cert(cert_pem)
        assert info['cn'] == 'test-consumer'

    def test_returns_issuer_cn(self, valid_cert_and_key):
        cert_pem, _ = valid_cert_and_key
        info = parse_cert(cert_pem)
        assert info['issuer_cn'] == 'Candlepin CA'

    def test_days_remaining_positive_for_valid_cert(self, valid_cert_and_key):
        cert_pem, _ = valid_cert_and_key
        info = parse_cert(cert_pem)
        assert info['days_remaining'] > 0

    def test_days_remaining_negative_for_expired_cert(self, expired_cert_and_key):
        cert_pem, _ = expired_cert_and_key
        info = parse_cert(cert_pem)
        assert info['days_remaining'] < 0

    def test_validity_days_correct(self, valid_cert_and_key):
        cert_pem, _ = valid_cert_and_key
        info = parse_cert(cert_pem)
        assert info['validity_days'] == 365

    def test_raises_on_invalid_pem(self):
        with pytest.raises(ValueError, match='Could not parse'):
            parse_cert('not-a-pem')

    def test_raises_on_empty_string(self):
        with pytest.raises(ValueError):
            parse_cert('')


# ---------------------------------------------------------------------------
# needs_renewal
# ---------------------------------------------------------------------------


class TestNeedsRenewal:
    def test_false_when_days_remaining_exceeds_threshold(self, valid_cert_and_key):
        cert_pem, _ = valid_cert_and_key
        assert needs_renewal(cert_pem, 30) is False

    def test_true_when_within_threshold(self, expiring_cert_and_key):
        cert_pem, _ = expiring_cert_and_key
        assert needs_renewal(cert_pem, 30) is True

    def test_true_when_cert_already_expired(self, expired_cert_and_key):
        cert_pem, _ = expired_cert_and_key
        assert needs_renewal(cert_pem, 30) is True

    def test_boundary_exactly_at_threshold(self, valid_cert_and_key):
        cert_pem, _ = _generate_cert(days_until_expiry=30)
        result = needs_renewal(cert_pem, 30)
        assert result is True

    def test_raises_on_invalid_pem(self):
        with pytest.raises(ValueError):
            needs_renewal('not-a-pem', 30)


# ---------------------------------------------------------------------------
# run_candlepin_lifecycle
# ---------------------------------------------------------------------------


class TestRunCandlepinLifecycle:
    def test_healthy_cert_does_checkin_but_no_renewal(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            instance = MockClient.return_value
            instance.checkin.return_value = True
            result_cert, result_key = run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        instance.checkin.assert_called_once_with(CONSUMER_UUID, cert_pem, key_pem)
        instance.regenerate_cert.assert_not_called()
        assert result_cert == cert_pem
        assert result_key == key_pem

    def test_expiring_cert_triggers_renewal(self, expiring_cert_and_key):
        cert_pem, key_pem = expiring_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            instance = MockClient.return_value
            instance.checkin.return_value = True
            instance.regenerate_cert.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
            result_cert, result_key = run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID, renewal_days=30)
        instance.regenerate_cert.assert_called_once_with(CONSUMER_UUID, cert_pem, key_pem)
        assert result_cert == SAMPLE_NEW_CERT
        assert result_key == SAMPLE_NEW_KEY

    def test_expired_cert_triggers_renewal(self, expired_cert_and_key):
        cert_pem, key_pem = expired_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            instance = MockClient.return_value
            instance.checkin.return_value = True
            instance.regenerate_cert.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
            result_cert, result_key = run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        instance.regenerate_cert.assert_called_once()

    def test_checkin_failure_does_not_abort(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            instance = MockClient.return_value
            instance.checkin.return_value = False
            result_cert, result_key = run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        assert result_cert == cert_pem

    def test_regeneration_failure_raises(self, expiring_cert_and_key):
        cert_pem, key_pem = expiring_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            instance = MockClient.return_value
            instance.checkin.return_value = True
            instance.regenerate_cert.side_effect = RuntimeError('Candlepin 500')
            with pytest.raises(RuntimeError, match='Candlepin 500'):
                run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID, renewal_days=30)

    def test_unparseable_cert_returns_originals(self):
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            result = run_candlepin_lifecycle('not-a-cert', 'not-a-key', CONSUMER_UUID)
        MockClient.return_value.checkin.assert_not_called()
        assert result == ('not-a-cert', 'not-a-key')

    def test_client_receives_correct_candlepin_url(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            MockClient.return_value.checkin.return_value = True
            run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID, candlepin_url='https://sub.example.com')
        MockClient.assert_called_once_with(base_url='https://sub.example.com', candlepin_ca=None, proxy=None)

    def test_client_receives_candlepin_ca(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.library.candlepin.lifecycle.CandlepinClient') as MockClient:
            MockClient.return_value.checkin.return_value = True
            run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID, candlepin_ca='/etc/rhsm/ca/redhat-uep.pem')
        _, kwargs = MockClient.call_args
        assert kwargs['candlepin_ca'] == '/etc/rhsm/ca/redhat-uep.pem'


# ---------------------------------------------------------------------------
# _fetch_candlepin_lifecycle_from_db
# ---------------------------------------------------------------------------


def _make_cursor_rows(rows):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


SAMPLE_CERT_PEM = '-----BEGIN CERTIFICATE-----\nMIIBtest\n-----END CERTIFICATE-----\n'
SAMPLE_KEY_PEM = '-----BEGIN RSA PRIVATE KEY-----\nMIIEtest\n-----END RSA PRIVATE KEY-----\n'


class TestFetchCandlepinLifecycleFromDb:
    def test_returns_all_three_when_present(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
            (CANDLEPIN_UUID_SETTING_KEY, json.dumps(CONSUMER_UUID)),
        ]
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows(rows)):
            cert, key, uuid = _fetch_candlepin_lifecycle_from_db()
        assert cert == SAMPLE_CERT_PEM
        assert key == SAMPLE_KEY_PEM
        assert uuid == CONSUMER_UUID

    def test_returns_none_when_uuid_missing(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows(rows)):
            cert, key, uuid = _fetch_candlepin_lifecycle_from_db()
        assert cert == SAMPLE_CERT_PEM
        assert uuid is None

    def test_returns_none_none_none_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB down')):
            cert, key, uuid = _fetch_candlepin_lifecycle_from_db()
        assert cert is None
        assert key is None
        assert uuid is None

    def test_queries_all_three_setting_keys(self):
        mock_conn = _make_cursor_rows([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            _fetch_candlepin_lifecycle_from_db()
        args = mock_conn.__enter__.return_value.execute.call_args[0][1]
        assert CANDLEPIN_CERT_SETTING_KEY in args
        assert CANDLEPIN_KEY_SETTING_KEY in args
        assert CANDLEPIN_UUID_SETTING_KEY in args

    def test_skips_empty_value_rows(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, ''),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
            (CANDLEPIN_UUID_SETTING_KEY, json.dumps(CONSUMER_UUID)),
        ]
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows(rows)):
            cert, key, uuid = _fetch_candlepin_lifecycle_from_db()
        assert cert is None
        assert key == SAMPLE_KEY_PEM


# ---------------------------------------------------------------------------
# _save_candlepin_cert_to_db
# ---------------------------------------------------------------------------


class TestSaveCandlepinCertToDb:
    def test_executes_upsert_for_cert_and_key(self):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.__exit__ = MagicMock(return_value=False)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            _save_candlepin_cert_to_db(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)
        assert mock_cursor.execute.call_count == 2
        calls = [c[0] for c in mock_cursor.execute.call_args_list]
        keys_updated = [c[1][0] for c in calls]
        assert CANDLEPIN_CERT_SETTING_KEY in keys_updated
        assert CANDLEPIN_KEY_SETTING_KEY in keys_updated

    def test_does_not_raise_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB error')):
            _save_candlepin_cert_to_db(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)

    def test_logs_error_on_db_failure(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB error')):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _save_candlepin_cert_to_db(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)
        mock_log.error.assert_called_once()
        assert 'Candlepin' in mock_log.error.call_args[0][0]


# ---------------------------------------------------------------------------
# _run_candlepin_lifecycle (validation.py orchestration wrapper)
# ---------------------------------------------------------------------------


class TestRunCandlepinLifecycleValidation:
    def test_skips_lifecycle_when_uuid_missing(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle') as mock_lc:
            result = _run_candlepin_lifecycle(cert_pem, key_pem, consumer_uuid=None)
        mock_lc.assert_not_called()
        assert result == (cert_pem, key_pem)

    def test_logs_warning_when_uuid_missing(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle'):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _run_candlepin_lifecycle(cert_pem, key_pem, consumer_uuid=None)
        mock_log.warning.assert_called_once()

    def test_calls_run_candlepin_lifecycle_with_correct_args(self, valid_cert_and_key, monkeypatch):
        cert_pem, key_pem = valid_cert_and_key
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_URL', 'https://sub.example.com')
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_RENEWAL_DAYS', '45')
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_CA', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(cert_pem, key_pem)) as mock_lc:
            _run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        mock_lc.assert_called_once_with(
            cert_pem,
            key_pem,
            CONSUMER_UUID,
            candlepin_url='https://sub.example.com',
            renewal_days=45,
            candlepin_ca=None,
            proxy=None,
        )

    def test_saves_cert_to_db_when_renewed(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)):
            with patch('metrics_utility.management.validation._save_candlepin_cert_to_db') as mock_save:
                _run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        mock_save.assert_called_once_with(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)

    def test_does_not_save_when_cert_unchanged(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(cert_pem, key_pem)):
            with patch('metrics_utility.management.validation._save_candlepin_cert_to_db') as mock_save:
                _run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        mock_save.assert_not_called()

    def test_returns_original_cert_on_lifecycle_exception(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', side_effect=RuntimeError('Candlepin down')):
            result = _run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        assert result == (cert_pem, key_pem)

    def test_logs_error_on_lifecycle_exception(self, valid_cert_and_key):
        cert_pem, key_pem = valid_cert_and_key
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', side_effect=RuntimeError('Candlepin down')):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _run_candlepin_lifecycle(cert_pem, key_pem, CONSUMER_UUID)
        mock_log.error.assert_called_once()


# ---------------------------------------------------------------------------
# handle_crc_ship_target lifecycle integration
# ---------------------------------------------------------------------------


class TestHandleCrcShipTargetLifecycleWiring:
    @pytest.fixture(autouse=True)
    def required_env(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
        monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
        monkeypatch.delenv('METRICS_UTILITY_RED_HAT_ORG_ID', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_SHIP_PATH', raising=False)

    def _rows_with_uuid(self):
        return [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
            (CANDLEPIN_UUID_SETTING_KEY, json.dumps(CONSUMER_UUID)),
        ]

    def test_lifecycle_not_called_when_flag_disabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'false')
        rows = self._rows_with_uuid()
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows(rows)):
            with patch('metrics_utility.management.validation._run_candlepin_lifecycle') as mock_lc:
                handle_crc_ship_target()
        mock_lc.assert_not_called()

    def test_lifecycle_called_when_flag_enabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'true')
        rows = self._rows_with_uuid()
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows(rows)):
            with patch('metrics_utility.management.validation._run_candlepin_lifecycle', return_value=(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)) as mock_lc:
                handle_crc_ship_target()
        mock_lc.assert_called_once_with(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)

    def test_renewed_cert_injected_into_billing_params(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'true')
        rows = self._rows_with_uuid()
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows(rows)):
            with patch('metrics_utility.management.validation._run_candlepin_lifecycle', return_value=(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)):
                params = handle_crc_ship_target()
        assert params['candlepin_cert_pem'] == SAMPLE_NEW_CERT
        assert params['candlepin_key_pem'] == SAMPLE_NEW_KEY

    def test_lifecycle_skipped_when_no_cert_in_db(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'true')
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows([])):
            with patch('metrics_utility.management.validation._run_candlepin_lifecycle') as mock_lc:
                params = handle_crc_ship_target()
        mock_lc.assert_not_called()
        assert 'candlepin_cert_pem' not in params

    def test_billing_provider_params_always_present(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'true')
        with patch('django.db.connection.cursor', return_value=_make_cursor_rows([])):
            params = handle_crc_ship_target()
        assert params['billing_provider'] == 'aws'
        assert params['billing_account_id'] == '123456789012'
