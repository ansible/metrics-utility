import json

from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.management.validation import (
    CANDLEPIN_CERT_SETTING_KEY,
    CANDLEPIN_KEY_SETTING_KEY,
    CANDLEPIN_UUID_PLACEHOLDER,
    SUBSCRIPTIONS_PASSWORD_SETTING_KEY,
    SUBSCRIPTIONS_USERNAME_SETTING_KEY,
    _fetch_candlepin_lifecycle_from_db,
    _fetch_registration_credentials_from_db,
    _register_candlepin_consumer,
    _run_candlepin_lifecycle,
    _save_candlepin_registration_to_db,
    handle_crc_ship_target,
)


SAMPLE_CERT_PEM = '-----BEGIN CERTIFICATE-----\nMIIBtest==\n-----END CERTIFICATE-----\n'
SAMPLE_KEY_PEM = '-----BEGIN RSA PRIVATE KEY-----\nMIIEtest==\n-----END RSA PRIVATE KEY-----\n'


def _make_cursor_with_rows(rows):
    """Return a mock cursor context manager whose fetchall() returns rows."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


class TestFetchCandlepinLifecycleFromDb:
    def test_returns_cert_and_key_when_both_present(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, _ = _fetch_candlepin_lifecycle_from_db()

        assert cert == SAMPLE_CERT_PEM
        assert key == SAMPLE_KEY_PEM

    def test_returns_none_none_when_no_rows(self):
        mock_conn, _ = _make_cursor_with_rows([])

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, _ = _fetch_candlepin_lifecycle_from_db()

        assert cert is None
        assert key is None

    def test_returns_none_for_missing_key(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            # no key row
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, _ = _fetch_candlepin_lifecycle_from_db()

        assert cert == SAMPLE_CERT_PEM
        assert key is None

    def test_returns_none_for_missing_cert(self):
        rows = [
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
            # no cert row
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, _ = _fetch_candlepin_lifecycle_from_db()

        assert cert is None
        assert key == SAMPLE_KEY_PEM

    def test_skips_rows_with_empty_value(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, ''),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, _ = _fetch_candlepin_lifecycle_from_db()

        assert cert is None
        assert key == SAMPLE_KEY_PEM

    def test_returns_none_none_on_db_exception(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB connection refused')):
            cert, key, _ = _fetch_candlepin_lifecycle_from_db()

        assert cert is None
        assert key is None

    def test_logs_warning_on_db_exception(self):
        with patch('django.db.connection.cursor', side_effect=Exception('timeout')):
            with patch('metrics_utility.management.validation.logger') as mock_logger:
                _fetch_candlepin_lifecycle_from_db()

        mock_logger.warning.assert_called_once()
        assert 'Could not fetch Candlepin' in mock_logger.warning.call_args[0][0]

    def test_queries_both_setting_keys(self):
        mock_conn, mock_cursor = _make_cursor_with_rows([])

        with patch('django.db.connection.cursor', return_value=mock_conn):
            _fetch_candlepin_lifecycle_from_db()

        sql_call = mock_cursor.execute.call_args[0][0]
        args = mock_cursor.execute.call_args[0][1]
        assert CANDLEPIN_CERT_SETTING_KEY in args
        assert CANDLEPIN_KEY_SETTING_KEY in args
        assert 'conf_setting' in sql_call


class TestHandleCrcShipTargetCandlepin:
    @pytest.fixture(autouse=True)
    def set_required_env(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
        monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
        monkeypatch.delenv('METRICS_UTILITY_RED_HAT_ORG_ID', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_SHIP_PATH', raising=False)

    def test_injects_cert_and_key_when_both_available(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            params = handle_crc_ship_target()

        assert params['candlepin_cert_pem'] == SAMPLE_CERT_PEM
        assert params['candlepin_key_pem'] == SAMPLE_KEY_PEM

    def test_does_not_inject_when_cert_missing(self):
        rows = [
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            params = handle_crc_ship_target()

        assert 'candlepin_cert_pem' not in params
        assert 'candlepin_key_pem' not in params

    def test_does_not_inject_when_key_missing(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            params = handle_crc_ship_target()

        assert 'candlepin_cert_pem' not in params
        assert 'candlepin_key_pem' not in params

    def test_does_not_inject_when_db_fails(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB error')):
            params = handle_crc_ship_target()

        assert 'candlepin_cert_pem' not in params
        assert 'candlepin_key_pem' not in params

    def test_logs_info_when_cert_loaded(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.logger') as mock_logger:
                handle_crc_ship_target()

        info_messages = [str(call) for call in mock_logger.info.call_args_list]
        assert any('mTLS' in msg for msg in info_messages)

    def test_logs_info_when_no_cert_found(self):
        mock_conn, _ = _make_cursor_with_rows([])

        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.logger') as mock_logger:
                handle_crc_ship_target()

        info_messages = [str(call) for call in mock_logger.info.call_args_list]
        assert any('service account' in msg for msg in info_messages)

    def test_billing_provider_params_always_includes_billing_fields(self):
        mock_conn, _ = _make_cursor_with_rows([])

        with patch('django.db.connection.cursor', return_value=mock_conn):
            params = handle_crc_ship_target()

        assert params['billing_provider'] == 'aws'
        assert params['billing_account_id'] == '123456789012'


class TestRunCandlepinLifecyclePlaceholderUUID:
    """_run_candlepin_lifecycle must treat the all-zeros placeholder UUID as absent."""

    def test_placeholder_uuid_skips_lifecycle(self):
        """Placeholder UUID must not be forwarded to run_candlepin_lifecycle."""
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle') as mock_lifecycle:
            result = _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CANDLEPIN_UUID_PLACEHOLDER)

        mock_lifecycle.assert_not_called()
        assert result == (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)

    def test_placeholder_uuid_logs_warning(self):
        with patch('metrics_utility.management.validation.logger') as mock_logger:
            _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CANDLEPIN_UUID_PLACEHOLDER)

        mock_logger.warning.assert_called_once()
        msg = mock_logger.warning.call_args[0][0]
        assert 'placeholder' in msg or 'not set' in msg

    def test_none_uuid_skips_lifecycle(self):
        """None consumer_uuid (absent DB row) must also skip lifecycle."""
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle') as mock_lifecycle:
            result = _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, None)

        mock_lifecycle.assert_not_called()
        assert result == (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)

    def test_real_uuid_proceeds_with_lifecycle(self):
        """A genuine UUID (non-placeholder, non-None) must invoke run_candlepin_lifecycle."""
        real_uuid = '12345678-1234-1234-1234-123456789abc'
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)) as mock_lifecycle:
            with patch('metrics_utility.management.validation.get_candlepin_url', return_value='https://example.com'):
                with patch('metrics_utility.management.validation.get_renewal_days', return_value=30):
                    with patch('metrics_utility.management.validation.get_candlepin_ca', return_value=None):
                        _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, real_uuid)

        mock_lifecycle.assert_called_once()


# ---------------------------------------------------------------------------
# Registration tests
# ---------------------------------------------------------------------------

CONSUMER_UUID = 'cccccccc-dddd-eeee-ffff-000000000000'
SAMPLE_NEW_CERT = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
SAMPLE_NEW_KEY = '-----BEGIN RSA PRIVATE KEY-----\nnewkey==\n-----END RSA PRIVATE KEY-----\n'
SAMPLE_USERNAME = 'rh-user@example.com'
SAMPLE_PASSWORD = 'secret'
SAMPLE_ORG = '1234567'
SAMPLE_INSTALL_UUID = 'aaaabbbb-cccc-dddd-eeee-ffffffffffff'


def _make_cursor_rows(rows):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


class TestFetchRegistrationCredentialsFromDb:
    def _rows(self, username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, org=SAMPLE_ORG, install_uuid=SAMPLE_INSTALL_UUID):
        return [
            (SUBSCRIPTIONS_USERNAME_SETTING_KEY, json.dumps(username)),
            (SUBSCRIPTIONS_PASSWORD_SETTING_KEY, json.dumps(password)),
            ('LICENSE', json.dumps({'account_number': org, 'license_type': 'enterprise'})),
            ('INSTALL_UUID', json.dumps(install_uuid)),
        ]

    def test_returns_all_fields_when_present(self):
        mock_conn = _make_cursor_rows(self._rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            username, password, org, install_uuid = _fetch_registration_credentials_from_db()
        assert username == SAMPLE_USERNAME
        assert password == SAMPLE_PASSWORD
        assert org == SAMPLE_ORG
        assert install_uuid == SAMPLE_INSTALL_UUID

    def test_extracts_account_number_from_license(self):
        rows = [('LICENSE', json.dumps({'account_number': '9999999', 'other': 'ignored'}))]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            _, _, org, _ = _fetch_registration_credentials_from_db()
        assert org == '9999999'

    def test_org_is_none_when_license_missing(self):
        rows = [
            (SUBSCRIPTIONS_USERNAME_SETTING_KEY, json.dumps(SAMPLE_USERNAME)),
            (SUBSCRIPTIONS_PASSWORD_SETTING_KEY, json.dumps(SAMPLE_PASSWORD)),
        ]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            _, _, org, _ = _fetch_registration_credentials_from_db()
        assert org is None

    def test_org_is_none_when_license_has_no_account_number(self):
        rows = [('LICENSE', json.dumps({'license_type': 'enterprise'}))]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            _, _, org, _ = _fetch_registration_credentials_from_db()
        assert org is None

    def test_returns_none_tuple_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB down')):
            result = _fetch_registration_credentials_from_db()
        assert result == (None, None, None, None)

    def test_logs_warning_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('timeout')):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _fetch_registration_credentials_from_db()
        mock_log.warning.assert_called_once()

    def test_queries_include_subscriptions_keys(self):
        mock_conn = _make_cursor_rows([])
        mock_cursor = mock_conn.__enter__.return_value
        with patch('django.db.connection.cursor', return_value=mock_conn):
            _fetch_registration_credentials_from_db()
        args = mock_cursor.execute.call_args[0][1]
        assert SUBSCRIPTIONS_USERNAME_SETTING_KEY in args
        assert SUBSCRIPTIONS_PASSWORD_SETTING_KEY in args


class TestSaveCandlepinRegistrationToDb:
    def test_saves_cert_key_and_uuid(self):
        mock_conn = _make_cursor_rows([])
        mock_cursor = mock_conn.__enter__.return_value
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('django.db.transaction.atomic') as mock_atomic:
                mock_atomic.return_value.__enter__ = MagicMock(return_value=None)
                mock_atomic.return_value.__exit__ = MagicMock(return_value=False)
                _save_candlepin_registration_to_db(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
        # Three UPSERTs: cert, key, uuid
        assert mock_cursor.execute.call_count == 3

    def test_logs_error_on_db_failure(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB error')):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _save_candlepin_registration_to_db(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
        mock_log.error.assert_called_once()

    def test_never_raises(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB error')):
            # must not raise
            _save_candlepin_registration_to_db(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)


class TestRegisterCandlepinConsumer:
    def _db_rows(self):
        return [
            (SUBSCRIPTIONS_USERNAME_SETTING_KEY, json.dumps(SAMPLE_USERNAME)),
            (SUBSCRIPTIONS_PASSWORD_SETTING_KEY, json.dumps(SAMPLE_PASSWORD)),
            ('LICENSE', json.dumps({'account_number': SAMPLE_ORG})),
            ('INSTALL_UUID', json.dumps(SAMPLE_INSTALL_UUID)),
        ]

    def test_returns_cert_key_uuid_on_success(self):
        mock_conn = _make_cursor_rows(self._db_rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                with patch('metrics_utility.management.validation._save_candlepin_registration_to_db'):
                    cert, key, uuid_ = _register_candlepin_consumer()
        assert cert == SAMPLE_NEW_CERT
        assert key == SAMPLE_NEW_KEY
        assert uuid_ == CONSUMER_UUID

    def test_saves_registration_to_db_on_success(self):
        mock_conn = _make_cursor_rows(self._db_rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                with patch('metrics_utility.management.validation._save_candlepin_registration_to_db') as mock_save:
                    _register_candlepin_consumer()
        mock_save.assert_called_once_with(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)

    def test_returns_none_tuple_when_username_missing(self):
        rows = [
            (SUBSCRIPTIONS_PASSWORD_SETTING_KEY, json.dumps(SAMPLE_PASSWORD)),
            ('LICENSE', json.dumps({'account_number': SAMPLE_ORG})),
        ]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            result = _register_candlepin_consumer()
        assert result == (None, None, None)

    def test_returns_none_tuple_when_password_missing(self):
        rows = [
            (SUBSCRIPTIONS_USERNAME_SETTING_KEY, json.dumps(SAMPLE_USERNAME)),
            ('LICENSE', json.dumps({'account_number': SAMPLE_ORG})),
        ]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            result = _register_candlepin_consumer()
        assert result == (None, None, None)

    def test_returns_none_tuple_when_org_missing(self):
        rows = [
            (SUBSCRIPTIONS_USERNAME_SETTING_KEY, json.dumps(SAMPLE_USERNAME)),
            (SUBSCRIPTIONS_PASSWORD_SETTING_KEY, json.dumps(SAMPLE_PASSWORD)),
        ]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            result = _register_candlepin_consumer()
        assert result == (None, None, None)

    def test_returns_none_tuple_when_api_fails(self):
        mock_conn = _make_cursor_rows(self._db_rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.side_effect = RuntimeError('Candlepin down')
                result = _register_candlepin_consumer()
        assert result == (None, None, None)

    def test_logs_error_when_api_fails(self):
        mock_conn = _make_cursor_rows(self._db_rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.side_effect = RuntimeError('Candlepin down')
                with patch('metrics_utility.management.validation.logger') as mock_log:
                    _register_candlepin_consumer()
        mock_log.error.assert_called_once()

    def test_logs_warning_when_username_missing(self):
        mock_conn = _make_cursor_rows([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _register_candlepin_consumer()
        mock_log.warning.assert_called()

    def test_passes_install_uuid_to_register_consumer(self):
        mock_conn = _make_cursor_rows(self._db_rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                with patch('metrics_utility.management.validation._save_candlepin_registration_to_db'):
                    _register_candlepin_consumer()
        call_kwargs = MockClient.return_value.register_consumer.call_args
        assert call_kwargs[1].get('install_uuid') == SAMPLE_INSTALL_UUID or SAMPLE_INSTALL_UUID in call_kwargs[0]


class TestHandleCrcShipTargetRegistration:
    @pytest.fixture(autouse=True)
    def required_env(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
        monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
        monkeypatch.delenv('METRICS_UTILITY_RED_HAT_ORG_ID', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_SHIP_PATH', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', raising=False)

    def test_registration_not_called_when_flag_disabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'false')
        mock_conn = _make_cursor_rows([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation._register_candlepin_consumer') as mock_reg:
                handle_crc_ship_target()
        mock_reg.assert_not_called()

    def test_registration_called_when_cert_absent_and_flag_enabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        mock_conn = _make_cursor_rows([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation._register_candlepin_consumer', return_value=(None, None, None)) as mock_reg:
                handle_crc_ship_target()
        mock_reg.assert_called_once()

    def test_registration_not_called_when_cert_already_in_db(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn = _make_cursor_rows(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation._register_candlepin_consumer') as mock_reg:
                handle_crc_ship_target()
        mock_reg.assert_not_called()

    def test_registered_cert_injected_into_billing_params(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        mock_conn = _make_cursor_rows([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch(
                'metrics_utility.management.validation._register_candlepin_consumer', return_value=(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
            ):
                params = handle_crc_ship_target()
        assert params['candlepin_cert_pem'] == SAMPLE_NEW_CERT
        assert params['candlepin_key_pem'] == SAMPLE_NEW_KEY

    def test_no_cert_in_params_when_registration_fails(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        mock_conn = _make_cursor_rows([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('metrics_utility.management.validation._register_candlepin_consumer', return_value=(None, None, None)):
                params = handle_crc_ship_target()
        assert 'candlepin_cert_pem' not in params
        assert 'candlepin_key_pem' not in params
