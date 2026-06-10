"""Tests for Candlepin validation helpers in metrics_utility.management.validation.

DB-level store tests (DBCandlepinStore.load / save_registration / save_cert) live in
test_candlepin_store.py.  This file covers the higher-level orchestration functions
and handle_crc_ship_target() integration.
"""

from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.management.validation import (
    CANDLEPIN_UUID_PLACEHOLDER,
    _fetch_registration_credentials_from_db,
    _register_candlepin_consumer,
    _resolve_registration_credentials,
    _run_candlepin_lifecycle,
    handle_crc_ship_target,
)


SAMPLE_CERT_PEM = '-----BEGIN CERTIFICATE-----\nMIIBtest==\n-----END CERTIFICATE-----\n'
SAMPLE_KEY_PEM = '-----BEGIN RSA PRIVATE KEY-----\nMIIEtest==\n-----END RSA PRIVATE KEY-----\n'
SAMPLE_NEW_CERT = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
SAMPLE_NEW_KEY = '-----BEGIN RSA PRIVATE KEY-----\nnewkey==\n-----END RSA PRIVATE KEY-----\n'
CONSUMER_UUID = 'cccccccc-dddd-eeee-ffff-000000000000'
SAMPLE_USERNAME = 'rh-user@example.com'
SAMPLE_PASSWORD = 'secret'
SAMPLE_ORG = '1234567'
SAMPLE_INSTALL_UUID = 'aaaabbbb-cccc-dddd-eeee-ffffffffffff'


def _make_mock_store(cert_pem=None, key_pem=None, uuid=None):
    store = MagicMock()
    store.load.return_value = (cert_pem, key_pem, uuid)
    store.save_registration.return_value = True
    store.save_cert.return_value = True
    return store


def _make_db_cursor(rows):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


# ---------------------------------------------------------------------------
# _fetch_registration_credentials_from_db (DB fallback, still in validation.py)
# ---------------------------------------------------------------------------


class TestFetchRegistrationCredentialsFromDb:
    def _rows(self, username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, install_uuid=SAMPLE_INSTALL_UUID):
        import json

        return [
            ('SUBSCRIPTIONS_USERNAME', json.dumps(username)),
            ('SUBSCRIPTIONS_PASSWORD', json.dumps(password)),
            ('INSTALL_UUID', json.dumps(install_uuid)),
        ]

    def test_returns_username_password_install_uuid(self):
        mock_conn = _make_db_cursor(self._rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            username, password, install_uuid = _fetch_registration_credentials_from_db()
        assert username == SAMPLE_USERNAME
        assert password == SAMPLE_PASSWORD
        assert install_uuid == SAMPLE_INSTALL_UUID

    def test_prefers_redhat_username_over_subscriptions(self):
        import json

        rows = [
            ('REDHAT_USERNAME', json.dumps('rh-user')),
            ('REDHAT_PASSWORD', json.dumps('rh-pass')),
            ('SUBSCRIPTIONS_USERNAME', json.dumps('sub-user')),
            ('SUBSCRIPTIONS_PASSWORD', json.dumps('sub-pass')),
            ('INSTALL_UUID', json.dumps(SAMPLE_INSTALL_UUID)),
        ]
        mock_conn = _make_db_cursor(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            username, password, _ = _fetch_registration_credentials_from_db()
        assert username == 'rh-user'
        assert password == 'rh-pass'

    def test_falls_back_to_subscriptions_when_redhat_absent(self):
        mock_conn = _make_db_cursor(self._rows())
        with patch('django.db.connection.cursor', return_value=mock_conn):
            username, password, _ = _fetch_registration_credentials_from_db()
        assert username == SAMPLE_USERNAME

    def test_returns_none_tuple_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB down')):
            result = _fetch_registration_credentials_from_db()
        assert result == (None, None, None)

    def test_logs_warning_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('timeout')):
            with patch('metrics_utility.management.validation.logger') as mock_log:
                _fetch_registration_credentials_from_db()
        mock_log.warning.assert_called_once()


# ---------------------------------------------------------------------------
# _resolve_registration_credentials — env vars vs DB fallback
# ---------------------------------------------------------------------------


class TestResolveRegistrationCredentials:
    def test_returns_env_vars_when_set(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_RH_USERNAME', SAMPLE_USERNAME)
        monkeypatch.setenv('METRICS_UTILITY_RH_PASSWORD', SAMPLE_PASSWORD)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'local')

        username, password, _ = _resolve_registration_credentials()
        assert username == SAMPLE_USERNAME
        assert password == SAMPLE_PASSWORD

    def test_returns_none_when_env_vars_absent_and_storage_is_local(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_RH_USERNAME', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_RH_PASSWORD', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'local')

        with patch('metrics_utility.management.validation._fetch_registration_credentials_from_db') as mock_db:
            username, password, _ = _resolve_registration_credentials()

        mock_db.assert_not_called()
        assert username is None
        assert password is None

    def test_falls_back_to_db_when_storage_is_db_and_env_vars_absent(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_RH_USERNAME', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_RH_PASSWORD', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'db')

        with patch(
            'metrics_utility.management.validation._fetch_registration_credentials_from_db',
            return_value=(SAMPLE_USERNAME, SAMPLE_PASSWORD, SAMPLE_INSTALL_UUID),
        ) as mock_db:
            username, password, install_uuid = _resolve_registration_credentials()

        mock_db.assert_called_once()
        assert username == SAMPLE_USERNAME
        assert install_uuid == SAMPLE_INSTALL_UUID

    def test_env_vars_take_priority_over_db(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_RH_USERNAME', 'env-user')
        monkeypatch.setenv('METRICS_UTILITY_RH_PASSWORD', 'env-pass')
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'db')

        with patch(
            'metrics_utility.management.validation._fetch_registration_credentials_from_db',
            return_value=('db-user', 'db-pass', None),
        ):
            username, password, _ = _resolve_registration_credentials()

        assert username == 'env-user'
        assert password == 'env-pass'


# ---------------------------------------------------------------------------
# _register_candlepin_consumer (takes store)
# ---------------------------------------------------------------------------


class TestRegisterCandlepinConsumer:
    def _patch_creds(self, username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, install_uuid=None):
        return patch('metrics_utility.management.validation._resolve_registration_credentials', return_value=(username, password, install_uuid))

    def test_returns_cert_key_uuid_on_success(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.discover_org.return_value = SAMPLE_ORG
                MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                cert, key, uuid_ = _register_candlepin_consumer(mock_store)
        assert cert == SAMPLE_NEW_CERT
        assert key == SAMPLE_NEW_KEY
        assert uuid_ == CONSUMER_UUID

    def test_calls_store_save_registration_on_success(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.discover_org.return_value = SAMPLE_ORG
                MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                _register_candlepin_consumer(mock_store)
        mock_store.save_registration.assert_called_once_with(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)

    def test_uses_env_var_org_without_discovery(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', 'override-org')
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                _register_candlepin_consumer(mock_store)
        MockClient.return_value.discover_org.assert_not_called()

    def test_returns_none_tuple_when_username_missing(self):
        mock_store = _make_mock_store()
        with self._patch_creds(username=None):
            result = _register_candlepin_consumer(mock_store)
        assert result == (None, None, None)

    def test_returns_none_tuple_when_password_missing(self):
        mock_store = _make_mock_store()
        with self._patch_creds(password=None):
            result = _register_candlepin_consumer(mock_store)
        assert result == (None, None, None)

    def test_returns_none_tuple_when_org_discovery_fails(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.discover_org.return_value = None
                result = _register_candlepin_consumer(mock_store)
        assert result == (None, None, None)

    def test_returns_none_tuple_when_api_fails(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.discover_org.return_value = SAMPLE_ORG
                MockClient.return_value.register_consumer.side_effect = RuntimeError('Candlepin down')
                result = _register_candlepin_consumer(mock_store)
        assert result == (None, None, None)

    def test_logs_error_when_api_fails(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.discover_org.return_value = SAMPLE_ORG
                MockClient.return_value.register_consumer.side_effect = RuntimeError('Candlepin down')
                with patch('metrics_utility.management.validation.logger') as mock_log:
                    _register_candlepin_consumer(mock_store)
        mock_log.error.assert_called_once()

    def test_never_raises(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()
        with self._patch_creds():
            with patch('metrics_utility.management.validation.CandlepinClient') as MockClient:
                MockClient.return_value.discover_org.return_value = SAMPLE_ORG
                MockClient.return_value.register_consumer.side_effect = RuntimeError('Candlepin down')
                result = _register_candlepin_consumer(mock_store)
        assert result == (None, None, None)


# ---------------------------------------------------------------------------
# _run_candlepin_lifecycle (takes store)
# ---------------------------------------------------------------------------


class TestRunCandlepinLifecyclePlaceholderUUID:
    def test_placeholder_uuid_skips_lifecycle(self):
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle') as mock_lifecycle:
            result = _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CANDLEPIN_UUID_PLACEHOLDER, mock_store)
        mock_lifecycle.assert_not_called()
        assert result == (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)

    def test_placeholder_uuid_logs_warning(self):
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.logger') as mock_logger:
            _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CANDLEPIN_UUID_PLACEHOLDER, mock_store)
        mock_logger.warning.assert_called_once()

    def test_none_uuid_skips_lifecycle(self):
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle') as mock_lifecycle:
            result = _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, None, mock_store)
        mock_lifecycle.assert_not_called()
        assert result == (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)

    def test_real_uuid_calls_run_candlepin_lifecycle(self):
        real_uuid = '12345678-1234-1234-1234-123456789abc'
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)) as mock_lifecycle:
            with patch('metrics_utility.management.validation.get_candlepin_url', return_value='https://example.com'):
                with patch('metrics_utility.management.validation.get_renewal_days', return_value=30):
                    with patch('metrics_utility.management.validation.get_candlepin_ca', return_value=None):
                        _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, real_uuid, mock_store)
        mock_lifecycle.assert_called_once()

    def test_saves_renewed_cert_via_store(self):
        real_uuid = '12345678-1234-1234-1234-123456789abc'
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)):
            with patch('metrics_utility.management.validation.get_candlepin_url', return_value='https://example.com'):
                with patch('metrics_utility.management.validation.get_renewal_days', return_value=30):
                    with patch('metrics_utility.management.validation.get_candlepin_ca', return_value=None):
                        _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, real_uuid, mock_store)
        mock_store.save_cert.assert_called_once_with(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)

    def test_does_not_save_when_cert_unchanged(self):
        real_uuid = '12345678-1234-1234-1234-123456789abc'
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.run_candlepin_lifecycle', return_value=(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)):
            with patch('metrics_utility.management.validation.get_candlepin_url', return_value='https://example.com'):
                with patch('metrics_utility.management.validation.get_renewal_days', return_value=30):
                    with patch('metrics_utility.management.validation.get_candlepin_ca', return_value=None):
                        _run_candlepin_lifecycle(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, real_uuid, mock_store)
        mock_store.save_cert.assert_not_called()


# ---------------------------------------------------------------------------
# handle_crc_ship_target — cert injection and lifecycle/registration flags
# ---------------------------------------------------------------------------


class TestHandleCrcShipTargetAwxSeeding:
    """When local store is empty, cert should be seeded from AWX conf_setting."""

    @pytest.fixture(autouse=True)
    def set_required_env(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
        monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
        monkeypatch.delenv('METRICS_UTILITY_RED_HAT_ORG_ID', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_SHIP_PATH', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'local')

    def test_seeds_local_store_from_awx_db_when_local_is_empty(self, tmp_path, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_CERT_DIR', str(tmp_path))
        mock_awx = MagicMock()
        mock_awx.load.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)

        with patch('metrics_utility.management.validation.DBCandlepinStore', return_value=mock_awx):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                params = handle_crc_ship_target()

        assert params['candlepin_cert_pem'] == SAMPLE_CERT_PEM
        assert params['candlepin_key_pem'] == SAMPLE_KEY_PEM
        assert (tmp_path / 'cert.pem').exists(), 'cert should have been written to local store'

    def test_does_not_seed_when_local_store_already_has_cert(self, tmp_path, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_CERT_DIR', str(tmp_path))
        (tmp_path / 'cert.pem').write_text(SAMPLE_CERT_PEM)
        (tmp_path / 'key.pem').write_text(SAMPLE_KEY_PEM)
        (tmp_path / 'uuid.txt').write_text(CONSUMER_UUID)

        mock_awx = MagicMock()
        with patch('metrics_utility.management.validation.DBCandlepinStore', return_value=mock_awx):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                params = handle_crc_ship_target()

        mock_awx.load.assert_not_called()
        assert params['candlepin_cert_pem'] == SAMPLE_CERT_PEM

    def test_falls_back_gracefully_when_awx_db_unavailable(self, tmp_path, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_CERT_DIR', str(tmp_path))
        mock_awx = MagicMock()
        mock_awx.load.return_value = (None, None, None)

        with patch('metrics_utility.management.validation.DBCandlepinStore', return_value=mock_awx):
            params = handle_crc_ship_target()

        assert 'candlepin_cert_pem' not in params

    def test_does_not_seed_when_storage_is_db(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'db')
        mock_db_store = MagicMock()
        mock_db_store.load.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)

        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_db_store):
            with patch('metrics_utility.management.validation.DBCandlepinStore') as MockDBClass:
                with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                    handle_crc_ship_target()

        MockDBClass.assert_not_called()


class TestHandleCrcShipTargetCandlepin:
    @pytest.fixture(autouse=True)
    def set_required_env(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
        monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
        monkeypatch.delenv('METRICS_UTILITY_RED_HAT_ORG_ID', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_SHIP_PATH', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', raising=False)

    def test_injects_cert_and_key_when_store_has_both(self):
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                params = handle_crc_ship_target()
        assert params['candlepin_cert_pem'] == SAMPLE_CERT_PEM
        assert params['candlepin_key_pem'] == SAMPLE_KEY_PEM

    def test_does_not_inject_when_store_has_no_cert(self):
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            params = handle_crc_ship_target()
        assert 'candlepin_cert_pem' not in params
        assert 'candlepin_key_pem' not in params

    def test_logs_info_when_cert_loaded(self):
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                with patch('metrics_utility.management.validation.logger') as mock_logger:
                    handle_crc_ship_target()
        info_msgs = [str(c) for c in mock_logger.info.call_args_list]
        assert any('mTLS' in m for m in info_msgs)

    def test_logs_info_when_no_cert_found(self):
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.logger') as mock_logger:
                handle_crc_ship_target()
        info_msgs = [str(c) for c in mock_logger.info.call_args_list]
        assert any('service account' in m for m in info_msgs)

    def test_billing_fields_always_present(self):
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            params = handle_crc_ship_target()
        assert params['billing_provider'] == 'aws'
        assert params['billing_account_id'] == '123456789012'

    def test_near_expiry_warning_logged(self):
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 5}):
                with patch('metrics_utility.management.validation.logger') as mock_logger:
                    handle_crc_ship_target()
        warn_msgs = [str(c) for c in mock_logger.warning.call_args_list]
        assert any('expires in' in m for m in warn_msgs)

    def test_no_warning_when_cert_healthy(self):
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                with patch('metrics_utility.management.validation.logger') as mock_logger:
                    handle_crc_ship_target()
        warn_msgs = [str(c) for c in mock_logger.warning.call_args_list]
        assert not any('expires in' in m for m in warn_msgs)

    def test_registration_not_called_when_flag_disabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'false')
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation._register_candlepin_consumer') as mock_reg:
                handle_crc_ship_target()
        mock_reg.assert_not_called()

    def test_registration_called_when_cert_absent_and_flag_enabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation._register_candlepin_consumer', return_value=(None, None, None)) as mock_reg:
                handle_crc_ship_target()
        mock_reg.assert_called_once_with(mock_store)

    def test_registration_not_called_when_cert_already_in_store(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                with patch('metrics_utility.management.validation._register_candlepin_consumer') as mock_reg:
                    handle_crc_ship_target()
        mock_reg.assert_not_called()

    def test_registered_cert_injected_into_billing_params(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', 'true')
        mock_store = _make_mock_store()
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch(
                'metrics_utility.management.validation._register_candlepin_consumer', return_value=(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
            ):
                with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                    params = handle_crc_ship_target()
        assert params['candlepin_cert_pem'] == SAMPLE_NEW_CERT
        assert params['candlepin_key_pem'] == SAMPLE_NEW_KEY

    def test_lifecycle_called_when_cert_present_and_flag_enabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'true')
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                with patch(
                    'metrics_utility.management.validation._run_candlepin_lifecycle', return_value=(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM)
                ) as mock_lc:
                    handle_crc_ship_target()
        mock_lc.assert_called_once_with(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID, mock_store)

    def test_lifecycle_not_called_when_flag_disabled(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', 'false')
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)
        with patch('metrics_utility.management.validation.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.validation.parse_cert', return_value={'days_remaining': 90}):
                with patch('metrics_utility.management.validation._run_candlepin_lifecycle') as mock_lc:
                    handle_crc_ship_target()
        mock_lc.assert_not_called()
