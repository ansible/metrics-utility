import json

from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.management.validation import (
    CANDLEPIN_CERT_SETTING_KEY,
    CANDLEPIN_KEY_SETTING_KEY,
    _fetch_candlepin_cert_from_db,
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


class TestFetchCandlepinCertFromDb:
    def test_returns_cert_and_key_when_both_present(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key = _fetch_candlepin_cert_from_db()

        assert cert == SAMPLE_CERT_PEM
        assert key == SAMPLE_KEY_PEM

    def test_returns_none_none_when_no_rows(self):
        mock_conn, _ = _make_cursor_with_rows([])

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key = _fetch_candlepin_cert_from_db()

        assert cert is None
        assert key is None

    def test_returns_none_for_missing_key(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, json.dumps(SAMPLE_CERT_PEM)),
            # no key row
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key = _fetch_candlepin_cert_from_db()

        assert cert == SAMPLE_CERT_PEM
        assert key is None

    def test_returns_none_for_missing_cert(self):
        rows = [
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
            # no cert row
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key = _fetch_candlepin_cert_from_db()

        assert cert is None
        assert key == SAMPLE_KEY_PEM

    def test_skips_rows_with_empty_value(self):
        rows = [
            (CANDLEPIN_CERT_SETTING_KEY, ''),
            (CANDLEPIN_KEY_SETTING_KEY, json.dumps(SAMPLE_KEY_PEM)),
        ]
        mock_conn, _ = _make_cursor_with_rows(rows)

        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key = _fetch_candlepin_cert_from_db()

        assert cert is None
        assert key == SAMPLE_KEY_PEM

    def test_returns_none_none_on_db_exception(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB connection refused')):
            cert, key = _fetch_candlepin_cert_from_db()

        assert cert is None
        assert key is None

    def test_logs_warning_on_db_exception(self):
        with patch('django.db.connection.cursor', side_effect=Exception('timeout')):
            with patch('metrics_utility.management.validation.logger') as mock_logger:
                _fetch_candlepin_cert_from_db()

        mock_logger.warning.assert_called_once()
        assert 'Could not fetch Candlepin' in mock_logger.warning.call_args[0][0]

    def test_queries_both_setting_keys(self):
        mock_conn, mock_cursor = _make_cursor_with_rows([])

        with patch('django.db.connection.cursor', return_value=mock_conn):
            _fetch_candlepin_cert_from_db()

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
