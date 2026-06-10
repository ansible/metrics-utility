"""Tests for metrics_utility.library.candlepin.store — both LocalCandlepinStore and DBCandlepinStore."""

import json

from unittest.mock import MagicMock, patch

from metrics_utility.library.candlepin.store import (
    _DB_CERT_KEY,
    _DB_KEY_KEY,
    _DB_UUID_KEY,
    DBCandlepinStore,
    LocalCandlepinStore,
    get_candlepin_store,
)


SAMPLE_CERT = '-----BEGIN CERTIFICATE-----\nMIIBtest==\n-----END CERTIFICATE-----\n'
SAMPLE_KEY = '-----BEGIN RSA PRIVATE KEY-----\nMIIEtest==\n-----END RSA PRIVATE KEY-----\n'
SAMPLE_UUID = 'aaaabbbb-cccc-dddd-eeee-ffffffffffff'


# ---------------------------------------------------------------------------
# LocalCandlepinStore
# ---------------------------------------------------------------------------


class TestLocalCandlepinStoreLoad:
    def test_returns_none_when_dir_missing(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path / 'nonexistent')
        cert, key, uuid = store.load()
        assert cert is None
        assert key is None
        assert uuid is None

    def test_returns_none_when_files_missing(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        cert, key, uuid = store.load()
        assert cert is None
        assert key is None
        assert uuid is None

    def test_returns_cert_key_uuid_when_all_present(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        (tmp_path / 'cert.pem').write_text(SAMPLE_CERT)
        (tmp_path / 'key.pem').write_text(SAMPLE_KEY)
        (tmp_path / 'uuid.txt').write_text(SAMPLE_UUID)

        cert, key, uuid = store.load()
        assert cert == SAMPLE_CERT
        assert key == SAMPLE_KEY
        assert uuid == SAMPLE_UUID

    def test_strips_trailing_whitespace(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        (tmp_path / 'uuid.txt').write_text(f'{SAMPLE_UUID}\n')
        _, _, uuid = store.load()
        assert uuid == SAMPLE_UUID

    def test_returns_none_for_empty_file(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        (tmp_path / 'cert.pem').write_text('')
        cert, _, _ = store.load()
        assert cert is None

    def test_load_never_raises_on_permission_error(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        cert_file = tmp_path / 'cert.pem'
        cert_file.write_text(SAMPLE_CERT)
        cert_file.chmod(0o000)
        try:
            cert, _, _ = store.load()
            assert cert is None
        finally:
            cert_file.chmod(0o644)


class TestLocalCandlepinStoreSaveRegistration:
    def test_creates_dir_and_files(self, tmp_path):
        cert_dir = tmp_path / 'candlepin'
        store = LocalCandlepinStore(cert_dir=cert_dir)
        ok = store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        assert ok is True
        assert (cert_dir / 'cert.pem').read_text() == SAMPLE_CERT
        assert (cert_dir / 'key.pem').read_text() == SAMPLE_KEY
        assert (cert_dir / 'uuid.txt').read_text() == SAMPLE_UUID

    def test_files_have_mode_0600(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        for name in ('cert.pem', 'key.pem', 'uuid.txt'):
            mode = (tmp_path / name).stat().st_mode & 0o777
            assert mode == 0o600, f'{name} should have mode 0600, got {oct(mode)}'

    def test_dir_has_mode_0700(self, tmp_path):
        cert_dir = tmp_path / 'candlepin'
        store = LocalCandlepinStore(cert_dir=cert_dir)
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        mode = cert_dir.stat().st_mode & 0o777
        assert mode == 0o700

    def test_write_is_atomic_tmp_file_is_cleaned_up(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        # .tmp sibling files should not remain after a successful write
        tmp_files = list(tmp_path.glob('*.tmp'))
        assert tmp_files == []

    def test_overwrites_existing_cert(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        new_cert = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
        store.save_registration(new_cert, SAMPLE_KEY, SAMPLE_UUID)
        assert (tmp_path / 'cert.pem').read_text() == new_cert

    def test_roundtrip_load_after_save(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        cert, key, uuid = store.load()
        assert cert == SAMPLE_CERT
        assert key == SAMPLE_KEY
        assert uuid == SAMPLE_UUID


class TestLocalCandlepinStoreSaveCert:
    def test_updates_cert_and_key_only(self, tmp_path):
        store = LocalCandlepinStore(cert_dir=tmp_path)
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        new_cert = '-----BEGIN CERTIFICATE-----\nrenewed==\n-----END CERTIFICATE-----\n'
        new_key = '-----BEGIN RSA PRIVATE KEY-----\nrenewedkey==\n-----END RSA PRIVATE KEY-----\n'
        ok = store.save_cert(new_cert, new_key)
        assert ok is True
        assert (tmp_path / 'cert.pem').read_text() == new_cert
        assert (tmp_path / 'key.pem').read_text() == new_key
        # uuid should be unchanged
        assert (tmp_path / 'uuid.txt').read_text() == SAMPLE_UUID


class TestLocalCandlepinStoreEnvVar:
    def test_uses_env_var_for_cert_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_CERT_DIR', str(tmp_path))
        store = LocalCandlepinStore()
        store.save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        cert, _, _ = store.load()
        assert cert == SAMPLE_CERT


# ---------------------------------------------------------------------------
# DBCandlepinStore
# ---------------------------------------------------------------------------


def _make_db_cursor(rows):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


class TestDBCandlepinStoreLoad:
    def test_returns_cert_key_uuid_when_all_present(self):
        rows = [
            (_DB_CERT_KEY, json.dumps(SAMPLE_CERT)),
            (_DB_KEY_KEY, json.dumps(SAMPLE_KEY)),
            (_DB_UUID_KEY, json.dumps(SAMPLE_UUID)),
        ]
        mock_conn, _ = _make_db_cursor(rows)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, uuid = DBCandlepinStore().load()
        assert cert == SAMPLE_CERT
        assert key == SAMPLE_KEY
        assert uuid == SAMPLE_UUID

    def test_returns_none_none_none_when_no_rows(self):
        mock_conn, _ = _make_db_cursor([])
        with patch('django.db.connection.cursor', return_value=mock_conn):
            cert, key, uuid = DBCandlepinStore().load()
        assert cert is None
        assert key is None
        assert uuid is None

    def test_returns_none_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB down')):
            cert, key, uuid = DBCandlepinStore().load()
        assert cert is None
        assert key is None
        assert uuid is None

    def test_uses_correct_key_names_matching_awx(self):
        assert _DB_CERT_KEY == 'CANDLEPIN_CERT_PEM'
        assert _DB_KEY_KEY == 'CANDLEPIN_KEY_PEM'
        assert _DB_UUID_KEY == 'CANDLEPIN_CONSUMER_UUID'


class TestDBCandlepinStoreSaveRegistration:
    def test_upserts_three_rows(self):
        mock_conn, mock_cursor = _make_db_cursor([])
        mock_tx = MagicMock()
        mock_tx.__enter__ = MagicMock(return_value=None)
        mock_tx.__exit__ = MagicMock(return_value=False)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('django.db.transaction.atomic', return_value=mock_tx):
                ok = DBCandlepinStore().save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        assert ok is True
        assert mock_cursor.execute.call_count == 3

    def test_returns_false_on_db_error(self):
        with patch('django.db.connection.cursor', side_effect=Exception('DB down')):
            ok = DBCandlepinStore().save_registration(SAMPLE_CERT, SAMPLE_KEY, SAMPLE_UUID)
        assert ok is False


class TestDBCandlepinStoreSaveCert:
    def test_upserts_two_rows(self):
        mock_conn, mock_cursor = _make_db_cursor([])
        mock_tx = MagicMock()
        mock_tx.__enter__ = MagicMock(return_value=None)
        mock_tx.__exit__ = MagicMock(return_value=False)
        with patch('django.db.connection.cursor', return_value=mock_conn):
            with patch('django.db.transaction.atomic', return_value=mock_tx):
                ok = DBCandlepinStore().save_cert(SAMPLE_CERT, SAMPLE_KEY)
        assert ok is True
        assert mock_cursor.execute.call_count == 2


# ---------------------------------------------------------------------------
# get_candlepin_store factory
# ---------------------------------------------------------------------------


class TestGetCandlepinStore:
    def test_returns_local_store_by_default(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_STORAGE', raising=False)
        store = get_candlepin_store()
        assert isinstance(store, LocalCandlepinStore)

    def test_returns_local_store_when_set_to_local(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'local')
        store = get_candlepin_store()
        assert isinstance(store, LocalCandlepinStore)

    def test_returns_db_store_when_set_to_db(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'db')
        store = get_candlepin_store()
        assert isinstance(store, DBCandlepinStore)

    def test_returns_local_store_on_unknown_backend(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'badvalue')
        store = get_candlepin_store()
        assert isinstance(store, LocalCandlepinStore)

    def test_case_insensitive(self, monkeypatch):
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'DB')
        store = get_candlepin_store()
        assert isinstance(store, DBCandlepinStore)
