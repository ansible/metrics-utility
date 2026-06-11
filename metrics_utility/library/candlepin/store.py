"""Candlepin certificate storage abstraction.

Provides two backends:
  LocalCandlepinStore  — reads/writes cert.pem, key.pem, uuid.txt on the local filesystem.
                         Default; works without any database connection.
  DBCandlepinStore     — reads/writes via the AWX conf_setting table (PostgreSQL).
                         Available when METRICS_UTILITY_CANDLEPIN_STORAGE=db.

Select the backend via the METRICS_UTILITY_CANDLEPIN_STORAGE env var (default: 'local').
"""

import json
import os
import tempfile

from abc import ABC, abstractmethod
from pathlib import Path

from metrics_utility.logger import logger


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class CandlepinStore(ABC):
    """Storage interface for the Candlepin consumer identity cert/key/UUID."""

    @abstractmethod
    def load(self):
        """Return (cert_pem, key_pem, consumer_uuid) — any may be None.

        Never raises; failures are logged as warnings.
        """

    @abstractmethod
    def save_registration(self, cert_pem, key_pem, consumer_uuid):
        """Persist the full registration result (cert, key, UUID).

        Returns True on success, False on failure.
        """

    @abstractmethod
    def save_cert(self, cert_pem, key_pem):
        """Persist an updated cert/key pair after renewal (UUID unchanged).

        Returns True on success, False on failure.
        """


# ---------------------------------------------------------------------------
# Local filesystem backend
# ---------------------------------------------------------------------------

_DEFAULT_CERT_DIR = '/var/lib/awx/candlepin-certs'


class LocalCandlepinStore(CandlepinStore):
    """Stores cert.pem, key.pem, and uuid.txt in a configurable directory.

    The directory defaults to /var/lib/awx/candlepin-certs/ and can be
    overridden with METRICS_UTILITY_CANDLEPIN_CERT_DIR.  Files are created
    with mode 0o600; the directory itself is created with mode 0o700.
    All writes are atomic: content is written to a .tmp sibling then
    renamed into place so partial writes never corrupt the live file.
    """

    def __init__(self, cert_dir=None):
        self.cert_dir = Path(cert_dir or os.getenv('METRICS_UTILITY_CANDLEPIN_CERT_DIR', _DEFAULT_CERT_DIR))

    # -- paths ----------------------------------------------------------------

    @property
    def _cert_path(self):
        return self.cert_dir / 'cert.pem'

    @property
    def _key_path(self):
        return self.cert_dir / 'key.pem'

    @property
    def _uuid_path(self):
        return self.cert_dir / 'uuid.txt'

    # -- helpers --------------------------------------------------------------

    def _read_file(self, path, strip=False):
        """Return file content as a string, or None if the file is absent or empty.

        PEM files (cert.pem, key.pem) are returned verbatim so trailing newlines
        are preserved.  uuid.txt is read with strip=True to remove editor-appended
        newlines from the UUID value.
        """
        try:
            content = path.read_text(encoding='utf-8')
            if strip:
                content = content.strip()
            return content if content.strip() else None
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.warning(f'Candlepin local store: could not read {path}: {e}')
            return None

    def _write_file(self, path, content):
        """Write content to path atomically with mode 0o600.

        Creates the parent directory (mode 0o700) if it does not exist.
        Returns True on success, False on failure.
        """
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            tmp = Path(str(path) + '.tmp')
            tmp.write_text(content, encoding='utf-8')
            tmp.chmod(0o600)
            tmp.replace(path)  # atomic on POSIX
            return True
        except Exception as e:
            logger.error(f'Candlepin local store: could not write {path}: {e}')
            return False

    # -- access probe ---------------------------------------------------------

    def is_writable(self):
        """Return True if the cert directory can be created and written to.

        Attempts a real probe write rather than os.access() to avoid false
        positives from ACLs or setuid environments.
        """
        try:
            self.cert_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=self.cert_dir, prefix='.probe-', delete=True):
                pass
            return True
        except Exception as e:
            logger.debug(f'Candlepin local store: write probe failed for {self.cert_dir}: {e}')
            return False

    # -- interface ------------------------------------------------------------

    def load(self):
        cert_pem = self._read_file(self._cert_path)
        key_pem = self._read_file(self._key_path)
        consumer_uuid = self._read_file(self._uuid_path, strip=True)
        return cert_pem, key_pem, consumer_uuid

    def save_registration(self, cert_pem, key_pem, consumer_uuid):
        ok = True
        ok = self._write_file(self._cert_path, cert_pem) and ok
        ok = self._write_file(self._key_path, key_pem) and ok
        ok = self._write_file(self._uuid_path, consumer_uuid) and ok
        if ok:
            logger.info(f'Candlepin registration saved to {self.cert_dir} (uuid={consumer_uuid}).')
        return ok

    def save_cert(self, cert_pem, key_pem):
        ok = self._write_file(self._cert_path, cert_pem)
        ok = self._write_file(self._key_path, key_pem) and ok
        if ok:
            logger.info(f'Renewed Candlepin cert and key saved to {self.cert_dir}.')
        return ok


# ---------------------------------------------------------------------------
# Database backend (conf_setting table)
# ---------------------------------------------------------------------------

# Key names match what AWX PR #16388 writes to conf_setting.
_DB_CERT_KEY = 'CANDLEPIN_CERT_PEM'
_DB_KEY_KEY = 'CANDLEPIN_KEY_PEM'
_DB_UUID_KEY = 'CANDLEPIN_CONSUMER_UUID'


class DBCandlepinStore(CandlepinStore):
    """Read-only view of the Candlepin cert/key/UUID from the AWX conf_setting table.

    Used to **seed** the local store: metrics-utility reads the cert that the AWX
    Controller registered with Candlepin and caches it locally for use during uploads.

    Key names match what AWX PR #16388 (merged) writes to conf_setting:
        CANDLEPIN_CERT_PEM, CANDLEPIN_KEY_PEM, CANDLEPIN_CONSUMER_UUID.

    Write methods (save_registration, save_cert) are intentionally not implemented.
    Writing Candlepin material back to a DB is future scope and will target a
    metrics-utility-specific database — not the AWX conf_setting table.
    """

    def load(self):
        all_keys = [_DB_CERT_KEY, _DB_KEY_KEY, _DB_UUID_KEY]
        try:
            from django.db import connection

            placeholders = ', '.join(['%s'] * len(all_keys))
            query = 'SELECT key, value FROM conf_setting WHERE key IN (' + placeholders + ')'
            with connection.cursor() as cursor:
                cursor.execute(query, all_keys)
                rows = {key: json.loads(value) for key, value in cursor.fetchall() if value}
            return (
                rows.get(_DB_CERT_KEY),
                rows.get(_DB_KEY_KEY),
                rows.get(_DB_UUID_KEY),
            )
        except Exception as e:
            logger.warning(f'Candlepin DB store: could not fetch from conf_setting: {e}')
            return None, None, None

    def save_registration(self, cert_pem, key_pem, consumer_uuid):
        # Writing to a metrics-utility-specific DB is future scope (not yet implemented).
        logger.warning(
            'DBCandlepinStore.save_registration is not implemented. '
            'Use METRICS_UTILITY_CANDLEPIN_STORAGE=local to persist the cert on the filesystem.'
        )
        return False

    def save_cert(self, cert_pem, key_pem):
        # Writing to a metrics-utility-specific DB is future scope (not yet implemented).
        logger.warning(
            'DBCandlepinStore.save_cert is not implemented. Use METRICS_UTILITY_CANDLEPIN_STORAGE=local to persist the cert on the filesystem.'
        )
        return False


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_candlepin_store():
    """Return the configured CandlepinStore backend.

    Reads METRICS_UTILITY_CANDLEPIN_STORAGE (default: 'local').
    'local' → LocalCandlepinStore  (reads/writes local filesystem)
    'db'    → DBCandlepinStore     (reads/writes AWX conf_setting table)
    """
    backend = os.getenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'local').strip().lower()
    if backend == 'db':
        return DBCandlepinStore()
    if backend != 'local':
        logger.warning(f'Unknown METRICS_UTILITY_CANDLEPIN_STORAGE={backend!r}; falling back to local storage.')
    return LocalCandlepinStore()
