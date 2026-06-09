"""Minimal Candlepin helpers for secure temp PEM file handling during mTLS upload."""

import os
import tempfile

from metrics_utility.logger import logger


class CandlepinClient:
    """Stub client exposing temp-file helpers used by CRC mTLS upload."""

    @staticmethod
    def _write_temp_pem(content):
        """Write PEM content to a secure temp file. Returns the file path."""
        fd, path = tempfile.mkstemp(prefix='candlepin_', suffix='.pem')
        try:
            os.chmod(path, 0o600)
            with os.fdopen(fd, 'w') as f:
                f.write(content)
            fd = None
        except Exception:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if os.path.exists(path):
                os.unlink(path)
            raise
        return path

    @staticmethod
    def _unlink_safe(path):
        try:
            if path and os.path.exists(path):
                os.unlink(path)
        except OSError as e:
            logger.warning(f'Could not remove temp cert file {path}: {e}')

    class _temp_cert_files:
        """Context manager: writes cert + key to secure temp files, cleans up on exit."""

        def __init__(self, cert_pem, key_pem):
            self._cert_pem = cert_pem
            self._key_pem = key_pem
            self._cert_path = None
            self._key_path = None

        def __enter__(self):
            self._cert_path = CandlepinClient._write_temp_pem(self._cert_pem)
            try:
                self._key_path = CandlepinClient._write_temp_pem(self._key_pem)
            except Exception:
                CandlepinClient._unlink_safe(self._cert_path)
                raise
            return self._cert_path, self._key_path

        def __exit__(self, *_):
            CandlepinClient._unlink_safe(self._cert_path)
            CandlepinClient._unlink_safe(self._key_path)
