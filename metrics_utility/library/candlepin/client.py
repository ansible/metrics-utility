"""Minimal Candlepin helpers for secure temp PEM file handling during mTLS upload."""

import os
import tempfile

from contextlib import contextmanager

from metrics_utility.logger import logger


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


def _unlink_safe(path):
    try:
        if path and os.path.exists(path):
            os.unlink(path)
    except OSError as e:
        logger.warning(f'Could not remove temp cert file {path}: {e}')


@contextmanager
def temp_cert_files(cert_pem, key_pem):
    """Context manager: writes cert + key to secure temp files, cleans up on exit."""
    cert_path = _write_temp_pem(cert_pem)
    try:
        key_path = _write_temp_pem(key_pem)
    except Exception:
        _unlink_safe(cert_path)
        raise
    try:
        yield cert_path, key_path
    finally:
        _unlink_safe(cert_path)
        _unlink_safe(key_path)
