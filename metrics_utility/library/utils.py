"""Library-level utility functions (temp directory, last-gather stubs)."""

import datetime
import os
import tempfile

from contextlib import contextmanager

from .debug import log


@contextmanager
def tempdir(prefix=None, cleanup=True):
    """Create a temporary directory, change into it, and clean up on exit.

    The directory name is prefixed with a UTC timestamp so that directories
    created by concurrent processes or tests remain distinct.

    Args:
        prefix: Optional string prepended to the generated directory name.
        cleanup: When True (default), the directory is deleted on exit.

    Yields:
        Absolute path to the temporary directory.
    """
    # Generate timestamp using the codebase convention: '%Y-%m-%d-%H%M%S%z'
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d-%H%M%S%z')

    # Combine prefix with timestamp
    if prefix:
        dir_prefix = f'{prefix}-{timestamp}-'
    else:
        dir_prefix = f'{timestamp}-'

    with tempfile.TemporaryDirectory(prefix=dir_prefix, delete=cleanup) as temp_dir:
        original_dir = os.getcwd()
        os.chdir(temp_dir)
        try:
            yield temp_dir
        finally:
            os.chdir(original_dir)


def last_gather(db=None, key=None):
    """Return the last-gather timestamp for *key* (stub — always returns None).

    Args:
        db: Unused database connection parameter.
        key: Unused collector key parameter.

    Returns:
        None (not yet implemented).
    """
    log('library.utils last_gather')
    return None


def save_last_gather(db=None, key=None, value=None):
    """Persist the last-gather timestamp for *key* (stub — no-op).

    Args:
        db: Unused database connection parameter.
        key: Unused collector key parameter.
        value: Unused timestamp value parameter.
    """
    log('library.utils save_last_gather')
