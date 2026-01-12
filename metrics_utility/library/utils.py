import datetime
import os
import tempfile

from contextlib import contextmanager

from .debug import log


@contextmanager
def tempdir(prefix=None, cleanup=True):
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
    log('library.utils last_gather')
    return None


def save_last_gather(db=None, key=None, value=None):
    log('library.utils save_last_gather')
