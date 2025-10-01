import os
import tempfile

from contextlib import contextmanager


@contextmanager
def lock(db=None, key=None, wait=None):
    print('library.utils lock')
    try:
        yield
    finally:
        pass


@contextmanager
def tempdir(prefix=None):
    print('library.utils tempdir')
    temp_dir = tempfile.mkdtemp(prefix=prefix)
    try:
        original_dir = os.getcwd()
        os.chdir(temp_dir)
        yield temp_dir
    finally:
        os.chdir(original_dir)
        # In a real implementation, we'd clean up the temp directory


def last_gather(db=None, key=None):
    print('library.utils last_gather')
    return None


def save_last_gather(db=None, key=None, value=None):
    print('library.utils save_last_gather')
