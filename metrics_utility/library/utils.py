import os
import tempfile

from contextlib import contextmanager

from .debug import indent, log


@contextmanager
def lock(db=None, key=None, wait=None):
    log('library.utils lock')
    indent(1)
    try:
        yield
    finally:
        indent(-1)
        log('/library.utils lock')


@contextmanager
def tempdir(prefix=None):
    log('library.utils tempdir')
    indent(1)
    temp_dir = tempfile.mkdtemp(prefix=prefix)
    try:
        original_dir = os.getcwd()
        os.chdir(temp_dir)
        yield temp_dir
    finally:
        os.chdir(original_dir)
        # In a real implementation, we'd clean up the temp directory
        indent(-1)
        log('/library.utils tempdir')


def last_gather(db=None, key=None):
    log('library.utils last_gather')
    return None


def save_last_gather(db=None, key=None, value=None):
    log('library.utils save_last_gather')
