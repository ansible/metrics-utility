import copy
import os
import shutil

from unittest.mock import patch

import pytest

from metrics_utility.gather.collection import Collection
from metrics_utility.gather.decorators import register
from metrics_utility.test.gather.support.analytics_collector import AnalyticsCollector
from metrics_utility.test.util import utcdt


@pytest.fixture
def collector():
    c = AnalyticsCollector(collection_type=AnalyticsCollector.DRY_RUN)
    c._gather_initialize(None, utcdt('2024-01-01'), utcdt('2024-01-02'))
    yield c
    if c.tmp_dir:
        shutil.rmtree(c.tmp_dir, ignore_errors=True)


def _make_fn(key, output_format='json', slicing=None):
    @register(key, '1.0', output_format=output_format, slicing=slicing)
    def fn(**kwargs):
        return None

    return fn


# --- cleanup with sub_collections ---


def test_cleanup_csv_no_filepath(collector):
    fn = _make_fn('test_csv', output_format='csv')
    collection = Collection(collector, fn)
    collection.cleanup()


def test_data_size_csv_file_missing(collector, tmp_path):
    fn = _make_fn('test_csv', output_format='csv')
    collection = Collection(collector, fn)
    collection.data_filepath = str(tmp_path / 'nonexistent.csv')
    assert collection.data_size() == 0


def test_cleanup_sub_collections(collector, tmp_path):
    fn = _make_fn('test_csv', output_format='csv')
    collection = Collection(collector, fn)

    sub = copy.copy(collection)
    sub.sub_collections = []
    sub.data_filepath = str(tmp_path / 'sub.csv')
    with open(sub.data_filepath, 'w') as f:
        f.write('header\ndata\n')

    collection.sub_collections = [sub]
    collection.data_filepath = str(tmp_path / 'main.csv')
    with open(collection.data_filepath, 'w') as f:
        f.write('header\ndata\n')

    collection.cleanup()

    assert not os.path.exists(collection.data_filepath)
    assert not os.path.exists(sub.data_filepath)


# --- data_size edge cases ---


def test_data_size_csv_no_filepath(collector):
    fn = _make_fn('test_csv', output_format='csv')
    collection = Collection(collector, fn)
    assert collection.data_size() == 0


def test_data_size_csv_oserror(collector, tmp_path):
    fn = _make_fn('test_csv', output_format='csv')
    collection = Collection(collector, fn)
    collection.data_filepath = str(tmp_path / 'missing.csv')

    with patch('os.path.exists', return_value=True):
        with patch('os.path.getsize', side_effect=OSError('permission denied')):
            assert collection.data_size() == 0


# --- gather exception ---


def test_gather_exception(collector):
    @register('boom', '1.0')
    def boom_fn(**kwargs):
        raise RuntimeError('boom')

    collection = Collection(collector, boom_fn)
    collection.gather()

    assert collection.gathering_successful is False
    assert collection.disabled is False


# --- update_last_gathered_entries ---


def test_update_last_gathered_disabled(collector):
    fn = _make_fn('disabled_key')
    collection = Collection(collector, fn)
    collection.disabled = True

    updates = {'keys': {}, 'locked': set()}
    collection.update_last_gathered_entries(updates)

    assert 'disabled_key' not in updates['keys']


def test_update_last_gathered_with_sub_collections(collector):
    fn = _make_fn('parent', output_format='csv')
    collection = Collection(collector, fn)

    sub1 = copy.copy(collection)
    sub1.sub_collections = []
    sub1.key = 'parent'
    sub1.gathering_successful = True
    sub1.until = utcdt('2024-01-02')
    sub1.disabled = False

    collection.sub_collections = [sub1]

    updates = {'keys': {}, 'locked': set()}
    collection.update_last_gathered_entries(updates)

    assert 'parent' in updates['keys']


def test_update_last_gathered_key_locked(collector):
    fn = _make_fn('locked_key')
    collection = Collection(collector, fn)
    collection.gathering_successful = True
    collection.until = utcdt('2024-01-02')

    updates = {'keys': {}, 'locked': {'locked_key'}}
    collection.update_last_gathered_entries(updates)

    assert 'locked_key' not in updates['keys']


def test_update_last_gathered_unsuccessful(collector):
    fn = _make_fn('fail_key')
    collection = Collection(collector, fn)
    collection.gathering_successful = False

    updates = {'keys': {}, 'locked': set()}
    collection.update_last_gathered_entries(updates)

    assert 'fail_key' in updates['locked']


# --- _save_gathering CSV as string ---


def test_save_gathering_csv_string(collector, tmp_path):
    @register('str_csv', '1.0', output_format='csv')
    def str_csv_fn(*, output, **kwargs):
        filepath = str(tmp_path / 'single.csv')
        with open(filepath, 'w') as f:
            f.write('header\ndata\n')
        return filepath

    collection = Collection(collector, str_csv_fn)
    collection.gather()

    assert collection.data_filepath == str(tmp_path / 'single.csv')
    assert collection.gathering_successful is True
