import shutil

from contextlib import contextmanager
from unittest.mock import patch

import pytest

from metrics_utility.gather.decorators import register
from metrics_utility.gather.slicing import daily_slicing
from metrics_utility.test.gather.support.analytics_collector import AnalyticsCollector
from metrics_utility.test.util import temporary_env, utcdt


@pytest.fixture
def collector():
    c = AnalyticsCollector(collection_type=AnalyticsCollector.DRY_RUN)
    yield c
    if c.tmp_dir:
        shutil.rmtree(c.tmp_dir, ignore_errors=True)


# --- lock suffix ---


def test_lock_suffix_env(collector):
    with temporary_env({'METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX': 'vcpu'}):
        tgz_files = collector.gather(subset=['config'], since=utcdt('2024-01-01'), until=utcdt('2024-01-02'))
    assert tgz_files is not None


# --- lock not acquired ---


def test_lock_not_acquired(collector):
    @contextmanager
    def fake_lock(*args, **kwargs):
        yield False

    with patch('metrics_utility.gather.collector.lock', fake_lock):
        result = collector.gather(subset=['config'], since=utcdt('2024-01-01'), until=utcdt('2024-01-02'))
    assert result is None


# --- _calculate_collection_interval edge cases ---


def test_until_in_future(collector):
    future = utcdt('2099-01-01')
    collector.gather(subset=['config'], since=utcdt('2024-01-01'), until=future)
    assert collector.gather_until < future


def test_since_in_future(collector):
    future = utcdt('2099-06-01')
    collector.gather(subset=['config'], since=future, until=utcdt('2099-07-01'))
    assert collector.gather_since < future


def test_since_until_too_far_apart(collector):
    collector.gather(subset=['config'], since=utcdt('2024-01-01'), until=utcdt('2024-06-01'))
    # until should be clamped to since + max days
    assert (collector.gather_until - collector.gather_since).days <= 28


def test_since_without_until(collector):
    collector.gather(subset=['config'], since=utcdt('2024-01-01'))
    assert collector.gather_until is not None


def test_since_after_until_raises(collector):
    with pytest.raises(ValueError):
        collector.gather(subset=['config'], since=utcdt('2024-06-01'), until=utcdt('2024-01-01'))


# --- _gather_finalize with DISABLE_SAVE_LAST_GATHERED_ENTRIES ---


def test_gather_finalize_disabled(ship_path):
    """Test that DISABLE_SAVE_LAST_GATHERED_ENTRIES skips finalize."""
    from metrics_utility.gather.collector import Collector

    env = {'METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES': 'true'}
    with temporary_env(env):
        c = Collector(collection_type=Collector.MANUAL_COLLECTION)
        c.ship = True
        c._gather_finalize()
    # should return early without error


# --- _update_last_gathered_entries with locked keys ---


def test_update_last_gathered_entries_locked_key():
    from unittest.mock import MagicMock

    from metrics_utility.gather.collector import Collector

    c = Collector(collection_type=Collector.DRY_RUN)
    c.last_gathered_entries = {}

    mock_package = MagicMock()

    def set_locked(updates_dict):
        updates_dict['locked'].add('failing_collector')
        # add a _full key at top level (what the pop targets)
        updates_dict['failing_collector_full'] = utcdt('2024-01-02')

    mock_package.update_last_gathered_entries.side_effect = set_locked

    c.packages = {'default': [mock_package]}

    c._update_last_gathered_entries()

    # the locked set should have been populated
    assert 'failing_collector' not in c.last_gathered_entries


# --- cli_config fallback ---


def test_cli_config_fallback_to_db_config():
    from unittest.mock import MagicMock

    from metrics_utility.gather.collector import cli_config
    from metrics_utility.library.collectors.util import CollectionOutput

    output = CollectionOutput('/tmp')

    mock_collector = MagicMock()
    mock_collector.gather.return_value = {'version': '2.0'}

    with patch('metrics_utility.gather.collector.config_django', side_effect=Exception('no django config')):
        with patch('metrics_utility.gather.collector.config', return_value=mock_collector):
            result = cli_config(output=output)

    assert result == {'version': '2.0'}


# --- failed / empty CSV collections ---


@register('config', '1.0')
def _config(**kwargs):
    return {'version': '1.0'}


@register('failing_csv', '1.0', output_format='csv', slicing=daily_slicing)
def _failing_csv(**kwargs):
    raise RuntimeError('boom')


@register('empty_csv', '1.0', output_format='csv', slicing=daily_slicing)
def _empty_csv(*, output, **kwargs):
    return None


def _make_module(*fns):
    """Create a fake module with the given registered functions."""
    import types

    mod = types.ModuleType('fake_collectors')
    for fn in fns:
        setattr(mod, fn.__name__, fn)
    return mod


def test_csv_collection_failure(collector):
    collector.collector_module = _make_module(_config, _failing_csv)
    tgz_files = collector.gather(since=utcdt('2024-01-01'), until=utcdt('2024-01-03'))
    assert tgz_files is not None


def test_csv_collection_empty(collector):
    collector.collector_module = _make_module(_config, _empty_csv)
    tgz_files = collector.gather(since=utcdt('2024-01-01'), until=utcdt('2024-01-03'))
    assert tgz_files is not None
