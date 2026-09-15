from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.awx.config import (
    _as_datetime,
    _decode,
    _get_install_type,
    _get_settings,
    config,
)
from metrics_utility.library.collectors.awx.events_table import (
    _window as events_window,
)
from metrics_utility.library.collectors.awx.events_table import (
    events_table,
)
from metrics_utility.library.collectors.awx.host_metric_table import (
    _window as host_metric_window,
)
from metrics_utility.library.collectors.awx.host_metric_table import (
    host_metric_table,
)
from metrics_utility.library.collectors.awx.unified_jobs_table import (
    _window as unified_jobs_window,
)
from metrics_utility.library.collectors.awx.unified_jobs_table import (
    unified_jobs_table,
)


SINCE = datetime(2024, 1, 1, tzinfo=UTC)
UNTIL = datetime(2024, 2, 1, tzinfo=UTC)


def _mock_db():
    db = MagicMock()
    cursor = MagicMock()
    db.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return db


def test_config_is_snapshot_only():
    db = _mock_db()
    cursor = db.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        ('INSTALL_UUID', '"install"'),
        ('SYSTEM_UUID', '"instance"'),
        ('LICENSE', '{"instance_count": 3}'),
    ]
    cursor.fetchone.return_value = ('4.5.0',)

    result = config(db=db).gather()

    assert result['install_uuid'] == 'install'
    assert result['instance_uuid'] == 'instance'
    assert result['tower_version'] == '4.5.0'
    assert result['total_licensed_instances'] == 3


def test_config_handles_non_mapping_license():
    db = _mock_db()
    cursor = db.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [('LICENSE', 'null')]
    cursor.fetchone.return_value = None

    result = config(db=db).gather()

    assert result['license_type'] == 'UNLICENSED'


@pytest.mark.parametrize(
    ('environment', 'expected'),
    [
        ({'container': 'oci'}, 'openshift'),
        ({'KUBERNETES_SERVICE_PORT': '443'}, 'k8s'),
        ({}, 'traditional'),
    ],
)
def test_get_install_type(environment, expected, monkeypatch):
    monkeypatch.delenv('container', raising=False)
    monkeypatch.delenv('KUBERNETES_SERVICE_PORT', raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    assert _get_install_type() == expected


def test_get_settings_none():
    assert _get_settings(None) == {}


def test_get_settings_skips_falsey_values():
    db = _mock_db()
    cursor = db.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        ('INSTALL_UUID', ''),
        ('SYSTEM_UUID', None),
        ('LICENSE', 0),
        ('TOWER_URL', False),
    ]

    assert _get_settings(db) == {}


@pytest.mark.parametrize('value', [None, 1, {}, []])
def test_decode_returns_non_string_values(value):
    assert _decode(value) is value


def test_decode_returns_malformed_json_unchanged():
    value = '{malformed json'

    assert _decode(value) == value


def test_as_datetime_parses_valid_value():
    value = '2024-01-01T00:00:00+00:00'

    assert _as_datetime(value) == datetime(2024, 1, 1, tzinfo=UTC)


def test_as_datetime_returns_invalid_value_unchanged():
    value = 'not a datetime'

    assert _as_datetime(value) == value


@pytest.mark.parametrize('value', [None, 1, {}, []])
def test_as_datetime_returns_non_string_values(value):
    assert _as_datetime(value) is value


@pytest.mark.parametrize(
    'window',
    [events_window, unified_jobs_window, host_metric_window],
)
@pytest.mark.parametrize(
    ('since', 'until', 'error'),
    [
        ('not a datetime', UNTIL, TypeError),
        (SINCE, 'not a datetime', TypeError),
        (datetime(2024, 1, 1), UNTIL, ValueError),
        (SINCE, datetime(2024, 2, 1), ValueError),
        (None, UNTIL, ValueError),
        (SINCE, None, ValueError),
    ],
)
def test_awx_windows_validate_bounds(window, since, until, error):
    with pytest.raises(error):
        window(since, until)


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_events_table_uses_direct_modified_exclusive_inclusive_window(copy_table):
    copy_table.return_value = pd.DataFrame()

    events_table(db=_mock_db(), since=SINCE, until=UNTIL).gather()

    query = copy_table.call_args.args[1]
    assert 'FROM main_jobevent' in query
    assert 'main_jobevent.modified >' in query
    assert 'main_jobevent.modified <=' in query
    assert 'AS event_data' not in query
    assert 'playbook_on_stats' in query
    assert 'warnings' in query
    assert 'deprecations' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_unified_jobs_table_uses_created_or_finished_window(copy_table):
    copy_table.return_value = pd.DataFrame()

    unified_jobs_table(db=_mock_db(), since=SINCE, until=UNTIL).gather()

    query = copy_table.call_args.args[1]
    assert '(main_unifiedjob.created >' in query
    assert '(main_unifiedjob.finished >' in query
    assert "main_unifiedjob.launch_type != 'sync'" in query
    assert 'main_unifiedjob.created <=' in query
    assert 'main_unifiedjob.finished <=' in query


@patch('metrics_utility.library.collectors.util._copy_table_pandas')
def test_host_metric_table_uses_automation_or_deleted_window(copy_table):
    copy_table.return_value = pd.DataFrame()

    host_metric_table(db=_mock_db(), since=SINCE, until=UNTIL).gather()

    query = copy_table.call_args.args[1]
    assert 'main_hostmetric.last_automation >' in query
    assert 'main_hostmetric.last_automation <=' in query
    assert 'main_hostmetric.last_deleted >' in query
    assert 'main_hostmetric.last_deleted <=' in query
