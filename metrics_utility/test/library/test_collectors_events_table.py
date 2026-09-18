from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.library.collectors.controller.events_table import (
    _window as events_window,
)
from metrics_utility.library.collectors.controller.events_table import (
    events_table,
)


SINCE = datetime(2024, 1, 1, tzinfo=UTC)
UNTIL = datetime(2024, 2, 1, tzinfo=UTC)


def _mock_db():
    db = MagicMock()
    cursor = MagicMock()
    db.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return db


@pytest.mark.parametrize(
    'window',
    [events_window],
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
def test_events_windows_validate_bounds(window, since, until, error):
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
