from unittest.mock import patch

import pytest

from django.db import DatabaseError

from metrics_utility.gather.utils import bool_from_env, get_last_entries_from_db, get_max_gather_period_days, get_optional_collectors
from metrics_utility.test.util import mock_cursor_db, utcdt


# bool_from_env


def test_bool_from_env_true_for_1():
    with patch.dict('os.environ', {'TEST_VAR': '1'}):
        assert bool_from_env('TEST_VAR') is True


def test_bool_from_env_true_for_true_lowercase():
    with patch.dict('os.environ', {'TEST_VAR': 'true'}):
        assert bool_from_env('TEST_VAR') is True


def test_bool_from_env_true_for_true_uppercase():
    with patch.dict('os.environ', {'TEST_VAR': 'TRUE'}):
        assert bool_from_env('TEST_VAR') is True


def test_bool_from_env_true_for_true_mixedcase():
    with patch.dict('os.environ', {'TEST_VAR': 'TrUe'}):
        assert bool_from_env('TEST_VAR') is True


def test_bool_from_env_false_for_0():
    with patch.dict('os.environ', {'TEST_VAR': '0'}):
        assert bool_from_env('TEST_VAR') is False


def test_bool_from_env_false_for_false_lowercase():
    with patch.dict('os.environ', {'TEST_VAR': 'false'}):
        assert bool_from_env('TEST_VAR') is False


def test_bool_from_env_false_for_false_uppercase():
    with patch.dict('os.environ', {'TEST_VAR': 'FALSE'}):
        assert bool_from_env('TEST_VAR') is False


def test_bool_from_env_false_for_arbitrary_string():
    with patch.dict('os.environ', {'TEST_VAR': 'random'}):
        assert bool_from_env('TEST_VAR') is False


def test_bool_from_env_false_for_empty_string():
    with patch.dict('os.environ', {'TEST_VAR': ''}):
        assert bool_from_env('TEST_VAR') is False


def test_bool_from_env_none_when_not_set():
    with patch.dict('os.environ', {}, clear=True):
        assert bool_from_env('MISSING_VAR') is None


def test_bool_from_env_default_when_not_set():
    with patch.dict('os.environ', {}, clear=True):
        assert bool_from_env('MISSING_VAR', default=False) is False


def test_bool_from_env_custom_default_when_not_set():
    with patch.dict('os.environ', {}, clear=True):
        assert bool_from_env('MISSING_VAR', default=True) is True


def test_bool_from_env_none_default_explicitly():
    with patch.dict('os.environ', {}, clear=True):
        assert bool_from_env('MISSING_VAR', default=None) is None


# get_max_gather_period_days


def test_max_gather_period_days_default_28():
    with patch.dict('os.environ', {}, clear=True):
        assert get_max_gather_period_days() == 28


def test_max_gather_period_days_from_env():
    with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '14'}):
        assert get_max_gather_period_days() == 14


def test_max_gather_period_days_1():
    with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '1'}):
        assert get_max_gather_period_days() == 1


def test_max_gather_period_days_raises_on_non_integer():
    with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': 'not_a_number'}):
        with pytest.raises(ValueError):
            get_max_gather_period_days()


# get_optional_collectors


def test_optional_collectors_default_main_jobevent():
    with patch.dict('os.environ', {}, clear=True):
        assert get_optional_collectors() == ['main_jobevent']


def test_optional_collectors_single_value():
    with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host'}):
        assert get_optional_collectors() == ['main_host']


def test_optional_collectors_comma_separated():
    with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,main_jobevent'}):
        assert get_optional_collectors() == ['main_host', 'main_jobevent']


def test_optional_collectors_strips_whitespace():
    with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': ' main_host,main_jobevent '}):
        result = get_optional_collectors()
        assert 'main_host' in result
        assert 'main_jobevent' in result


def test_optional_collectors_empty_string():
    with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': ''}):
        assert get_optional_collectors() == []


def test_optional_collectors_filters_empty_segments():
    with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,'}):
        result = get_optional_collectors()
        assert '' not in result
        assert 'main_host' in result


# get_last_entries_from_db


def test_last_entries_from_db_success():
    mock_connection, mock_cursor = mock_cursor_db()
    test_json = '"{\\"config\\": \\"2024-01-01T00:00:00Z\\", \\"hosts\\": \\"2024-01-03T00:00:00Z\\", \\"jobs\\": \\"2024-01-02T00:00:00Z\\"}"'
    mock_cursor.fetchone.return_value = (test_json,)

    with patch('metrics_utility.gather.utils.connection', mock_connection):
        result = get_last_entries_from_db()

    assert result == {
        'config': utcdt('2024-01-01'),
        'hosts': utcdt('2024-01-03'),
        'jobs': utcdt('2024-01-02'),
    }
    mock_cursor.execute.assert_called_once()
    assert 'AUTOMATION_ANALYTICS_LAST_ENTRIES' in mock_cursor.execute.call_args[0][0]


def test_last_entries_from_db_no_entries():
    mock_connection, mock_cursor = mock_cursor_db()
    mock_cursor.fetchone.return_value = None

    with patch('metrics_utility.gather.utils.connection', mock_connection):
        assert get_last_entries_from_db() == {}


@patch('metrics_utility.gather.utils.logger')
@patch('metrics_utility.gather.utils.connection')
def test_last_entries_from_db_database_error(mock_connection, mock_logger):
    mock_connection.cursor.side_effect = DatabaseError('Query failed')

    assert get_last_entries_from_db() == {}
    mock_logger.error.assert_called_once()
    assert 'Error getting AUTOMATION_ANALYTICS_LAST_ENTRIES from database' in str(mock_logger.error.call_args)
