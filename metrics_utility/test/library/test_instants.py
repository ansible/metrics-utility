from datetime import datetime, timedelta, timezone

from metrics_utility.library.instants import (
    days_ago,
    minutes_ago,
    months_ago,
    now,
    this_day,
    this_minute,
    this_month,
)
from metrics_utility.test.util import utcdt


def test_now_returns_datetime_with_timezone():
    result = now()
    assert isinstance(result, datetime)
    assert result.tzinfo is not None
    assert result.tzinfo == timezone.utc


def test_this_minute_returns_start_of_minute():
    result = this_minute()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.second == 0
    assert result.microsecond == 0


def test_this_day_returns_start_of_day():
    result = this_day()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_this_month_returns_start_of_month():
    result = this_month()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.day == 1
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_days_ago():
    result = days_ago(5)
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.hour == 0
    expected = this_day() - timedelta(days=5)
    assert result == expected


def test_days_ago_with_relative_to():
    result = days_ago(5, relative_to=utcdt('2025-01-15'))
    assert result == utcdt('2025-01-10')


def test_months_ago():
    result = months_ago(2)
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.day == 1
    current_month = this_month()
    total_months = current_month.year * 12 + current_month.month - 1
    total_months -= 2
    expected_year = total_months // 12
    expected_month = (total_months % 12) + 1
    expected = current_month.replace(year=expected_year, month=expected_month)
    assert result == expected


def test_months_ago_with_relative_to():
    result = months_ago(2, relative_to=utcdt('2025-05-01'))
    assert result == utcdt('2025-03-01')


def test_months_ago_year_boundary():
    result = months_ago(3, relative_to=utcdt('2025-02-01'))
    assert result == utcdt('2024-11-01')


def test_minutes_ago():
    result = minutes_ago(10)
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.second == 0
    expected = this_minute() - timedelta(minutes=10)
    assert result == expected


def test_minutes_ago_with_relative_to():
    result = minutes_ago(10, relative_to=utcdt('2025-01-15T12:30:00'))
    assert result == utcdt('2025-01-15T12:20:00')
