from datetime import datetime, timedelta, timezone

from metrics_utility.library.instants import (
    days_ago,
    hours_ago,
    iso,
    last_day,
    last_hour,
    last_month,
    last_week,
    minutes_ago,
    months_ago,
    now,
    this_day,
    this_hour,
    this_minute,
    this_month,
    this_week,
    weeks_ago,
)


def test_now_returns_datetime_with_timezone():
    """Test that now() returns a datetime with timezone set."""
    result = now()
    assert isinstance(result, datetime)
    assert result.tzinfo is not None
    assert result.tzinfo == timezone.utc


def test_this_minute_returns_start_of_minute():
    """Test that this_minute() returns the start of the current minute."""
    result = this_minute()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.second == 0
    assert result.microsecond == 0


def test_this_hour_returns_start_of_hour():
    """Test that this_hour() returns the start of the current hour."""
    result = this_hour()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_this_day_returns_start_of_day():
    """Test that this_day() returns the start of the current day (midnight)."""
    result = this_day()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_this_week_returns_start_of_week():
    """Test that this_week() returns the start of the current week (Monday at midnight)."""
    result = this_week()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.weekday() == 0  # Monday
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_this_month_returns_start_of_month():
    """Test that this_month() returns the start of the current month (1st at midnight)."""
    result = this_month()
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.day == 1
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_last_hour_without_relative_to():
    """Test that last_hour() returns the start of the previous hour."""
    result = last_hour()

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 1 hour before this_hour()
    expected = this_hour() - timedelta(hours=1)
    assert result == expected


def test_last_hour_with_relative_to():
    """Test that last_hour() accepts a relative_to parameter."""
    reference_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = last_hour(relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2025, 1, 15, 11, 0, 0, tzinfo=timezone.utc)


def test_last_day_without_relative_to():
    """Test that last_day() returns the start of the previous day."""
    result = last_day()

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 1 day before this_day()
    expected = this_day() - timedelta(days=1)
    assert result == expected


def test_last_day_with_relative_to():
    """Test that last_day() accepts a relative_to parameter."""
    reference_time = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    result = last_day(relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2025, 1, 14, 0, 0, 0, tzinfo=timezone.utc)


def test_last_week_without_relative_to():
    """Test that last_week() returns the start of the previous week."""
    result = last_week()

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.weekday() == 0  # Monday
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 1 week before this_week()
    expected = this_week() - timedelta(weeks=1)
    assert result == expected


def test_last_week_with_relative_to():
    """Test that last_week() accepts a relative_to parameter."""
    # Monday, Jan 13, 2025 at midnight
    reference_time = datetime(2025, 1, 13, 0, 0, 0, tzinfo=timezone.utc)
    result = last_week(relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    # Should be Monday, Jan 6, 2025 at midnight
    assert result == datetime(2025, 1, 6, 0, 0, 0, tzinfo=timezone.utc)
    assert result.weekday() == 0  # Monday


def test_last_month_without_relative_to():
    """Test that last_month() returns the start of the previous month."""
    result = last_month()

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.day == 1
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be the start of the previous month relative to this_month()
    current_month = this_month()
    if current_month.month == 1:
        expected = current_month.replace(year=current_month.year - 1, month=12)
    else:
        expected = current_month.replace(month=current_month.month - 1)
    assert result == expected


def test_last_month_with_relative_to():
    """Test that last_month() accepts a relative_to parameter."""
    # March 1, 2025 at midnight
    reference_time = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    result = last_month(relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    # Should be February 1, 2025 at midnight
    assert result == datetime(2025, 2, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_last_month_year_boundary():
    """Test that last_month() correctly handles year boundaries."""
    # January 1, 2025 at midnight
    reference_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    result = last_month(relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    # Should be December 1, 2024 at midnight
    assert result == datetime(2024, 12, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_hours_ago():
    """Test that hours_ago(n) returns the start of the hour n hours ago."""
    result = hours_ago(3)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 3 hours before this_hour()
    expected = this_hour() - timedelta(hours=3)
    assert result == expected


def test_hours_ago_with_relative_to():
    """Test that hours_ago(n) accepts a relative_to parameter."""
    reference_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = hours_ago(3, relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


def test_days_ago():
    """Test that days_ago(n) returns the start of the day n days ago."""
    result = days_ago(5)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 5 days before this_day()
    expected = this_day() - timedelta(days=5)
    assert result == expected


def test_days_ago_with_relative_to():
    """Test that days_ago(n) accepts a relative_to parameter."""
    reference_time = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    result = days_ago(5, relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2025, 1, 10, 0, 0, 0, tzinfo=timezone.utc)


def test_weeks_ago():
    """Test that weeks_ago(n) returns the start of the week n weeks ago."""
    result = weeks_ago(2)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.weekday() == 0  # Monday
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 2 weeks before this_week()
    expected = this_week() - timedelta(weeks=2)
    assert result == expected


def test_weeks_ago_with_relative_to():
    """Test that weeks_ago(n) accepts a relative_to parameter."""
    # Monday, Jan 20, 2025 at midnight
    reference_time = datetime(2025, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
    result = weeks_ago(2, relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    # Should be Monday, Jan 6, 2025 at midnight
    assert result == datetime(2025, 1, 6, 0, 0, 0, tzinfo=timezone.utc)
    assert result.weekday() == 0  # Monday


def test_months_ago():
    """Test that months_ago(n) returns the start of the month n months ago."""
    result = months_ago(2)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.day == 1
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 2 months before this_month()
    current_month = this_month()
    # Calculate expected month manually
    total_months = current_month.year * 12 + current_month.month - 1
    total_months -= 2
    expected_year = total_months // 12
    expected_month = (total_months % 12) + 1
    expected = current_month.replace(year=expected_year, month=expected_month)
    assert result == expected


def test_months_ago_with_relative_to():
    """Test that months_ago(n) accepts a relative_to parameter."""
    # May 1, 2025 at midnight
    reference_time = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    result = months_ago(2, relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    # Should be exactly March 1, 2025
    assert result == datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_months_ago_year_boundary():
    """Test that months_ago correctly handles year boundaries."""
    # February 1, 2025 at midnight
    reference_time = datetime(2025, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
    result = months_ago(3, relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    # Should be November 1, 2024
    assert result == datetime(2024, 11, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_minutes_ago():
    """Test that minutes_ago(n) returns the start of the minute n minutes ago."""
    result = minutes_ago(10)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result.second == 0
    assert result.microsecond == 0

    # Should be exactly 10 minutes before this_minute()
    expected = this_minute() - timedelta(minutes=10)
    assert result == expected


def test_minutes_ago_with_relative_to():
    """Test that minutes_ago(n) accepts a relative_to parameter."""
    reference_time = datetime(2025, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
    result = minutes_ago(10, relative_to=reference_time)

    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    assert result == datetime(2025, 1, 15, 12, 20, 0, tzinfo=timezone.utc)


def test_iso_converts_datetime_to_string():
    """Test that iso() converts a datetime to ISO 8601 string."""
    dt = datetime(2025, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
    result = iso(dt)

    assert isinstance(result, str)
    assert result == '2025-01-15T12:30:45+00:00'


def test_iso_preserves_timezone():
    """Test that iso() preserves timezone information in the output."""
    dt = datetime(2025, 1, 15, 12, 30, 45, tzinfo=timezone.utc)
    result = iso(dt)

    # The ISO string should contain timezone information
    assert '+00:00' in result or 'Z' in result or result.endswith('+00:00')
