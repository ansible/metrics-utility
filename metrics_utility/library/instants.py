from datetime import datetime, timedelta, timezone


def now():
    return datetime.now(tz=timezone.utc)


def this_minute():
    """Return the start of the current minute."""
    return now().replace(second=0, microsecond=0)


def this_hour():
    """Return the start of the current hour."""
    return now().replace(minute=0, second=0, microsecond=0)


def this_day():
    """Return the start of the current day (midnight)."""
    return now().replace(hour=0, minute=0, second=0, microsecond=0)


def this_week():
    """Return the start of the current week (Monday at midnight)."""
    current = this_day()
    # weekday() returns 0 for Monday, 6 for Sunday
    days_since_monday = current.weekday()
    return current - timedelta(days=days_since_monday)


def this_month():
    """Return the start of the current month (1st day at midnight)."""
    return now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def last_hour(relative_to=None):
    """Return the start of the hour before relative_to (or current hour if not specified)."""
    return (relative_to or this_hour()) - timedelta(hours=1)


def last_day(relative_to=None):
    """Return the start of the day before relative_to (or current day if not specified)."""
    return (relative_to or this_day()) - timedelta(days=1)


def last_week(relative_to=None):
    """Return the start of the week before relative_to (or current week if not specified)."""
    return (relative_to or this_week()) - timedelta(weeks=1)


def last_month(relative_to=None):
    """Return the start of the month before relative_to (or current month if not specified)."""
    relative_to = relative_to or this_month()
    # To get the previous month, we need to handle year boundaries
    if relative_to.month == 1:
        return relative_to.replace(year=relative_to.year - 1, month=12)
    else:
        return relative_to.replace(month=relative_to.month - 1)


def minutes_ago(minutes, relative_to=None):
    """Return the start of the minute n minutes ago from relative_to (or current minute if not specified)."""
    return (relative_to or this_minute()) - timedelta(minutes=minutes)


def hours_ago(n, relative_to=None):
    """Return the start of the hour n hours ago from relative_to (or current hour if not specified)."""
    return (relative_to or this_hour()) - timedelta(hours=n)


def days_ago(n, relative_to=None):
    """Return the start of the day n days ago from relative_to (or current day if not specified)."""
    return (relative_to or this_day()) - timedelta(days=n)


def weeks_ago(n, relative_to=None):
    """Return the start of the week n weeks ago from relative_to (or current week if not specified)."""
    return (relative_to or this_week()) - timedelta(weeks=n)


def months_ago(months, relative_to=None):
    """Return the start of the month n months ago from relative_to (or current month if not specified)."""
    relative_to = relative_to or this_month()

    # Calculate total months from year 0
    total_months = relative_to.year * 12 + relative_to.month - 1  # -1 because month is 1-indexed
    total_months -= months

    new_year = total_months // 12
    new_month = (total_months % 12) + 1  # +1 because month is 1-indexed

    return relative_to.replace(year=new_year, month=new_month)


def iso(dt):
    """Convert a datetime to ISO 8601 string format."""
    return dt.isoformat()
