"""Date-parsing helpers for management commands."""

import datetime
import re

from dateutil.relativedelta import relativedelta

from metrics_utility.exceptions import UnparsableParameter


date_format_text = (
    'An absolute date (--{name}=2023-12-20) (start of day, UTC), '
    'a number of minutes ago (--{name}=2m) (m, minute, minutes; relative to now), '
    'a number of days ago (--{name}=5d) (d, day, days; start of day, UTC), or '
    'a number of months ago (--{name}=2mo) (mo, month, months; start of day, UTC).'
)


# patchable in tests
def now():
    return datetime.datetime.now()


def startofday(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_date_param(value, help_texts=None, name=None):
    """Parse a human-friendly date string into a timezone-aware datetime.

    Supported formats: ISO date (``2023-12-20``), ``Nd``/``Ndays`` (N days ago,
    start of day), ``Nmo``/``Nmonths`` (N months ago, start of day), or
    ``Nm``/``Nminutes`` (N minutes ago).
    """
    if not value:
        return None

    help_text = help_texts.get(name) if help_texts else ''

    if value.isdigit():
        raise UnparsableParameter(f'Bare integers are not allowed for --{name}: {help_text}')

    try:
        if match := re.fullmatch(r'(\d+)(d|day|days)', value):
            days_ago = int(match.group(1))
            parsed = startofday(now() - datetime.timedelta(days=days_ago - 1))
        elif match := re.fullmatch(r'(\d+)(mo|mon|month|months)', value):
            months_ago = int(match.group(1))
            parsed = startofday(now() - relativedelta(months=months_ago))
        elif match := re.fullmatch(r'(\d+)(m|min|minute|minutes)', value):
            minutes_ago = int(match.group(1))
            parsed = now() - datetime.timedelta(minutes=minutes_ago)
        else:
            parsed = datetime.datetime.fromisoformat(value).astimezone(datetime.timezone.utc)
    except Exception as e:
        raise UnparsableParameter(f'{str(e)}: {help_text}')

    # Set timezone to UTC when missing
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)

    return parsed
