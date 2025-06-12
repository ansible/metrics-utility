import json
import re

from datetime import datetime, timedelta, timezone
from itertools import chain

import pandas as pd

from dateutil import parser
from dateutil.relativedelta import relativedelta

from metrics_utility.exceptions import DateFormatError, UnparsableParameter


def parse_json_array(x):
    if pd.isnull(x):
        return []
    try:
        parsed = json.loads(x)
        # Check if the parsed JSON object is a list (array)
        if isinstance(parsed, list):
            return parsed
        else:
            return []
    except json.JSONDecodeError:
        return []


# Helper function to parse a JSON string or return the dict if it's already a dict.
def parse_json(val):
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}  # Return empty dict if parsing fails.
    elif isinstance(val, dict):
        return val
    return {}


# Function to merge a list of JSON values into a dict mapping each key to a set of non-null/non-empty values.
def merge_json_sets(json_values):
    merged = {}
    for val in json_values:
        d = parse_json(val)
        if isinstance(d, dict):
            for key, value in d.items():
                # Ignore null (None) or empty string values.
                # We also want to ignore NA value used when facts are not available
                if value is not None and value != '' and value != 'NA':
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(value)
                    else:
                        merged.setdefault(key, set()).add(value)
    return merged


# Function to merge array type columns getting a unique set back
def merge_arrays(values):
    # Filter out None values
    valid_events = [e for e in values if e is not None]
    # Flatten the list of lists and extract unique events
    unique = set(chain.from_iterable(valid_events))
    return list(unique)


# should also suport quarters, start/end of month ("4mo_ago_beginning" vs "1 mo ago end"), but doesn't yet
def parse_date_param(value, help=''):
    if not value:
        return None

    value = value.strip().lower()
    if value.isdigit():
        raise UnparsableParameter(f'Bare numbers are not valid ({help})')

    now = datetime.now()
    parsed_date = None

    # N days ago, start of day
    match = re.fullmatch(r'(\d+)\s*_*(d|da|day|days)(\s*_*ago)?', value)
    if match:
        days_ago = int(match.group(1))
        parsed_date = (now - timedelta(days=days_ago - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # N months ago, start of day
    match = re.fullmatch(r'(\d+)\s*_*(mo|mon|mont|month|months)(\s*_*ago)?', value)
    if match:
        months_ago = int(match.group(1))
        parsed_date = (now - relativedelta(months=months_ago)).replace(hour=0, minute=0, second=0, microsecond=0)

    # N minutes ago
    match = re.fullmatch(r'(\d+)\s*_*(m|mi|min|minu|minut|minute|minutes)(\s*_*ago)?', value)
    if match:
        minutes_ago = int(match.group(1))
        parsed_date = now - timedelta(minutes=minutes_ago)

    # actual date
    if not parsed_date:
        parsed_date = parser.parse(value)

    # Add default UTC timezone
    if parsed_date and parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)

    return parsed_date


def parse_number_of_days(value, help=''):
    if not value:
        return None

    value = value.strip().lower()
    if value.isdigit():
        raise UnparsableParameter(f'Bare numbers are not valid ({help})')

    # N days ago
    match = re.fullmatch(r'(\d+)\s*_*(d|da|day|days)', value)
    if match:
        return int(match.group(1))

    # N months ago - using 30 days per month
    match = re.fullmatch(r'(\d+)\s*_*(m|mo|mon|mont|month|months)', value)
    if match:
        return int(match.group(1)) * 30

    raise UnparsableParameter(f"Can't parse parameter value {value} ({help})")


def parse_month(month):
    """Process month argument"""
    if month is not None:
        try:
            date = datetime.strptime(f'{month}', '%Y-%m')
        except ValueError:
            raise DateFormatError('Invalid --month format. Supported date format: YYYY-MM')
    else:
        """Return last month if no month was passed"""
        beginning_of_the_month = datetime.today().replace(day=1)
        beginning_of_the_previous_month = beginning_of_the_month - relativedelta(months=1)
        date = beginning_of_the_previous_month
        y = date.strftime('%Y')
        m = date.strftime('%m')
        month = f'{y}-{m}'

    return month, date, date + relativedelta(months=1)
