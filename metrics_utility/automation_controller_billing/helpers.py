import datetime
import json

from datetime import timezone
from itertools import chain

import pandas as pd

from dateutil import parser
from dateutil.relativedelta import relativedelta

from metrics_utility.exceptions import DateFormatError, UnparsableParameter


ALLOWED_EPHEMERAL_PATTERN = r'^\d+(d|day|days|m|mo|month|months)$'
SINCE_AND_UNTIL_GATHER_PATTERN = r'^\d+[dm]$|^\d{4}-\d{2}-\d{2}$'
SINCE_AND_UNTIL_BUILD_PATTERN = r'^\d+(d|mo|month|months|m)$|^\d{4}-\d{2}-\d{2}$'


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


# patchable in tests
def now():
    return datetime.datetime.now()


def parse_date_param(date_option):
    if not date_option:
        return None

    parsed_date = None
    if date_option.endswith('d'):
        days_ago = int(date_option[0:-1])
        parsed_date = (now() - datetime.timedelta(days=days_ago - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_option.endswith('mo') or date_option.endswith('month') or date_option.endswith('months'):
        if date_option.endswith('mo'):
            suffix_length = len('mo')
        elif date_option.endswith('month'):
            suffix_length = len('month')
        elif date_option.endswith('months'):
            suffix_length = len('months')
        months_ago = int(date_option[0:-suffix_length])
        parsed_date = (now() - relativedelta(months=months_ago)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_option.endswith('m'):
        minutes_ago = int(date_option[0:-1])
        parsed_date = now() - datetime.timedelta(minutes=minutes_ago)
    else:
        parsed_date = parser.parse(date_option)

    # Set timezone to UTC when missing
    if parsed_date and parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)

    return parsed_date


def parse_number_of_days(date_option):
    if not date_option:
        return None

    if date_option.endswith('d') or date_option.endswith('day') or date_option.endswith('days'):
        if date_option.endswith('d'):
            suffix_length = len('d')
        elif date_option.endswith('day'):
            suffix_length = len('day')
        elif date_option.endswith('days'):
            suffix_length = len('days')

        days = int(date_option[0:-suffix_length])
    elif date_option.endswith('mo') or date_option.endswith('month') or date_option.endswith('months'):
        if date_option.endswith('mo'):
            suffix_length = len('mo')
        elif date_option.endswith('month'):
            suffix_length = len('month')
        elif date_option.endswith('months'):
            suffix_length = len('months')

        days = int(date_option[0:-suffix_length]) * 30  # using 30 days per month
    else:
        raise UnparsableParameter(f"Can't parse parameter value {date_option}")

    return days


def handle_month(month):
    """Process month argument"""
    if month is not None:
        try:
            date = datetime.datetime.strptime(f'{month}', '%Y-%m')
        except ValueError:
            raise DateFormatError('Invalid --month format. Supported date format: YYYY-MM')
    else:
        """Return last month if no month was passed"""
        beginning_of_the_month = datetime.datetime.today().replace(day=1)
        beginning_of_the_previous_month = beginning_of_the_month - relativedelta(months=1)
        date = beginning_of_the_previous_month
        y = date.strftime('%Y')
        m = date.strftime('%m')
        month = f'{y}-{m}'

    return month, date, date + relativedelta(months=1)
