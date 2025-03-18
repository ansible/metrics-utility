import datetime
import json
import pandas as pd

from dateutil import parser
from dateutil.relativedelta import relativedelta
from datetime import timezone
from itertools import chain

from metrics_utility.exceptions import UnparsableParameter


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


def parse_date_param(date_option):
    parsed_date = None
    if date_option and date_option.endswith('d'):
        days_ago = int(date_option[0:-1])
        parsed_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_option and (date_option.endswith('mo') or date_option.endswith('month') or date_option.endswith('months')):
        if date_option.endswith('mo'):
            suffix_length = len('mo')
        elif date_option.endswith('month'):
            suffix_length = len('month')
        elif date_option.endswith('months'):
            suffix_length = len('months')
        months_ago = int(date_option[0:-suffix_length])
        parsed_date = (datetime.datetime.now() - relativedelta(months=months_ago)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_option and date_option.endswith('m'):
        minutes_ago = int(date_option[0:-1])
        parsed_date = datetime.datetime.now() - datetime.timedelta(minutes=minutes_ago)
    else:
        parsed_date = parser.parse(date_option) if date_option else None
    # Add default utc timezone
    if parsed_date and parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)

    return parsed_date


def parse_number_of_days(date_option):
    if date_option and (date_option.endswith('d') or date_option.endswith('day') or date_option.endswith('days')):
        if date_option.endswith('d'):
            suffix_length = len('d')
        elif date_option.endswith('day'):
            suffix_length = len('day')
        elif date_option.endswith('days'):
            suffix_length = len('days')

        days = int(date_option[0:-suffix_length])
    elif date_option and (date_option.endswith('mo') or date_option.endswith('month') or date_option.endswith('months')):
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
