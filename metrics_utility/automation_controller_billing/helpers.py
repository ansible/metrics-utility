"""Helper utilities for the automation_controller_billing package."""

import json

from itertools import chain

import pandas as pd

from django.db import connection

from metrics_utility.library.collectors.controller.config import _datetime_hook
from metrics_utility.logger import logger


def get_last_entries_from_db() -> dict:
    """
    Get AUTOMATION_ANALYTICS_LAST_ENTRIES directly from database.

    Returns:
        Optional[str]: JSON string from database, or None if not found or error occurs
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT value
                FROM conf_setting
                WHERE key = 'AUTOMATION_ANALYTICS_LAST_ENTRIES'
                LIMIT 1
            """)
            result = cursor.fetchone()

            if result and result[0]:
                json_in_json = json.loads(result[0])
                return json.loads(json_in_json, object_hook=_datetime_hook)  # This is the JSON value
    except Exception as e:
        logger.error(f'Error getting AUTOMATION_ANALYTICS_LAST_ENTRIES from database: {e}')
    return {}


def parse_json_array(x):
    """Parse a JSON string as a list, returning an empty list on failure.

    Args:
        x: A JSON string, or null/NaN value.

    Returns:
        The parsed list, or an empty list if *x* is null/NaN or not a JSON array.
    """
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


def parse_json(val):
    """Parse a JSON string into a dict, or pass through a dict unchanged.

    Args:
        val: A JSON-encoded string or an existing dict.

    Returns:
        The parsed dict, or an empty dict if parsing fails or *val* is neither
        a string nor a dict.
    """
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}  # Return empty dict if parsing fails.
    elif isinstance(val, dict):
        return val
    return {}


def merge_json_sets(json_values):
    """Merge a sequence of JSON dict values into a mapping of key → set of non-null values.

    Each value in *json_values* is parsed as a JSON dict (if necessary).  For
    every key across all dicts, non-null/non-empty/non-``'NA'`` values are
    collected into a set.

    Args:
        json_values: Iterable of JSON strings or dicts.

    Returns:
        Dict mapping each key to a set of its distinct non-empty values.
    """
    merged = {}
    for val in json_values:
        d = parse_json(val)
        if isinstance(d, dict):
            for key, value in d.items():
                # Ignore null (None) or empty string values.
                # We also want to ignore NA value used when facts are not available
                if value is not None and value not in {'', 'NA'}:
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(value)
                    else:
                        merged.setdefault(key, set()).add(value)
    return merged


def merge_arrays(values):
    """Flatten and deduplicate a sequence of lists into a single list of unique items.

    Args:
        values: Iterable of lists (None entries are ignored).

    Returns:
        A list containing all unique non-None items from all input lists.
    """
    # Filter out None values
    valid_events = [e for e in values if e is not None]
    # Flatten the list of lists and extract unique events
    unique = set(chain.from_iterable(valid_events))
    return list(unique)
