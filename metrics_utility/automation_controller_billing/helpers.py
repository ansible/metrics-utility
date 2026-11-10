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


class HashableDict(dict):
    """Dict subclass that can be stored in a set and ordered for report rendering.

    Nested dict/list values are expected to already be converted via
    :func:`make_hashable`. Remains a real ``dict`` so ``json.dumps`` emits a
    native JSON object (not a string).
    """

    def __hash__(self):
        return hash(frozenset(self.items()))

    def __lt__(self, other):
        if not isinstance(other, dict):
            return NotImplemented
        return sorted(self.items()) < sorted(other.items())


def make_hashable(obj):
    """Recursively convert dict/list/set values into hashable (and orderable) forms.

    Dicts become :class:`HashableDict`, lists become tuples, and sets become
    frozensets. Scalars are returned unchanged. Used by aggregation helpers that
    collect fact values into ``set()`` members.
    """
    if isinstance(obj, HashableDict):
        return obj
    if isinstance(obj, dict):
        return HashableDict((k, make_hashable(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return tuple(make_hashable(v) for v in obj)
    if isinstance(obj, tuple):
        return tuple(make_hashable(v) for v in obj)
    if isinstance(obj, set):
        return frozenset(make_hashable(v) for v in obj)
    return obj


def merge_json_sets(json_values):
    """Merge a sequence of JSON dict values into a mapping of key → set of non-null values.

    Each value in *json_values* is parsed as a JSON dict (if necessary).  For
    every key across all dicts, non-null/non-empty/non-``'NA'`` values are
    collected into a set.

    Nested dict/list values are wrapped via :func:`make_hashable` so they can be
    stored as set members (AAP-101848).

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
                # We also want to ignore NA value used when facts are not available.
                # Use != (not set membership): nested dict values are unhashable (AAP-101848).
                if value is not None and value != '' and value != 'NA':
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(make_hashable(v) for v in value)
                    else:
                        merged.setdefault(key, set()).add(make_hashable(value))
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
