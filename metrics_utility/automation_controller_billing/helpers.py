import json

from itertools import chain
from typing import Any, Dict, Tuple

import pandas as pd

from django.db import connection
from django.utils.dateparse import parse_datetime

from metrics_utility.logger import logger


def get_last_entries_from_db() -> Dict:
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
                return json.loads(json_in_json, object_hook=datetime_hook)  # This is the JSON value
    except Exception as e:
        logger.error(f'Error getting AUTOMATION_ANALYTICS_LAST_ENTRIES from database: {e}')
    return {}


def get_config_and_settings_from_db() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Get license information directly from the database."""
    license_info = {}
    settings_info = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT key, value
                FROM conf_setting
                WHERE key IN ('LICENSE', 'INSTALL_UUID', 'TOWER_URL_BASE',
                           'SUBSCRIPTION_USAGE_MODEL','PENDO_TRACKING_STATE','AUTHENTICATION_BACKENDS',
                           'LOG_AGGREGATOR_LOGGERS',  'SYSTEM_UUID', 'LOG_AGGREGATOR_ENABLED',
                           'LOG_AGGREGATOR_TYPE')
            """)
            rows = cursor.fetchall()
            for row in rows:
                key, value = row
                if key == 'LICENSE':
                    license_info = json.loads(value)  # The LICENSE key has a value which is an object.
                # We want all the items in the object put on their own
                # dict.
                else:
                    settings_info[key.lower()] = json.loads(value)

    except Exception as e:
        logger.error(f'Error getting license information from database: {e}')
    return license_info, settings_info


def _fetch_one(db, sql):
    with db.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
    return None


def get_controller_version_from_db() -> str:
    """Get AWX/Controller version from the main_instance DB table."""
    return _fetch_one(
        connection,
        """
        SELECT version
        FROM main_instance
        WHERE enabled = true
            AND version IS NOT NULL
            AND version != ''
        ORDER BY last_seen DESC
        LIMIT 1
    """,
    )


def datetime_hook(d):
    new_d = {}
    for key, value in d.items():
        try:
            new_d[key] = parse_datetime(value)
        except TypeError:
            new_d[key] = value
    return new_d


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
