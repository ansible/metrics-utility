import json
import os

from django.db import connection

from metrics_utility.library.collectors.controller.config import _datetime_hook
from metrics_utility.logger import logger


def get_max_gather_period_days():
    """
    Get the maximum gather period in days from environment variable.
    Defaults to 28 days if not set or invalid.
    """
    default = 28

    try:
        return int(os.getenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', str(default)))
    except (ValueError, TypeError):
        logger.error('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS can not be converted to an integer')
        # raise original exception
        raise


def get_optional_collectors():
    """
    Get the list of optional collectors from environment variable.
    Defaults to 'main_jobevent' if not set.
    """
    return list(filter(bool, os.getenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').strip(', \t').split(',')))


def bool_from_env(name, default=None):
    """
    Convert environment variable to boolean.
    Returns True if value is '1' or 'true' (case-insensitive).
    Returns default if environment variable is not set.
    """
    s = os.getenv(name, None)
    if s is None:
        return default

    return s.lower() in {'1', 'true'}


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
