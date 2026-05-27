import os

from metrics_utility.logger import logger


def get_max_gather_period_days():
    """
    Get the maximum gather period in days from environment variable.
    Defaults to 28 days if not set or invalid.
    """
    MAX_GATHER_PERIOD_DAYS_DEFAULT = 28

    try:
        return int(os.getenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', str(MAX_GATHER_PERIOD_DAYS_DEFAULT)))
    except (ValueError, TypeError):
        logger.error('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS can not be converted to an integer')
        # raise original exception
        raise


def get_dashboard_page_size():
    """
    Get the dashboard collector page size from environment variable.
    Controls how many job rows are fetched per SQL query when paginating.
    Defaults to 10000 if not set or invalid.
    """
    DASHBOARD_PAGE_SIZE_DEFAULT = 10000

    try:
        return int(os.getenv('METRICS_UTILITY_DASHBOARD_PAGE_SIZE', str(DASHBOARD_PAGE_SIZE_DEFAULT)))
    except (ValueError, TypeError):
        logger.error('METRICS_UTILITY_DASHBOARD_PAGE_SIZE can not be converted to an integer')
        raise


def get_max_dashboard_records():
    """
    Get the maximum number of dashboard job records to collect per run from environment variable.
    Acts as a safety cap to prevent OOM errors on very large deployments.
    Set to 0 (the default) to disable the limit and collect all records in the window.
    """
    MAX_DASHBOARD_RECORDS_DEFAULT = 0

    try:
        return int(os.getenv('METRICS_UTILITY_MAX_DASHBOARD_RECORDS', str(MAX_DASHBOARD_RECORDS_DEFAULT)))
    except (ValueError, TypeError):
        logger.error('METRICS_UTILITY_MAX_DASHBOARD_RECORDS can not be converted to an integer')
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

    b = s.lower() in {'1', 'true'}
    return b
