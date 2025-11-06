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


def get_optional_collectors():
    """
    Get the list of optional collectors from environment variable.
    Defaults to 'main_jobevent' if not set.
    """
    return list(filter(bool, os.getenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').strip(', \t').split(',')))
