"""Validation helpers for the gather_automation_controller_billing_data command."""

import datetime
import os
import re

from dateutil.relativedelta import relativedelta

from metrics_utility.exceptions import MissingRequiredEnvVar, UnparsableParameter
from metrics_utility.logger import logger


date_format_text = (
    'An absolute date (--{name}=2023-12-20) (start of day, UTC), '
    'a number of minutes ago (--{name}=2m) (m, minute, minutes; relative to now), '
    'a number of days ago (--{name}=5d) (d, day, days; start of day, UTC), or '
    'a number of months ago (--{name}=2mo) (mo, month, months; start of day, UTC).'
)

# Constants for valid values
MAX_GATHER_PERIOD_DAYS = 3650  # 10 years maximum
MAX_GATHER_PERIOD_DAYS_ERROR_MSG = f'Value must be number between 0 to {MAX_GATHER_PERIOD_DAYS}'

VALID_COLLECTORS = {
    ## shared
    # config, manifest, data_collection_status are always on
    ## ccsp
    # job_host_summary is on by default
    'main_host',
    'main_host_daily',
    'main_indirectmanagednodeaudit',
    'main_jobevent',
    ## vcpu
    'total_workers_vcpu',
    ## anonymized
    'controller_version_service',
    'credentials_service',
    'execution_environments',
    'feature_flags_service',
    'job_host_summary_service',
    'main_jobevent_service',
    'table_metadata',
    'unified_jobs',
    ## dashboard & service
    'dashboard_jobs',
    'task_executions_service',
}

VALID_SHIP_TARGET_GATHER = {'directory', 's3', 'crc'}

ship_path_description = 'place for collected data'


def handle_directory_ship_target():
    """Read and validate METRICS_UTILITY_SHIP_PATH for directory mode.

    Returns:
        Tuple of ``(billing_provider_params, ship_params)`` where *billing_provider_params* is
        empty and *ship_params* contains ``'ship_path'``.

    Raises:
        :exc:`~metrics_utility.exceptions.MissingRequiredEnvVar`: If the
            environment variable is not set.
    """
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')

    if not ship_path:
        raise MissingRequiredEnvVar(f'Missing required env variable METRICS_UTILITY_SHIP_PATH - {ship_path_description}')

    return {}, {'ship_path': ship_path}


def handle_s3_ship_target():
    """Read and validate all S3-related environment variables.

    Returns:
        Tuple of ``(billing_provider_params, ship_params)`` where *billing_provider_params* is
        empty and *ship_params* contains ``'ship_path'`` and S3 credentials.

    Raises:
        :exc:`~metrics_utility.exceptions.MissingRequiredEnvVar`: If any required
            S3 variable is missing.
    """
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
    bucket_name = os.getenv('METRICS_UTILITY_BUCKET_NAME')
    bucket_endpoint = os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT')
    bucket_region = os.getenv('METRICS_UTILITY_BUCKET_REGION')  # optional

    # S3 credentials
    bucket_access_key = os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY')
    bucket_secret_key = os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY')

    missing = []
    if not bucket_name:
        missing += ['METRICS_UTILITY_BUCKET_NAME - name of S3 bucket']
    if not bucket_endpoint:
        missing += ['METRICS_UTILITY_BUCKET_ENDPOINT - S3 endpoint, eg. https://s3.us-east.example.com']
    if not bucket_access_key:
        missing += ['METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key']
    if not bucket_secret_key:
        missing += ['METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key']
    if not ship_path:
        missing += [f'METRICS_UTILITY_SHIP_PATH - {ship_path_description}']
    # bucket_region is optional

    if missing:
        raise MissingRequiredEnvVar(f'Missing some required env variables for S3 configuration, namely: {", ".join(missing)}.')

    ship_params = {
        'ship_path': ship_path,
        'bucket_name': bucket_name,
        'bucket_endpoint': bucket_endpoint,
        'bucket_region': bucket_region,
        'bucket_access_key': bucket_access_key,
        'bucket_secret_key': bucket_secret_key,
    }
    return {}, ship_params


def handle_not_s3():
    """Warn if S3 environment variables are set while the ship target is not ``'s3'``."""
    surplus = []

    if os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY'):
        surplus += ['METRICS_UTILITY_BUCKET_ACCESS_KEY']
    if os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT'):
        surplus += ['METRICS_UTILITY_BUCKET_ENDPOINT']
    if os.getenv('METRICS_UTILITY_BUCKET_NAME'):
        surplus += ['METRICS_UTILITY_BUCKET_NAME']
    if os.getenv('METRICS_UTILITY_BUCKET_REGION'):
        surplus += ['METRICS_UTILITY_BUCKET_REGION']
    if os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY'):
        surplus += ['METRICS_UTILITY_BUCKET_SECRET_KEY']

    if surplus:
        logger.warning(f'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="s3": {", ".join(surplus)}')


def handle_crc_ship_target():
    """Read and validate CRC-related billing provider environment variables.

    Returns:
        Tuple of ``(billing_provider_params, ship_params)`` where *billing_provider_params*
        contains ``'billing_provider'`` (and optionally ``'billing_account_id'``
        and ``'red_hat_org_id'``) and *ship_params* is empty.

    Raises:
        :exc:`~metrics_utility.exceptions.MissingRequiredEnvVar`: If a required
            billing variable is missing or has an unsupported value.
    """
    billing_provider = os.getenv('METRICS_UTILITY_BILLING_PROVIDER')
    red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID')

    billing_provider_params = {'billing_provider': billing_provider}
    if billing_provider == 'aws':
        billing_account_id = os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID')
        if not billing_account_id:
            raise MissingRequiredEnvVar('METRICS_UTILITY_BILLING_ACCOUNT_ID, containing AWS 12 digit customer id needs to be provided.')
        billing_provider_params['billing_account_id'] = billing_account_id
    else:
        raise MissingRequiredEnvVar('Uknown METRICS_UTILITY_BILLING_PROVIDER env var, supported values are [aws].')

    if red_hat_org_id:
        billing_provider_params['red_hat_org_id'] = red_hat_org_id

    # only used for the other modes
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
    if ship_path:
        allowed = '", "'.join(['directory', 's3'])
        logger.warning(f'Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="{allowed}"')

    return billing_provider_params, {}


def validate_collectors(errors):
    """
    Validates the list of optional collectors specified in the
    METRICS_UTILITY_OPTIONAL_COLLECTORS environment variable against a set
    of valid collectors.

    If any invalid collectors are found, an error message is appended to the
    provided errors list.

    Args:
        errors (list): A list to which error messages will be appended if
            invalid collectors are found.

    Environment Variables:
        METRICS_UTILITY_OPTIONAL_COLLECTORS (str, optional): Comma-separated
            list of collector names. Defaults to 'main_jobevent' if not set.

    Notes:
        - The set of valid optional collectors is defined by the global variable VALID_COLLECTORS.
        - Error messages include the invalid collector names and the list ofvalid values.
    """

    collectors = os.getenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').strip(', \t')
    if collectors:
        collectors = collectors.split(',')
    if collectors:
        invalid = set(collectors) - VALID_COLLECTORS
        if invalid:
            errors.append(f'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS: {", ".join(invalid)}. Valid values: {", ".join(VALID_COLLECTORS)}')


def validate_ship_target(errors):
    """Validate METRICS_UTILITY_SHIP_TARGET for the gather command.

    Args:
        errors (list): A list to which error messages will be appended if validation fails.

    Returns:
        str or None: The value of the 'METRICS_UTILITY_SHIP_TARGET' environment variable if set, otherwise None.
    """
    ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET')
    if ship_target is None:
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET is empty. Valid values: {", ".join(VALID_SHIP_TARGET_GATHER)}')
    if ship_target and ship_target not in VALID_SHIP_TARGET_GATHER:
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET: {ship_target}. Valid values: {", ".join(VALID_SHIP_TARGET_GATHER)}')
    return ship_target


def validate_max_gather_period_days(errors):
    """
    Validates the 'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS' environment variable.

    Checks that the value is an integer within 0..MAX_GATHER_PERIOD_DAYS (10 years).
    If the environment variable is set and its value is not valid, an error message
    is appended to the provided errors list.

    Args:
        errors (list): A list to which error messages will be appended if validation fails.

    Returns:
        int or None: The validated value as an integer if set and valid, otherwise None.
    """
    max_gather_days_str = os.getenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS')
    if max_gather_days_str is None:
        return None

    try:
        max_gather_days = int(max_gather_days_str)
        if max_gather_days < 0 or max_gather_days > MAX_GATHER_PERIOD_DAYS:
            errors.append(f'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: {max_gather_days}. {MAX_GATHER_PERIOD_DAYS_ERROR_MSG}')
        else:
            return max_gather_days
    except (ValueError, TypeError):
        errors.append(f'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "{max_gather_days_str}". {MAX_GATHER_PERIOD_DAYS_ERROR_MSG}')

    return None


def handle_env_validation():
    """
    Validates required environment variables for the gather command.

    Validation steps include:
    - Validating collectors.
    - Validating the max gather period days.
    - Validating the ship target.

    Raises:
        MissingRequiredEnvVar: If any required environment variable or configuration is missing or invalid.
    """
    errors = []
    validate_collectors(errors)
    validate_max_gather_period_days(errors)
    ship_target = validate_ship_target(errors)
    if errors:
        raise MissingRequiredEnvVar('\n'.join(errors))
    return ship_target


def handle_not_crc():
    """Warn if CRC environment variables are set while the ship target is not ``'crc'``."""
    surplus = []

    if os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID'):
        surplus += ['METRICS_UTILITY_BILLING_ACCOUNT_ID']
    if os.getenv('METRICS_UTILITY_BILLING_PROVIDER'):
        surplus += ['METRICS_UTILITY_BILLING_PROVIDER']
    if os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID'):
        surplus += ['METRICS_UTILITY_RED_HAT_ORG_ID']

    if surplus:
        logger.warning(f'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="crc": {", ".join(surplus)}')


# patchable in tests
def now():
    """Return the current local datetime (patchable in tests).

    Returns:
        :class:`datetime.datetime` representing the current moment.
    """
    return datetime.datetime.now()


def startofday(dt):
    """Return *dt* with the time component reset to midnight.

    Args:
        dt: A :class:`datetime.datetime` instance.

    Returns:
        The same date at 00:00:00.000000.
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_date_param(value, help_texts={None: ''}, name=None):
    """Parse a human-friendly date string into a timezone-aware datetime.

    Supported formats: ISO date (``2023-12-20``), ``Nd``/``Ndays`` (N days ago,
    start of day), ``Nmo``/``Nmonths`` (N months ago, start of day), or
    ``Nm``/``Nminutes`` (N minutes ago).

    Args:
        value: The string to parse, or None/empty to return None.
        help_texts: Dict mapping parameter names to help text shown in errors.
        name: The parameter name (used as a key into *help_texts* and in error messages).

    Returns:
        A timezone-aware :class:`datetime.datetime`, or None if *value* is empty.

    Raises:
        :exc:`~metrics_utility.exceptions.UnparsableParameter`: If the string
            cannot be parsed.
    """
    if not value:
        return None

    help_text = help_texts.get(name)

    if value.isdigit():
        raise UnparsableParameter(f'Bare integers are not allowed for --{name}: {help_text}')

    parsed = None
    try:
        # N days ago, start of day
        match = re.fullmatch(r'(\d+)(d|day|days)', value)
        if match:
            days_ago = int(match.group(1))
            parsed = startofday(now() - datetime.timedelta(days=days_ago - 1))

        # N months ago, start of day
        match = re.fullmatch(r'(\d+)(mo|mon|month|months)', value)
        if match:
            months_ago = int(match.group(1))
            parsed = startofday(now() - relativedelta(months=months_ago))

        # N minutes ago
        match = re.fullmatch(r'(\d+)(m|min|minute|minutes)', value)
        if match:
            minutes_ago = int(match.group(1))
            parsed = now() - datetime.timedelta(minutes=minutes_ago)

        # actual date
        if not parsed:
            parsed = datetime.datetime.fromisoformat(value).astimezone(datetime.timezone.utc)
    except Exception as e:
        raise UnparsableParameter(f'{str(e)}: {help_text}')

    # Set timezone to UTC when missing
    if parsed and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)

    return parsed
