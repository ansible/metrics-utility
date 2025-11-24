import datetime
import os
import re

from dateutil.relativedelta import relativedelta

from metrics_utility.exceptions import BadParameter, DateFormatError, MissingRequiredEnvVar, MissingRequiredParameter, UnparsableParameter
from metrics_utility.logger import logger


date_format_text = (
    'An absolute date (--{name}=2023-12-20) (start of day, UTC), '
    'a number of minutes ago (--{name}=2m) (m, minute, minutes; relative to now), '
    'a number of days ago (--{name}=5d) (d, day, days; start of day, UTC), or '
    'a number of months ago (--{name}=2mo) (mo, month, months; start of day, UTC).'
)

ALLOWED_EPHEMERAL_PATTERN = r'^\d+(d|day|days|m|mo|month|months)$'

# Constants for valid values
MAX_GATHER_PERIOD_DAYS = 3650  # 10 years maximum
MAX_GATHER_PERIOD_DAYS_ERROR_MSG = f'Value must be number between 0 to {MAX_GATHER_PERIOD_DAYS}'

VALID_REPORT_TYPES = {'CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE'}

VALID_SHEETS = {
    'CCSP': {
        'ccsp_summary',
        'managed_nodes',
        'indirectly_managed_nodes',
        'inventory_scope',
        'usage_by_collections',
        'usage_by_roles',
        'usage_by_modules',
        'usage_by_organizations',
        'managed_nodes_by_organizations',
    },
    'CCSPv2': {
        'ccsp_summary',
        'jobs',
        'managed_nodes',
        'indirectly_managed_nodes',
        'inventory_scope',
        'infrastructure_summary',
        'usage_by_organizations',
        'usage_by_collections',
        'usage_by_roles',
        'usage_by_modules',
        'data_collection_status',
        'managed_nodes_by_organizations',
    },
}

VALID_COLLECTORS = {
    'execution_environments',
    'job_host_summary_service',
    'main_host',
    'main_host_daily',
    'main_indirectmanagednodeaudit',
    'main_jobevent',
    'main_jobevent_service',
    'total_workers_vcpu',
    'unified_jobs',
}

VALID_SHIP_TARGET_BUILD = {'directory', 's3', 'controller_db'}
VALID_SHIP_TARGET_GATHER = {'directory', 's3', 'crc'}

ship_path_description = 'place for collected data and built reports'


def handle_directory_ship_target():
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')

    if not ship_path:
        raise MissingRequiredEnvVar(f'Missing required env variable METRICS_UTILITY_SHIP_PATH - {ship_path_description}')

    return {'ship_path': ship_path}


def handle_s3_ship_target():
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

    # S3Handler params
    return {
        'ship_path': ship_path,
        'bucket_name': bucket_name,
        'bucket_endpoint': bucket_endpoint,
        'bucket_region': bucket_region,
        'bucket_access_key': bucket_access_key,
        'bucket_secret_key': bucket_secret_key,
    }


def handle_not_s3():
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
        allowed = '", "'.join(['controller_db', 'directory', 's3'])
        logger.warning(f'Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="{allowed}"')

    return billing_provider_params


def validate_report_type(errors, method):
    """
    Validates the 'METRICS_UTILITY_REPORT_TYPE' environment variable against a set of valid report types.

    If the environment variable is set and its value is not in the list of valid report types,
    an error message is appended to the provided errors list.

    Args:
        errors (list): A list to which error messages will be appended if validation fails.

    Returns:
        str or None: The value of the 'METRICS_UTILITY_REPORT_TYPE' environment variable if set, otherwise None.
    """
    if method == 'gather':
        return None

    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
    if report_type and report_type not in VALID_REPORT_TYPES:
        errors.append(
            f'Invalid METRICS_UTILITY_REPORT_TYPE: {report_type}. Valid values: {", ".join(VALID_REPORT_TYPES)}. '
            f'Please note these values are case sensitive'
        )
    if report_type is None:
        errors.append(
            f'Invalid METRICS_UTILITY_REPORT_TYPE is Empty. Valid values: {", ".join(VALID_REPORT_TYPES)}. '
            f'Please note these values are case sensitive'
        )
    return report_type


def validate_ccsp_report_sheets(errors, report_type):
    """
    Validates the optional CCSP report sheets specified in the environment variable
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS' for a given report type.

    Args:
        errors (list): A list to which error messages will be appended if invalid sheets are found.
        report_type (str): The type of report for which to validate the optional sheets.

    Side Effects:
        Appends error messages to the 'errors' list if any specified sheets are not valid for the given report type.

    Environment Variables:
        METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS: A comma-separated string of optional sheet names to validate.

    Notes:
        - If 'ccsp_sheets' is not set or 'report_type' is None, no validation is performed.
        - The set of valid sheets for each report type is defined in the global 'VALID_SHEETS' dictionary.
    """
    ccsp_sheets = (
        os.getenv(
            'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
            'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
        )
        .rstrip(',')
        .split(',')
    )
    if ccsp_sheets and report_type:
        ccsp_sheets_set = set(ccsp_sheets)
        if report_type in VALID_SHEETS:
            invalid = ccsp_sheets_set - VALID_SHEETS[report_type]
            if invalid:
                errors.append(
                    f'Invalid METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS for '
                    f'{report_type}: {", ".join(invalid)}. Valid values: '
                    f'{", ".join(VALID_SHEETS[report_type])}'
                )


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


def validate_ship_target(errors, method, report_type):
    """
    Validates the 'METRICS_UTILITY_SHIP_TARGET' environment variable against a set of valid ship targets.

    If the environment variable is set and its value is not in the list of valid ship targets,
    an error message is appended to the provided errors list.

    Args:
        errors (list): A list to which error messages will be appended if validation fails.

    Returns:
        str or None: The value of the 'METRICS_UTILITY_SHIP_TARGET' environment variable if set, otherwise None.

    Notes:
        - The set of valid ship targets is defined by the global variable VALID_SHIP_TARGET.
        - Error messages include the invalid ship target and the list of valid values.
    """
    ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET')
    ship_target_type = VALID_SHIP_TARGET_BUILD
    if method == 'gather':
        ship_target_type = VALID_SHIP_TARGET_GATHER
    if ship_target is None:
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET is empty. Valid values: {", ".join(ship_target_type)}')
    if ship_target and ship_target not in ship_target_type:
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET: {ship_target}. Valid values: {", ".join(ship_target_type)}')
    if method == 'build' and report_type == 'RENEWAL_GUIDANCE' and ship_target != 'controller_db':
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET: {ship_target}. Only "controller_db" is allowed for "RENEWAL_GUIDANCE"')
    return ship_target


def validate_ship_path(errors, ship_target, method):
    """
    Validates the ship path environment variable based on the ship target.

    Args:
        errors (list): A list to which error messages will be appended if validation fails.
        ship_target (str): The value of the METRICS_UTILITY_SHIP_TARGET environment variable.

    Notes:
        - For 'directory' ship target, checks if METRICS_UTILITY_SHIP_PATH is an existing directory.
        - Appends an error message to 'errors' if the directory does not exist.
    """
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
    if not ship_path:
        # already handled in handle_*_ship_target
        return

    if method == 'build' and ship_target in VALID_SHIP_TARGET_BUILD - {'s3'} and not os.path.isdir(ship_path):
        errors.append(f'Invalid METRICS_UTILITY_SHIP_PATH: {ship_path} is not an existing directory.')


def validate_max_gather_period_days(errors):
    """
    Validates the 'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS' environment variable.

    Checks that the value is a positive integer within a reasonable range (1-3650 days).
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


def handle_env_validation(method: str):
    """
    Validates required environment variables and configuration for the application.

    This function performs a series of validation checks on environment variables and configuration
    settings required for the application to run correctly. It collects any validation errors and
    raises a `MissingRequiredEnvVar` exception if any issues are found.

    Validation steps include:
    - Validating the report type.
    - Validating CCSP report sheets based on the report type.
    - Validating collectors.
    - Validating the ship target (uses the `method` argument to determine which set of valid targets to check).
    - Validating the ship path based on the ship target.
    - Validating the max gather period days.

    Args:
        method (str): Determines which command is running, for command-specific logic
            Should be either 'gather' or 'build'

    Raises:
        MissingRequiredEnvVar: If any required environment variable or configuration is missing or invalid.

    Notes:
        - The function accumulates all errors before raising an exception, providing a comprehensive error message.
        - The specific validation functions (`validate_*`) are expected to append error messages to the provided `errors` list.
    """
    errors = []
    report_type = validate_report_type(errors, method)
    validate_collectors(errors)
    validate_max_gather_period_days(errors)
    if method == 'build':
        validate_ccsp_report_sheets(errors, report_type)
    ship_target = validate_ship_target(errors, method, report_type)
    validate_ship_path(errors, ship_target, method)
    if errors:
        raise MissingRequiredEnvVar('\n'.join(errors))


def handle_not_crc():
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
    return datetime.datetime.now()


def startofday(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_date_param(value, help_texts={None: ''}, name=None):
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


def validate_ccsp_params(options):
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
    opt_month = options.get('month', None)
    opt_since = options.get('since', None)
    opt_until = options.get('until', None)
    opt_ephemeral = options.get('ephemeral', None)

    # bad type
    if opt_ephemeral:
        raise BadParameter(f'METRICS_UTILITY_REPORT_TYPE {report_type} does not allow --ephemeral.')

    # bad combos
    if opt_month and (opt_since or opt_until):
        raise BadParameter('The --since and --until parameters are not allowed if the --month parameter is provided.')
    if opt_until and not opt_since:
        raise BadParameter('The --until parameter is ignored without --since.')


def validate_renewal_params(options, help_texts):
    opt_month = options.get('month', None)
    opt_since = options.get('since', None)
    opt_until = options.get('until', None)
    opt_ephemeral = options.get('ephemeral', None)

    # bad type
    if opt_month:
        raise BadParameter('The --month parameter is not allowed for renewal guidance report.')
    if opt_until:
        raise BadParameter('The --until parameter is not allowed for renewal guidance report.')

    # required
    if not opt_since:
        raise MissingRequiredParameter('The --since parameter is required for renewal guidance report.')

    # validation
    if opt_ephemeral and not re.match(ALLOWED_EPHEMERAL_PATTERN, opt_ephemeral):
        raise UnparsableParameter(help_texts.get('ephemeral'))


def parse_since_until(options, help_texts):
    opt_since = options.get('since', None)
    opt_until = options.get('until', None)

    since = parse_date_param(opt_since, help_texts, 'since')
    until = parse_date_param(opt_until, help_texts, 'until')

    if (opt_since and opt_until) and until < since:
        raise UnparsableParameter('The date for --until cannot be before the date for --since.')

    return since, until


def validate_build_params(options, help_texts):
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
    if not report_type:
        return None, None

    if report_type in {'CCSP', 'CCSPv2'}:
        validate_ccsp_params(options)

    if report_type in {'RENEWAL_GUIDANCE'}:
        validate_renewal_params(options, help_texts)

    return parse_since_until(options, help_texts)


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
            date = datetime.datetime.strptime(month, '%Y-%m')
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
