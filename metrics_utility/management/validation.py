import logging
import os
import re

from datetime import datetime, timedelta, timezone

from dateutil import parser
from dateutil.relativedelta import relativedelta

from metrics_utility.exceptions import BadParameter, DateFormatError, MissingRequiredEnvVar, MissingRequiredParameter, UnparsableParameter


# Constants for valid values
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
    },
    'CCSPv2': {
        'ccsp_summary',
        'jobs',
        'managed_nodes',
        'indirectly_managed_nodes',
        'inventory_scope',
        'usage_by_organizations',
        'usage_by_collections',
        'usage_by_roles',
        'usage_by_modules',
        'managed_nodes_by_organization',
        'data_collection_status',
    },
}
VALID_COLLECTORS = {'main_host', 'main_jobevent', 'main_indirectmanagednodeaudit'}
VALID_SHIP_TARGET_BUILD = {'directory', 's3', 'controller_db'}
VALID_SHIP_TARGET_GATHER = {'directory', 's3', 'crc'}


logger = logging.getLogger(__name__)

ship_path_description = 'place for collected data and built reports'


def handle_directory_ship_target():
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)

    if not ship_path:
        raise MissingRequiredEnvVar(f'Missing required env variable METRICS_UTILITY_SHIP_PATH - {ship_path_description}')

    return {'ship_path': ship_path}


def handle_s3_ship_target():
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
    bucket_name = os.getenv('METRICS_UTILITY_BUCKET_NAME', None)
    bucket_endpoint = os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', None)
    bucket_region = os.getenv('METRICS_UTILITY_BUCKET_REGION', None)  # optional

    # S3 credentials
    bucket_access_key = os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY', None)
    bucket_secret_key = os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY', None)

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

    if os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY', None):
        surplus += ['METRICS_UTILITY_BUCKET_ACCESS_KEY']
    if os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', None):
        surplus += ['METRICS_UTILITY_BUCKET_ENDPOINT']
    if os.getenv('METRICS_UTILITY_BUCKET_NAME', None):
        surplus += ['METRICS_UTILITY_BUCKET_NAME']
    if os.getenv('METRICS_UTILITY_BUCKET_REGION', None):
        surplus += ['METRICS_UTILITY_BUCKET_REGION']
    if os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY', None):
        surplus += ['METRICS_UTILITY_BUCKET_SECRET_KEY']

    if surplus:
        logger.warning(f'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="s3": {", ".join(surplus)}')


def handle_crc_ship_target():
    billing_provider = os.getenv('METRICS_UTILITY_BILLING_PROVIDER', None)
    red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID', None)

    billing_provider_params = {'billing_provider': billing_provider}
    if billing_provider == 'aws':
        billing_account_id = os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', None)
        if not billing_account_id:
            raise MissingRequiredEnvVar('Env var: METRICS_UTILITY_BILLING_ACCOUNT_ID, containing  AWS 12 digit customer id needs to be provided.')
        billing_provider_params['billing_account_id'] = billing_account_id
    else:
        raise MissingRequiredEnvVar('Uknown METRICS_UTILITY_BILLING_PROVIDER env var, supported values are [aws].')

    if red_hat_org_id:
        billing_provider_params['red_hat_org_id'] = red_hat_org_id

    # only used for the other modes
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
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
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
    if report_type and report_type not in VALID_REPORT_TYPES:
        errors.append(
            f'Invalid METRICS_UTILITY_REPORT_TYPE: {report_type}. Valid values: {", ".join(VALID_REPORT_TYPES)}. '
            f'Please note these values are case sensitive'
        )

    if method == 'build' and report_type is None:
        errors.append(
            f'Invalid METRICS_UTILITY_REPORT_TYPE is Empty. Valid values: {", ".join(VALID_REPORT_TYPES)}. '
            f'Please note these values are case sensitive'
        )

    if method == 'gather' and report_type is not None:
        logger.warning('Ignoring METRICS_UTILITY_REPORT_TYPE used without build_report')

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
    ccsp_sheets = os.getenv(
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
        'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
    ).split(',')
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
        - The set of valid collectors is defined by the global variable
          VALID_COLLECTORS.
        - Error messages include the invalid collector names and the list of
          valid values.
    """
    collectors = os.environ.get('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').split(',')
    if collectors:
        invalid = set(collectors) - VALID_COLLECTORS
        if invalid:
            errors.append(f'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS: {", ".join(invalid)}. Valid values: {", ".join(VALID_COLLECTORS)}')


def validate_ship_target(errors, ship_target_type):
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
    ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
    if ship_target is None:
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET is Empty. Valid values: {", ".join(ship_target_type)}')
    if ship_target and ship_target not in ship_target_type:
        errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET: {ship_target}. Valid values: {", ".join(ship_target_type)}')
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
    no_path = 'No Path Provided'
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', no_path)
    dir_paths = VALID_SHIP_TARGET_BUILD
    if 's3' in dir_paths:
        dir_paths.remove('s3')
    if ship_target and ship_target in dir_paths and method == 'build':
        if not os.path.isdir(ship_path):
            errors.append(f'Invalid METRICS_UTILITY_SHIP_PATH: {ship_path} is not an existing directory.')
    if ship_path == no_path and method == 'gather' and ship_target == 'directory':
        logger.info('No path set under METRICS_UTILITY_SHIP_PATH. A directory will be created')


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

    Args:
        method (str): Determines which set of valid ship targets to use for validation.
            Should be either 'gather' or another supported method.

    Notes:
        - The function accumulates all errors before raising an exception, providing a comprehensive
          error message.
        - The specific validation functions (`validate_report_type`, `validate_ccsp_report_sheets`,
          `validate_collectors`, `validate_ship_target`, `validate_ship_path`) are expected to
          append error messages to the provided `errors` list.
        - The `method` parameter controls which ship target validation set is used.
        - Raises:
            MissingRequiredEnvVar: If any required environment variable or configuration is missing
            or invalid.
    """
    errors = []
    report_type = validate_report_type(errors, method)
    validate_collectors(errors)
    if method == 'build':
        validate_ccsp_report_sheets(errors, report_type)
        ship_target = validate_ship_target(errors, VALID_SHIP_TARGET_BUILD)
    else:
        ship_target = validate_ship_target(errors, VALID_SHIP_TARGET_GATHER)
    validate_ship_path(errors, ship_target, method)
    if errors:
        raise MissingRequiredEnvVar('\n'.join(errors))


def handle_not_crc():
    surplus = []

    if os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', None):
        surplus += ['METRICS_UTILITY_BILLING_ACCOUNT_ID']
    if os.getenv('METRICS_UTILITY_BILLING_PROVIDER', None):
        surplus += ['METRICS_UTILITY_BILLING_PROVIDER']
    if os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID', None):
        surplus += ['METRICS_UTILITY_RED_HAT_ORG_ID']

    if surplus:
        logger.warning(f'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="crc": {", ".join(surplus)}')


def now():
    return datetime.now(tz=timezone.utc)


def validate_build_params(options, HelpText):
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
    # report_type value validation in validate_report_type

    opt_month = options.get('month', None) or None
    opt_since = options.get('since', None) or None
    opt_until = options.get('until', None) or None
    opt_ephemeral = options.get('ephemeral', None) or None

    if opt_month:
        if opt_since or opt_until:
            raise BadParameter('The --since and --until parameters are not allowed if the --month parameter is provided.')

        if report_type == 'RENEWAL_GUIDANCE':
            raise BadParameter('The --month parameter is not allowed when METRICS_UTILITY_REPORT_TYPE="RENEWAL_GUIDANCE"')

    # has defaults if empty
    month = parse_month(opt_month)

    since = None
    if opt_since:
        since = parse_date_param(opt_since, help=HelpText.since)
        if since > now():
            logger.warning('The date for --since is in the future.')
    elif report_type == 'RENEWAL_GUIDANCE':
        raise MissingRequiredParameter(f'Missing --since parameter, required when METRICS_UTILITY_REPORT_TYPE="RENEWAL_GUIDANCE": {HelpText.since}')

    until = None
    if opt_until:
        if report_type == 'RENEWAL_GUIDANCE':
            raise BadParameter('The --until parameter is not allowed when METRICS_UTILITY_REPORT_TYPE="RENEWAL_GUIDANCE"')

        if not opt_since:
            raise BadParameter('--until is not allowed without --since.')

        until = parse_date_param(opt_until, help=HelpText.until)
        if until > now():
            logger.warning('The date for --until is in the future.')

        if since > until:
            raise UnparsableParameter('The date for --until cannot be before the date for --since.')

    ephemeral_days = None
    if opt_ephemeral:
        if report_type != 'RENEWAL_GUIDANCE':
            raise BadParameter(f'METRICS_UTILITY_REPORT_TYPE="{report_type}" does not allow --ephemeral.')

        ephemeral_days = parse_number_of_days(opt_ephemeral, help=HelpText.ephemeral)

    return {
        'month': month,
        'since': since,
        'until': until,
        'ephemeral_days': ephemeral_days,
    }


def validate_gather_params(options, HelpText):
    opt_since = options.get('since', None) or None
    opt_until = options.get('until', None) or None

    since = None
    if opt_since:
        since = parse_date_param(opt_since, help=HelpText.since)
        if since > now():
            logger.warning('The date for --since is in the future.')

    until = None
    if opt_until:
        until = parse_date_param(opt_until, help=HelpText.until)
        if until > now():
            logger.warning('The date for --until is in the future.')

    return since, until


# should also suport quarters, start/end of month ("4mo_ago_beginning" vs "1 mo ago end"), but doesn't yet
def parse_date_param(value, help=''):
    if not value:
        return None

    value = value.strip().lower()
    if value.isdigit():
        raise UnparsableParameter(f'Bare numbers are not valid ({help})')

    parsed_date = None

    # N days ago, start of day
    match = re.fullmatch(r'(\d+)\s*_*(d|da|day|days)(\s*_*ago)?', value)
    if match:
        days_ago = int(match.group(1))
        parsed_date = (now() - timedelta(days=days_ago - 1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # N months ago, start of day
    match = re.fullmatch(r'(\d+)\s*_*(mo|mon|mont|month|months)(\s*_*ago)?', value)
    if match:
        months_ago = int(match.group(1))
        parsed_date = (now() - relativedelta(months=months_ago)).replace(hour=0, minute=0, second=0, microsecond=0)

    # N minutes ago
    match = re.fullmatch(r'(\d+)\s*_*(m|mi|min|minu|minut|minute|minutes)(\s*_*ago)?', value)
    if match:
        minutes_ago = int(match.group(1))
        parsed_date = now() - timedelta(minutes=minutes_ago)

    # actual date
    if not parsed_date:
        parsed_date = parser.parse(value)

    # Add default UTC timezone
    if parsed_date and parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)

    return parsed_date


def parse_number_of_days(value, help=''):
    if not value:
        return None

    value = value.strip().lower()
    if value.isdigit():
        raise UnparsableParameter(f'Bare numbers are not valid ({help})')

    # N days ago
    match = re.fullmatch(r'(\d+)\s*_*(d|da|day|days)', value)
    if match:
        return int(match.group(1))

    # N months ago - using 30 days per month
    match = re.fullmatch(r'(\d+)\s*_*(m|mo|mon|mont|month|months)', value)
    if match:
        return int(match.group(1)) * 30

    raise UnparsableParameter(f"Can't parse parameter value {value} ({help})")


def parse_month(month):
    """Process month argument"""
    if month is not None:
        try:
            date = datetime.strptime(f'{month}', '%Y-%m')
        except ValueError:
            raise DateFormatError('Invalid --month format. Supported date format: YYYY-MM')
    else:
        """Return last month if no month was passed"""
        beginning_of_the_month = datetime.today().replace(day=1)
        beginning_of_the_previous_month = beginning_of_the_month - relativedelta(months=1)
        date = beginning_of_the_previous_month
        y = date.strftime('%Y')
        m = date.strftime('%m')
        month = f'{y}-{m}'

    return month, date, date + relativedelta(months=1)
