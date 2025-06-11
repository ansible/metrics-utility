import logging
import os
import re

from dateutil import parser

from metrics_utility.automation_controller_billing.helpers import (
    ALLOWED_EPHEMERAL_PATTERN,
    SINCE_AND_UNTIL_BUILD_PATTERN,
    SINCE_AND_UNTIL_GATHER_PATTERN,
    parse_date_param,
)
from metrics_utility.exceptions import BadParameter, MissingRequiredEnvVar, MissingRequiredParameter, UnparsableParameter


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


def handle_validate_date_param(param, help_text, command):
    exceptions = []

    if param is None:
        return

    if param.isdigit():
        """If the value is a digit stop execution and render the failure message."""
        exceptions.append('isdigit')
        help_text = 'Integers are not allowed for parameters --since and --until.'
        raise UnparsableParameter(help_text)

    try:
        """Try to parse the date, and if it fails go to next loop iteration.  Then, determine if we need to render the failure message."""
        parser.parse(param)
    except Exception:
        exceptions.append(help_text)

        """Try to parse the date, and if it fails go to next loop iteration.  Then, determine if we need to render the failure message."""
    if command == 'build':
        match = match_build_date_param_regex(param)
    elif command == 'gather':
        match = match_gather_date_param_regex(param)
    if match is None:
        exceptions.append(help_text)
    if len(exceptions) > 1:
        raise UnparsableParameter(help_text)


def match_build_date_param_regex(date):
    return re.match(SINCE_AND_UNTIL_BUILD_PATTERN, date)


def match_gather_date_param_regex(date):
    return re.match(SINCE_AND_UNTIL_GATHER_PATTERN, date)


def validate_build_extra_params(help_text, options):
    opt_month = options.get('month') or None
    # since = None
    until = options.get('until', None)
    since = options.get('since', None)
    since_help = help_text.get('since')
    until_help = help_text.get('until')
    # until = None
    handle_validate_ephemeral_param(options.get('ephemeral', None), help_text.get('ephemeral'))
    handle_validate_date_param(since, since_help, 'build')
    handle_validate_date_param(until, until_help, 'build')

    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
    if report_type is None:
        return

    until = parse_date_param(until)
    since = parse_date_param(since)

    has_since = since is not None
    has_until = until is not None

    validate_renewal_guidance_params(has_since, has_until, help_text)

    if (has_since and has_until) and until < since:
        raise UnparsableParameter('The date for --until cannot be before the date for --since.')

    if (has_since or has_until) and opt_month is not None:
        raise BadParameter('The --since and --until parameters are not allowed if the --month parameter is provided.')


def validate_renewal_guidance_params(since, until, help_text):
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
    is_renewal = report_type.startswith('RENEWAL_GUIDANCE')
    if not is_renewal:
        return

    since_help = help_text.get('since')
    until_help = help_text.get('until')
    if until:
        raise BadParameter('The --until parameter is not allowed when environment variable METRICS_UTILITY_REPORT_TYPE is RENEWAL_GUIDANCE')

    if since:
        raise MissingRequiredParameter(f"""{help_text.time_frame_extra_params} \n\n{since_help} \n{until_help} \n{help_text.month}""")


def handle_validate_ephemeral_param(value, help):
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
    if not value:
        return
    if not report_type.startswith('RENEWAL_GUIDANCE'):
        raise BadParameter(f'METRICS_UTILITY_REPORT_TYPE {report_type} does not allow --ephemeral.')
    if re.match(ALLOWED_EPHEMERAL_PATTERN, value):
        return
    raise UnparsableParameter(help)
