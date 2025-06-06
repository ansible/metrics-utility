import os

from metrics_utility.exceptions import MissingRequiredEnvVar


# Constants for valid values
VALID_REPORT_TYPES = {'CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE'}
VALID_SHEETS = {
    'CCSP': {
        'ccsp_summary', 'managed_nodes', 'indirectly_managed_nodes',
        'inventory_scope', 'usage_by_collections', 'usage_by_roles',
        'usage_by_modules'
    },
    'CCSPv2': {
        'ccsp_summary', 'jobs', 'managed_nodes',
        'indirectly_managed_nodes', 'inventory_scope',
        'usage_by_organizations', 'usage_by_collections',
        'usage_by_roles', 'usage_by_modules',
        'managed_nodes_by_organization', 'data_collection_status'
    }
}
VALID_COLLECTORS = {
    'main_host', 'main_jobevent', 'main_indirectmanagednodeaudit'
}
VALID_SHIP_PATHS = {'directory', 's3', 'crc'}


def handle_directory_ship_target(ship_target):
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)

    if not ship_path:
        raise MissingRequiredEnvVar('Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination for the generated data')

    return {'ship_path': ship_path}


def handle_s3_ship_target(ship_target):
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
    bucket_name = os.getenv('METRICS_UTILITY_BUCKET_NAME', None)
    bucket_endpoint = os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', None)
    bucket_region = os.getenv('METRICS_UTILITY_BUCKET_REGION', None)
    # Define S3 credentials
    bucket_access_key = os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY', None)
    bucket_secret_key = os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY', None)

    if not ship_path:
        raise MissingRequiredEnvVar('Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination for the generated data')

    if not bucket_name or not bucket_endpoint or not bucket_access_key or not bucket_secret_key:
        raise MissingRequiredEnvVar(
            'Missing one of required env variables for S3 configuration, namely: METRICS_UTILITY_BUCKET_NAME,'
            'METRICS_UTILITY_BUCKET_ENDPOINT, METRICS_UTILITY_BUCKET_ACCESS_KEY '
            'and METRICS_UTILITY_BUCKET_SECRET_KEY.'
        )

    return {
        'ship_path': ship_path,
        'bucket_name': bucket_name,
        'bucket_endpoint': bucket_endpoint,
        'bucket_region': bucket_region,
        'bucket_access_key': bucket_access_key,
        'bucket_secret_key': bucket_secret_key,
    }


def handle_crc_ship_target(ship_target):
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

    return billing_provider_params


def validate_report_type(errors):
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
    if report_type and report_type not in VALID_REPORT_TYPES:
        errors.append(
            f"Invalid METRICS_UTILITY_REPORT_TYPE: {report_type}. "
            f"Valid values: {', '.join(VALID_REPORT_TYPES)}"
        )
    return report_type


def validate_ccsp_report_sheets(errors, report_type):
    ccsp_sheets = os.getenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', None)
    if ccsp_sheets and report_type:
        ccsp_sheets_set = set(
            [s.strip() for s in ccsp_sheets.split(',') if s.strip()]
        )
        if report_type in VALID_SHEETS:
            invalid = ccsp_sheets_set - VALID_SHEETS[report_type]
            if invalid:
                errors.append(
                    f"Invalid METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS for "
                    f"{report_type}: {', '.join(invalid)}. Valid values: "
                    f"{', '.join(VALID_SHEETS[report_type])}"
                )


def validate_collectors(errors):
    collectors = os.getenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', None)
    if collectors:
        collectors_set = set(
            [c.strip() for c in collectors.split(',') if c.strip()]
        )
        invalid = collectors_set - VALID_COLLECTORS
        if invalid:
            errors.append(
                f"Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS: "
                f"{', '.join(invalid)}. Valid values: "
                f"{', '.join(VALID_COLLECTORS)}"
            )


def validate_ship_path(errors):
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
    if ship_path and ship_path not in VALID_SHIP_PATHS:
        errors.append(
            f"Invalid METRICS_UTILITY_SHIP_PATH: {ship_path}. Valid values: "
            f"{', '.join(VALID_SHIP_PATHS)}"
        )
    return ship_path


def validate_ship_target(errors, ship_path):
    ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
    if ship_path == 'directory' and ship_target:
        if not os.path.isdir(ship_target):
            errors.append(
                f"Invalid METRICS_UTILITY_SHIP_TARGET: {ship_target} "
                f"is not an existing directory."
            )


def handle_env_validation():
    errors = []
    report_type = validate_report_type(errors)
    validate_ccsp_report_sheets(errors, report_type)
    validate_collectors(errors)
    ship_path = validate_ship_path(errors)
    validate_ship_target(errors, ship_path)
    if errors:
        raise MissingRequiredEnvVar("\n".join(errors))

