"""Validation helpers for metrics-utility management commands (gather and build)."""

import datetime
import json
import os
import re

from dateutil.relativedelta import relativedelta

from metrics_utility.base.utils import bool_from_env
from metrics_utility.exceptions import BadParameter, DateFormatError, MissingRequiredEnvVar, MissingRequiredParameter, UnparsableParameter
from metrics_utility.library.candlepin.client import CandlepinClient
from metrics_utility.library.candlepin.lifecycle import get_candlepin_ca, get_candlepin_url, get_renewal_days, run_candlepin_lifecycle
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
    'job_host_summary_service',
    'main_jobevent_service',
    'table_metadata',
    'unified_jobs',
}

VALID_SHIP_TARGET_BUILD = {'directory', 's3', 'controller_db'}
VALID_SHIP_TARGET_GATHER = {'directory', 's3', 'crc'}

# Keys in the conf_setting table where the Candlepin consumer identity cert is stored.
# Update these to match the actual row keys used by the AAP controller.
CANDLEPIN_CERT_SETTING_KEY = 'CANDLEPIN_CONSUMER_CERT'
CANDLEPIN_KEY_SETTING_KEY = 'CANDLEPIN_CONSUMER_KEY'
CANDLEPIN_UUID_SETTING_KEY = 'CANDLEPIN_CONSUMER_UUID'

# AWX conf_setting keys for the subscription credentials used for initial registration.
# These are set by AWX when the customer configures their Red Hat subscription.
SUBSCRIPTIONS_USERNAME_SETTING_KEY = 'SUBSCRIPTIONS_USERNAME'
SUBSCRIPTIONS_PASSWORD_SETTING_KEY = 'SUBSCRIPTIONS_PASSWORD'

# Placeholder UUID written by the AAP DB seed / migration before a real consumer is
# registered.  Treat it the same as an absent UUID so we never attempt a Candlepin
# lifecycle call with a non-functional consumer identity.
CANDLEPIN_UUID_PLACEHOLDER = '00000000-0000-0000-0000-000000000000'

ship_path_description = 'place for collected data and built reports'


def handle_directory_ship_target():
    """Read and validate METRICS_UTILITY_SHIP_PATH for directory mode.

    Returns:
        Dict with ``'ship_path'`` key.

    Raises:
        :exc:`~metrics_utility.exceptions.MissingRequiredEnvVar`: If the
            environment variable is not set.
    """
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')

    if not ship_path:
        raise MissingRequiredEnvVar(f'Missing required env variable METRICS_UTILITY_SHIP_PATH - {ship_path_description}')

    return {'ship_path': ship_path}


def handle_s3_ship_target():
    """Read and validate all S3-related environment variables.

    Returns:
        Dict with ``'ship_path'``, ``'bucket_name'``, ``'bucket_endpoint'``,
        ``'bucket_region'``, ``'bucket_access_key'``, and ``'bucket_secret_key'``.

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


def _fetch_candlepin_lifecycle_from_db():
    """Read cert PEM, key PEM, and consumer UUID from conf_setting in a single query.

    Returns (cert_pem, key_pem, consumer_uuid), any of which may be None if the
    corresponding row is absent or on any DB error.  Best-effort: failures are
    logged as warnings and never propagate.
    """
    all_keys = [CANDLEPIN_CERT_SETTING_KEY, CANDLEPIN_KEY_SETTING_KEY, CANDLEPIN_UUID_SETTING_KEY]
    try:
        from django.db import connection

        placeholders = ', '.join(['%s'] * len(all_keys))
        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT key, value FROM conf_setting WHERE key IN ({placeholders})',
                all_keys,
            )
            rows = {key: json.loads(value) for key, value in cursor.fetchall() if value}
        return (
            rows.get(CANDLEPIN_CERT_SETTING_KEY),
            rows.get(CANDLEPIN_KEY_SETTING_KEY),
            rows.get(CANDLEPIN_UUID_SETTING_KEY),
        )
    except Exception as e:
        logger.warning(f'Could not fetch Candlepin lifecycle data from DB: {e}')
        return None, None, None


def _save_candlepin_cert_to_db(cert_pem, key_pem):
    """Persist a renewed Candlepin identity cert and key back to conf_setting.

    Uses UPSERT so that rows are created if missing and updated if present.
    Best-effort: failures are logged as errors but never propagate.
    """
    try:
        from django.db import connection, transaction

        upsert_sql = """
            INSERT INTO conf_setting (created, modified, key, value)
            VALUES (NOW(), NOW(), %s, %s)
            ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value,
                    modified = NOW()
        """
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(upsert_sql, [CANDLEPIN_CERT_SETTING_KEY, json.dumps(cert_pem)])
                cursor.execute(upsert_sql, [CANDLEPIN_KEY_SETTING_KEY, json.dumps(key_pem)])
        logger.info('Renewed Candlepin cert and key saved to conf_setting.')
    except Exception as e:
        logger.error(f'Could not save renewed Candlepin cert to DB: {e}')


def _fetch_registration_credentials_from_db():
    """Read Candlepin registration credentials from AWX conf_setting.

    Reads SUBSCRIPTIONS_USERNAME, SUBSCRIPTIONS_PASSWORD (set by AWX when the
    customer configures their Red Hat subscription), LICENSE.account_number (org
    key for the Candlepin /consumers endpoint), and INSTALL_UUID (used as the
    consumer's aap.instance_uuid fact).

    Returns (username, password, org, install_uuid), any of which may be None
    if the corresponding row is absent or on any DB error.  Best-effort: failures
    are logged as warnings and never propagate.
    """
    keys = [SUBSCRIPTIONS_USERNAME_SETTING_KEY, SUBSCRIPTIONS_PASSWORD_SETTING_KEY, 'LICENSE', 'INSTALL_UUID']
    try:
        from django.db import connection

        placeholders = ', '.join(['%s'] * len(keys))
        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT key, value FROM conf_setting WHERE key IN ({placeholders})',
                keys,
            )
            rows = {key: json.loads(value) for key, value in cursor.fetchall() if value}

        license_data = rows.get('LICENSE', {})
        org = license_data.get('account_number') if isinstance(license_data, dict) else None
        return (
            rows.get(SUBSCRIPTIONS_USERNAME_SETTING_KEY),
            rows.get(SUBSCRIPTIONS_PASSWORD_SETTING_KEY),
            org,
            rows.get('INSTALL_UUID'),
        )
    except Exception as e:
        logger.warning(f'Could not fetch Candlepin registration credentials from DB: {e}')
        return None, None, None, None


def _save_candlepin_registration_to_db(cert_pem, key_pem, consumer_uuid):
    """Persist a new Candlepin consumer registration (cert, key, UUID) to conf_setting.

    Uses UPSERT so that rows are created on first registration and updated on
    subsequent calls.  Best-effort: failures are logged as errors but never propagate.
    """
    try:
        from django.db import connection, transaction

        upsert_sql = """
            INSERT INTO conf_setting (created, modified, key, value)
            VALUES (NOW(), NOW(), %s, %s)
            ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value,
                    modified = NOW()
        """
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute(upsert_sql, [CANDLEPIN_CERT_SETTING_KEY, json.dumps(cert_pem)])
                cursor.execute(upsert_sql, [CANDLEPIN_KEY_SETTING_KEY, json.dumps(key_pem)])
                cursor.execute(upsert_sql, [CANDLEPIN_UUID_SETTING_KEY, json.dumps(consumer_uuid)])
        logger.info(f'Candlepin consumer registration saved to conf_setting (uuid={consumer_uuid}).')
    except Exception as e:
        logger.error(f'Could not save Candlepin registration to DB: {e}')


def _register_candlepin_consumer():
    """Register a new Candlepin consumer using credentials from AWX conf_setting.

    Called from handle_crc_ship_target() when no identity cert exists in the DB
    and METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED is set.

    Reads SUBSCRIPTIONS_USERNAME / SUBSCRIPTIONS_PASSWORD and the org key from
    LICENSE.account_number, then calls POST /consumers on Candlepin to obtain an
    identity certificate.  On success the cert, key, and consumer UUID are
    persisted to conf_setting via UPSERT.

    Returns (cert_pem, key_pem, consumer_uuid) on success, (None, None, None) on
    any failure.  Best-effort: logs errors but never propagates.
    """
    username, password, org, install_uuid = _fetch_registration_credentials_from_db()

    if not username or not password:
        logger.warning(
            'Candlepin registration is enabled but SUBSCRIPTIONS_USERNAME / SUBSCRIPTIONS_PASSWORD '
            'are not set in conf_setting; skipping registration.'
        )
        return None, None, None

    if not org:
        logger.warning('Candlepin registration is enabled but LICENSE.account_number is not available; skipping registration.')
        return None, None, None

    candlepin_url = get_candlepin_url()
    candlepin_ca = get_candlepin_ca()
    proxy = os.getenv('METRICS_UTILITY_PROXY_URL')
    client = CandlepinClient(base_url=candlepin_url, candlepin_ca=candlepin_ca, proxy=proxy)

    try:
        cert_pem, key_pem, consumer_uuid = client.register_consumer(username, password, org, install_uuid)
    except Exception as e:
        logger.error(f'Candlepin consumer registration failed: {e}')
        return None, None, None

    _save_candlepin_registration_to_db(cert_pem, key_pem, consumer_uuid)
    return cert_pem, key_pem, consumer_uuid


def handle_crc_ship_target():
    """Read and validate CRC-related billing provider environment variables.

    Returns:
        Dict with ``'billing_provider'`` and optionally ``'billing_account_id'``
        and ``'red_hat_org_id'``.

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
        allowed = '", "'.join(['controller_db', 'directory', 's3'])
        logger.warning(f'Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="{allowed}"')

    # Attempt to load Candlepin mTLS credentials from the AAP DB (best-effort).
    cert_pem, key_pem, consumer_uuid = _fetch_candlepin_lifecycle_from_db()

    # If no cert exists yet, attempt initial registration when enabled.
    if (not cert_pem or not key_pem) and bool_from_env('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED'):
        cert_pem, key_pem, consumer_uuid = _register_candlepin_consumer()

    if cert_pem and key_pem:
        # Run lifecycle management (check-in + proactive renewal) when enabled.
        if bool_from_env('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED'):
            cert_pem, key_pem = _run_candlepin_lifecycle(cert_pem, key_pem, consumer_uuid)

        billing_provider_params['candlepin_cert_pem'] = cert_pem
        billing_provider_params['candlepin_key_pem'] = key_pem
        logger.info('Candlepin identity cert loaded from DB; mTLS will be attempted.')
    else:
        logger.info('No Candlepin identity cert found in DB; will use service account auth.')

    return billing_provider_params


def _run_candlepin_lifecycle(cert_pem, key_pem, consumer_uuid):
    """Orchestrate Candlepin check-in and proactive cert renewal.

    Called from handle_crc_ship_target() when
    METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED is set.  Returns the
    (possibly renewed) (cert_pem, key_pem) tuple.  If renewal fails, the
    original cert is returned so the caller can still attempt mTLS (which
    will then fall back to service-account auth via the existing SSLError
    handler in PackageCRC.ship()).
    """
    if not consumer_uuid or consumer_uuid == CANDLEPIN_UUID_PLACEHOLDER:
        logger.warning(
            'Candlepin lifecycle is enabled but CANDLEPIN_CONSUMER_UUID is not set in conf_setting '
            '(or still contains the placeholder value); '
            'skipping check-in and renewal (registration must be performed by the AAP platform).'
        )
        return cert_pem, key_pem

    candlepin_url = get_candlepin_url()
    renewal_days = get_renewal_days()
    candlepin_ca = get_candlepin_ca()
    proxy = os.getenv('METRICS_UTILITY_PROXY_URL')

    try:
        new_cert_pem, new_key_pem = run_candlepin_lifecycle(
            cert_pem,
            key_pem,
            consumer_uuid,
            candlepin_url=candlepin_url,
            renewal_days=renewal_days,
            candlepin_ca=candlepin_ca,
            proxy=proxy,
        )
    except Exception as e:
        logger.error(f'Candlepin lifecycle failed: {e}; proceeding with existing cert')
        return cert_pem, key_pem

    if new_cert_pem != cert_pem or new_key_pem != key_pem:
        _save_candlepin_cert_to_db(new_cert_pem, new_key_pem)

    return new_cert_pem, new_key_pem


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


def validate_ccsp_params(options):
    """Validate CLI option combinations specific to CCSP/CCSPv2 report types.

    Args:
        options: Dict of parsed CLI options.

    Raises:
        :exc:`~metrics_utility.exceptions.BadParameter`: On invalid combinations.
    """
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
    """Validate CLI option combinations specific to the RENEWAL_GUIDANCE report type.

    Args:
        options: Dict of parsed CLI options.
        help_texts: Dict mapping parameter names to their help strings.

    Raises:
        :exc:`~metrics_utility.exceptions.BadParameter`: On invalid combinations.
        :exc:`~metrics_utility.exceptions.MissingRequiredParameter`: If ``--since``
            is not provided.
        :exc:`~metrics_utility.exceptions.UnparsableParameter`: If ``--ephemeral``
            has an unsupported format.
    """
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
    """Parse ``--since`` and ``--until`` options and validate their ordering.

    Args:
        options: Dict of parsed CLI options.
        help_texts: Dict mapping parameter names to their help strings.

    Returns:
        Tuple of ``(since, until)`` as timezone-aware datetimes (either may be None).

    Raises:
        :exc:`~metrics_utility.exceptions.UnparsableParameter`: If ``--until`` is
            earlier than ``--since``.
    """
    opt_since = options.get('since', None)
    opt_until = options.get('until', None)

    since = parse_date_param(opt_since, help_texts, 'since')
    until = parse_date_param(opt_until, help_texts, 'until')

    if (opt_since and opt_until) and until < since:
        raise UnparsableParameter('The date for --until cannot be before the date for --since.')

    return since, until


def validate_build_params(options, help_texts):
    """Dispatch to the report-type-specific validation function and parse since/until.

    Args:
        options: Dict of parsed CLI options.
        help_texts: Dict mapping parameter names to their help strings.

    Returns:
        Tuple of ``(since, until)`` datetimes (either may be None).
    """
    report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
    if not report_type:
        return None, None

    if report_type in {'CCSP', 'CCSPv2'}:
        validate_ccsp_params(options)

    if report_type in {'RENEWAL_GUIDANCE'}:
        validate_renewal_params(options, help_texts)

    return parse_since_until(options, help_texts)


def parse_number_of_days(date_option):
    """Convert an ephemeral duration string (e.g. ``'3months'``, ``'5days'``) to an integer day count.

    Args:
        date_option: String with a numeric prefix and a unit suffix (``d``/``day``/``days``
            or ``mo``/``month``/``months``), or None/empty.

    Returns:
        Integer number of days, or None if *date_option* is empty.

    Raises:
        :exc:`~metrics_utility.exceptions.UnparsableParameter`: If the format is not recognised.
    """
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
