"""Validation helpers for metrics-utility management commands (gather and build)."""

import datetime
import os
import re

from dateutil.relativedelta import relativedelta

from metrics_utility.base.utils import bool_from_env, get_optional_ccsp_report_sheets, get_optional_collectors, get_report_type
from metrics_utility.exceptions import BadParameter, DateFormatError, MissingRequiredEnvVar, MissingRequiredParameter, UnparsableParameter
from metrics_utility.library.candlepin.client import CandlepinClient
from metrics_utility.library.candlepin.lifecycle import (
    get_candlepin_ca,
    get_candlepin_url,
    get_renewal_days,
    is_cert_valid,
    parse_cert,
    run_candlepin_lifecycle,
)
from metrics_utility.library.candlepin.store import DBCandlepinStore, LocalCandlepinStore, get_candlepin_store
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
        'infrastructure_summary',
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
    'main_hostmetric',
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

# AWX conf_setting keys for the subscription credentials used for DB-backed registration.
# AWX sets these when the customer configures their Red Hat subscription.
# Priority matches AWX PR #16388: REDHAT_USERNAME/PASSWORD first, then SUBSCRIPTIONS_*.
_REDHAT_USERNAME_SETTING_KEY = 'REDHAT_USERNAME'
_REDHAT_PASSWORD_SETTING_KEY = 'REDHAT_PASSWORD'
_SUBSCRIPTIONS_USERNAME_SETTING_KEY = 'SUBSCRIPTIONS_USERNAME'
_SUBSCRIPTIONS_PASSWORD_SETTING_KEY = 'SUBSCRIPTIONS_PASSWORD'

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
    if bool(bucket_access_key) != bool(bucket_secret_key):
        missing += [
            (
                'METRICS_UTILITY_BUCKET_ACCESS_KEY and METRICS_UTILITY_BUCKET_SECRET_KEY must both be set or both be omitted'
                ' (omit both to use implicit credentials such as IRSA or EC2 instance profiles)'
            )
        ]
    if not ship_path:
        missing += [f'METRICS_UTILITY_SHIP_PATH - {ship_path_description}']
    # bucket_region, bucket_access_key, bucket_secret_key are optional

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


def _fetch_registration_credentials_from_db():
    """Read Candlepin registration credentials from AWX conf_setting (DB-backend fallback).

    Credential priority matches AWX PR #16388:
      1. REDHAT_USERNAME / REDHAT_PASSWORD
      2. SUBSCRIPTIONS_USERNAME / SUBSCRIPTIONS_PASSWORD

    The org key is NOT read from the DB — it is discovered dynamically via
    CandlepinClient.discover_org() (GET /users/{username}/owners) so that customers
    do not need to know or configure their org key manually.

    Returns (username, password, install_uuid), any of which may be None.
    Best-effort: failures are logged as warnings and never propagate.
    """
    keys = [
        _REDHAT_USERNAME_SETTING_KEY,
        _REDHAT_PASSWORD_SETTING_KEY,
        _SUBSCRIPTIONS_USERNAME_SETTING_KEY,
        _SUBSCRIPTIONS_PASSWORD_SETTING_KEY,
        'INSTALL_UUID',
    ]
    try:
        from awx.conf.models import Setting
        from awx.main.utils.encryption import decrypt_field, is_encrypted

        rows = {}
        for setting in Setting.objects.filter(key__in=keys):
            value = setting.value
            if value is None:
                continue
            if is_encrypted(value):
                value = decrypt_field(setting, 'value')
            rows[setting.key] = value

        username = rows.get(_REDHAT_USERNAME_SETTING_KEY) or rows.get(_SUBSCRIPTIONS_USERNAME_SETTING_KEY)
        password = rows.get(_REDHAT_PASSWORD_SETTING_KEY) or rows.get(_SUBSCRIPTIONS_PASSWORD_SETTING_KEY)
        return username, password, rows.get('INSTALL_UUID')
    except Exception as e:
        logger.warning(f'Could not fetch Candlepin registration credentials from DB: {e}')
        return None, None, None


def _resolve_registration_credentials():
    """Resolve Candlepin registration credentials for standalone or DB-backed deployments.

    The org key is NOT returned here — it is discovered dynamically from the Candlepin
    API (GET /users/{username}/owners) inside _register_candlepin_consumer(), unless the
    operator has set METRICS_UTILITY_CANDLEPIN_ORG explicitly as an override.

    Credential priority:
      1. METRICS_UTILITY_RH_USERNAME / METRICS_UTILITY_RH_PASSWORD env vars
         (standalone, no database required).
      2. If storage=db and env vars are absent: AWX conf_setting
         (REDHAT_USERNAME/PASSWORD first, then SUBSCRIPTIONS_USERNAME/PASSWORD,
         matching the priority order in AWX PR #16388).

    Returns (username, password, install_uuid), any of which may be None.
    """
    username = os.getenv('METRICS_UTILITY_RH_USERNAME')
    password = os.getenv('METRICS_UTILITY_RH_PASSWORD')
    install_uuid = None

    storage = os.getenv('METRICS_UTILITY_CANDLEPIN_STORAGE', 'local').strip().lower()
    if (not username or not password) and storage == 'db':
        db_username, db_password, db_install_uuid = _fetch_registration_credentials_from_db()
        username = username or db_username
        password = password or db_password
        install_uuid = db_install_uuid

    return username, password, install_uuid


def _register_candlepin_consumer(store):
    """Register a new Candlepin consumer and persist the cert via *store*.

    Called from handle_crc_ship_target() when no cert is found and
    METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED is set.

    Resolves credentials from env vars (or DB fallback when storage=db), then
    discovers the org via GET /users/{username}/owners unless METRICS_UTILITY_CANDLEPIN_ORG
    is set as an explicit override.  Persists the result via store.save_registration().

    Returns (cert_pem, key_pem, consumer_uuid) on success, (None, None, None)
    on any failure.  Best-effort: logs errors but never propagates.
    """
    username, password, install_uuid = _resolve_registration_credentials()

    if not username or not password:
        logger.warning(
            'Candlepin registration is enabled but credentials are not configured. '
            'Set METRICS_UTILITY_RH_USERNAME / METRICS_UTILITY_RH_PASSWORD, '
            'or use METRICS_UTILITY_CANDLEPIN_STORAGE=db with REDHAT_USERNAME / REDHAT_PASSWORD '
            'or SUBSCRIPTIONS_USERNAME / SUBSCRIPTIONS_PASSWORD in conf_setting.'
        )
        return None, None, None

    candlepin_url = get_candlepin_url()
    candlepin_ca = get_candlepin_ca()
    proxy = os.getenv('METRICS_UTILITY_PROXY_URL')
    client = CandlepinClient(base_url=candlepin_url, candlepin_ca=candlepin_ca, proxy=proxy)

    # Org key: use explicit override if set, otherwise discover via Candlepin API.
    org = os.getenv('METRICS_UTILITY_CANDLEPIN_ORG') or client.discover_org(username, password)
    if not org:
        logger.warning(
            'Could not determine Candlepin org key. Set METRICS_UTILITY_CANDLEPIN_ORG, or ensure credentials have access to a Candlepin organisation.'
        )
        return None, None, None

    try:
        cert_pem, key_pem, consumer_uuid = client.register_consumer(username, password, org, install_uuid)
    except Exception as e:
        logger.error(f'Candlepin consumer registration failed: {e}')
        return None, None, None

    store.save_registration(cert_pem, key_pem, consumer_uuid)
    return cert_pem, key_pem, consumer_uuid


def _run_candlepin_lifecycle(cert_pem, key_pem, consumer_uuid, store):
    """Orchestrate Candlepin check-in and proactive cert renewal.

    Called from handle_crc_ship_target() when METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED
    is set.  Returns the (possibly renewed) (cert_pem, key_pem) tuple.  If renewal
    fails the original cert is returned so the caller can still attempt mTLS (which
    then falls back to service-account auth via the SSLError handler in PackageCRC.ship()).
    """
    if not consumer_uuid or consumer_uuid == CANDLEPIN_UUID_PLACEHOLDER:
        logger.warning(
            'Candlepin lifecycle is enabled but CANDLEPIN_CONSUMER_UUID is absent '
            '(or still contains the placeholder value); '
            'skipping check-in and renewal.'
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
        store.save_cert(new_cert_pem, new_key_pem)

    return new_cert_pem, new_key_pem


def _build_billing_provider_params():
    """Validate and build the billing provider parameters dict.

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

    return billing_provider_params


def _load_candlepin_cert(store):
    """Load the Candlepin cert from *store*, seeding or registering as needed.

    Returns:
        Tuple ``(cert_pem, key_pem, consumer_uuid)`` — any may be None.
    """
    cert_pem, key_pem, consumer_uuid = store.load()

    # If the local store is empty, try to seed it from the AWX Controller database.
    # The Controller registers with Candlepin and stores the resulting cert in
    # conf_setting; metrics-utility reads it from there and caches it locally so
    # subsequent runs work without any database connection.
    if not cert_pem and isinstance(store, LocalCandlepinStore):
        if not store.is_writable():
            logger.warning(
                f'Candlepin cert dir {store.cert_dir} is not writable by the current process; '
                'skipping cert-based auth and falling back to service account or basic auth. '
                'Set METRICS_UTILITY_CANDLEPIN_CERT_DIR to a writable path to enable mTLS.'
            )
            return None, None, None

        awx_cert, awx_key, awx_uuid = DBCandlepinStore().load()
        if awx_cert and awx_key:
            logger.info('Seeding local Candlepin cert from AWX conf_setting.')
            store.save_registration(awx_cert, awx_key, awx_uuid or '')
            cert_pem, key_pem, consumer_uuid = awx_cert, awx_key, awx_uuid

    # If no cert exists yet, attempt initial registration when enabled.
    if not (cert_pem and key_pem) and bool_from_env('METRICS_UTILITY_CANDLEPIN_REGISTRATION_ENABLED', default=False):
        cert_pem, key_pem, consumer_uuid = _register_candlepin_consumer(store)

    return cert_pem, key_pem, consumer_uuid


def _warn_if_cert_expiring(cert_pem):
    """Log a warning when the cert is within the renewal threshold of expiry."""
    try:
        info = parse_cert(cert_pem)
        threshold = get_renewal_days()
        if info['days_remaining'] < threshold:
            logger.warning(
                f'Candlepin cert expires in {info["days_remaining"]} day(s). Run candlepin_manage renew or ensure Controller cert renewal is active.'
            )
    except Exception:
        pass


def _load_cert_from_controller_db():
    """Try to load a valid Candlepin cert directly from the AWX Controller conf_setting table.

    The Controller (AWX PR #16388) manages its own Candlepin lifecycle — registration,
    check-in, and renewal. When a cert is present and valid in the Controller DB,
    metrics-utility should use it directly and skip its own lifecycle so the two
    systems do not run parallel check-ins or competing renewals.

    Returns:
        Tuple ``(cert_pem, key_pem, consumer_uuid)`` if a valid cert is found,
        or ``(None, None, None)`` if the DB is unavailable, empty, or the cert
        is expired / unparseable.
    """
    try:
        cert_pem, key_pem, consumer_uuid = DBCandlepinStore().load()
    except Exception as e:
        logger.debug(f'Controller DB cert lookup failed: {e}')
        return None, None, None

    if not cert_pem or not key_pem:
        return None, None, None

    if not is_cert_valid(cert_pem):
        logger.warning('Candlepin cert found in controller DB but it is expired or invalid; falling back to local store.')
        return None, None, None

    return cert_pem, key_pem, consumer_uuid


def handle_crc_ship_target():
    """Read and validate CRC-related billing provider environment variables.

    Returns:
        Dict with ``'billing_provider'`` and optionally ``'billing_account_id'``,
        ``'red_hat_org_id'``, ``'candlepin_cert_pem'``, and ``'candlepin_key_pem'``.

    Raises:
        :exc:`~metrics_utility.exceptions.MissingRequiredEnvVar`: If a required
            billing variable is missing or has an unsupported value.
    """
    billing_provider_params = _build_billing_provider_params()

    # only used for the other modes
    ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
    if ship_path:
        allowed = '", "'.join(['controller_db', 'directory', 's3'])
        logger.warning(f'Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="{allowed}"')

    # Master flag to disable all Candlepin functionality (cert loading, registration, lifecycle).
    # When disabled, metrics-utility will use service account auth only.
    if not bool_from_env('METRICS_UTILITY_CANDLEPIN_ENABLED', default=False):
        logger.info('Candlepin disabled; using service account auth only.')
        return billing_provider_params

    # Prefer the cert already managed by the AWX Controller (AWX PR #16388).
    # The Controller owns registration, check-in, and renewal; when its cert is
    # present and valid, use it directly and skip the metrics-utility lifecycle to
    # avoid duplicate check-ins or competing renewals.
    cert_pem, key_pem, consumer_uuid = _load_cert_from_controller_db()
    if cert_pem and key_pem:
        _warn_if_cert_expiring(cert_pem)
        billing_provider_params['candlepin_cert_pem'] = cert_pem
        billing_provider_params['candlepin_key_pem'] = key_pem
        logger.info('Candlepin identity cert loaded from controller DB; mTLS will be attempted.')
        return billing_provider_params

    # Fallback: use the configured local store with the metrics-utility lifecycle.
    # This path runs when the Controller DB is unavailable or has no cert (e.g.
    # standalone deployments without AWX PR #16388).
    store = get_candlepin_store()
    cert_pem, key_pem, consumer_uuid = _load_candlepin_cert(store)

    if cert_pem and key_pem:
        _warn_if_cert_expiring(cert_pem)
        if bool_from_env('METRICS_UTILITY_CANDLEPIN_LIFECYCLE_ENABLED', default=False):
            cert_pem, key_pem = _run_candlepin_lifecycle(cert_pem, key_pem, consumer_uuid, store)
        billing_provider_params['candlepin_cert_pem'] = cert_pem
        billing_provider_params['candlepin_key_pem'] = key_pem
        logger.info('Candlepin identity cert loaded; mTLS will be attempted.')
    else:
        logger.info('No Candlepin identity cert found; will use service account auth.')

    return billing_provider_params


def validate_report_type(errors, method):
    """
    Validates the 'METRICS_UTILITY_REPORT_TYPE' environment variable against a set of valid report types.

    If the environment variable is set and its value is not in the list of valid report types,
    an error message is appended to the provided errors list. If unset, it defaults to 'CCSPv2'.

    Args:
        errors (list): A list to which error messages will be appended if validation fails.

    Returns:
        str or None: The value of the 'METRICS_UTILITY_REPORT_TYPE' environment variable if set,
            'CCSPv2' if unset, or None for the 'gather' method.
    """
    if method == 'gather':
        return None

    report_type = get_report_type()
    if report_type not in VALID_REPORT_TYPES:
        errors.append(
            f'Invalid METRICS_UTILITY_REPORT_TYPE: {report_type}. Valid values: {", ".join(VALID_REPORT_TYPES)}. '
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
    ccsp_sheets = get_optional_ccsp_report_sheets(report_type)
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
            list of collector names. See get_optional_collectors() for the default.

    Notes:
        - The set of valid optional collectors is defined by the global variable VALID_COLLECTORS.
        - Error messages include the invalid collector names and the list ofvalid values.
    """

    collectors = get_optional_collectors()
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
            parsed = datetime.datetime.fromisoformat(value).astimezone(datetime.UTC)
    except Exception as e:
        raise UnparsableParameter(f'{str(e)}: {help_text}')

    # Set timezone to UTC when missing
    if parsed and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.UTC)

    return parsed


def validate_ccsp_params(options):
    """Validate CLI option combinations specific to CCSP/CCSPv2 report types.

    Args:
        options: Dict of parsed CLI options.

    Raises:
        :exc:`~metrics_utility.exceptions.BadParameter`: On invalid combinations.
    """
    report_type = get_report_type()
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
    report_type = get_report_type()

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

    if date_option.endswith(('d', 'day', 'days')):
        if date_option.endswith('d'):
            suffix_length = len('d')
        elif date_option.endswith('day'):
            suffix_length = len('day')
        elif date_option.endswith('days'):
            suffix_length = len('days')

        days = int(date_option[0:-suffix_length])
    elif date_option.endswith(('mo', 'month', 'months')):
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
