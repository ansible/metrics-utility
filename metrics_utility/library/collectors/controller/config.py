"""Collector that gathers Controller configuration and license information."""

import json
import os
import platform

from datetime import datetime
from importlib.metadata import PackageNotFoundError, version

import distro

from ..util import DictOutput, collector


# controller settings we collect
SETTINGS = [
    'AUTHENTICATION_BACKENDS',
    'INSTALL_UUID',
    'LICENSE',
    'LOG_AGGREGATOR_ENABLED',
    'LOG_AGGREGATOR_LOGGERS',
    'LOG_AGGREGATOR_TYPE',
    'PENDO_TRACKING_STATE',
    'SUBSCRIPTION_USAGE_MODEL',
    'SYSTEM_UUID',
    'TOWER_URL_BASE',
]


@collector
def config(*, db=None, billing_provider_params={}, output=DictOutput()):
    """Collect Controller configuration, license, and version information.

    Args:
        db: Django database connection used to read ``conf_setting``.
        billing_provider_params: Dict of billing-provider metadata included
            in the ``billing_provider_params`` key of the returned dict.
        output: Output adapter (defaults to :class:`~..util.DictOutput`).

    Returns:
        Dict containing settings, license info, version, and platform details.
    """
    settings = _get_controller_settings(db, keys=SETTINGS)
    license_info = _normalize_license(settings)
    controller_version = _get_controller_version(db) or _version('awx')

    return output.dict(
        {
            # settings
            'authentication_backends': settings.get('AUTHENTICATION_BACKENDS'),
            'controller_url_base': settings.get('TOWER_URL_BASE'),
            'external_logger_enabled': settings.get('LOG_AGGREGATOR_ENABLED'),
            'external_logger_type': settings.get('LOG_AGGREGATOR_TYPE'),
            'install_uuid': settings.get('INSTALL_UUID'),
            'instance_uuid': settings.get('SYSTEM_UUID'),
            'logging_aggregators': settings.get('LOG_AGGREGATOR_LOGGERS'),
            'pendo_tracking': settings.get('PENDO_TRACKING_STATE'),
            'subscription_usage_model': settings.get('SUBSCRIPTION_USAGE_MODEL'),
            # license
            'account_number': license_info.get('account_number'),
            'automated_instances': license_info.get('automated_instances'),
            'automated_since': license_info.get('automated_since'),
            'compliant': license_info.get('compliant'),
            'current_instances': license_info.get('current_instances'),
            'date_expired': license_info.get('date_expired'),
            'date_warning': license_info.get('date_warning'),
            'free_instances': license_info.get('free_instances', 0),
            'grace_period_remaining': license_info.get('grace_period_remaining'),
            'license_date': license_info.get('license_date'),
            'license_expiry': license_info.get('time_remaining', 0),
            'license_type': license_info.get('license_type', 'UNLICENSED'),
            'pool_id': license_info.get('pool_id'),
            'product_name': license_info.get('product_name'),
            'satellite': license_info.get('satellite'),
            'sku': license_info.get('sku'),
            'subscription_id': license_info.get('subscription_id'),
            'subscription_name': license_info.get('subscription_name'),
            'support_level': license_info.get('support_level'),
            'total_licensed_instances': license_info.get('instance_count', 0),
            'trial': license_info.get('trial'),
            'usage': license_info.get('usage'),
            'valid_key': license_info.get('valid_key'),
            # versions & config
            'billing_provider_params': billing_provider_params,
            'controller_version': controller_version,
            'metrics_utility_version': _version('metrics-utility'),  # version from setup.cfg
            'platform': {
                'dist': distro.linux_distribution(),
                'release': platform.release(),
                'system': platform.system(),
                'type': _get_install_type(),
            },
        }
    )


def _get_controller_settings(db, keys):
    """Get controller settings from database using parameterized queries to prevent SQL injection."""
    return _get_settings(db, keys)


def _get_install_type():
    """Detect the deployment type from environment variables."""
    if os.getenv('container') == 'oci':
        return 'openshift'
    if os.getenv('KUBERNETES_SERVICE_PORT'):
        return 'k8s'
    return 'traditional'


def _get_controller_version(db):
    """Get AWX/Controller version from the main_instance DB table."""
    if db is None:
        return None

    query = """
        SELECT version
        FROM main_instance
        WHERE enabled = true AND version IS NOT NULL AND version != ''
        ORDER BY last_seen DESC
        LIMIT 1
    """
    with db.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        if row and row[0]:
            return row[0]
    return None


def _datetime_hook(data):
    """Convert ISO 8601 string values in a decoded JSON object to datetimes."""
    return {key: _as_datetime(value) for key, value in data.items()}


def _get_settings(db, keys):
    """Return decoded values for the requested ``conf_setting`` keys."""
    if db is None:
        return {}

    placeholders = ', '.join(['%s'] * len(keys))
    settings = {}
    with db.cursor() as cursor:
        cursor.execute(f'SELECT key, value FROM conf_setting WHERE key IN ({placeholders})', keys)
        for key, value in cursor.fetchall():
            if value:
                settings[key] = _decode(value)
    return settings


def _decode(value):
    """Decode a setting value, preserving values that are not JSON."""
    if not isinstance(value, str):
        return value

    try:
        return json.loads(value, object_hook=_datetime_hook)
    except (TypeError, ValueError):
        return value


def _as_datetime(value):
    """Convert one ISO 8601 string to a datetime, if possible."""
    if not isinstance(value, str):
        return value

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value


def _normalize_license(settings):
    """Return license data as a mapping, or an empty mapping."""
    license_info = settings.get('LICENSE')
    return license_info if isinstance(license_info, dict) else {}


def _version(package):
    """Return an installed package version, or ``None`` if unavailable."""
    try:
        return version(package)
    except PackageNotFoundError:
        return None
