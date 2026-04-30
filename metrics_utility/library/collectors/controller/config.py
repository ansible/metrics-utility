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
    license_info = settings.get('LICENSE', {})

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
            'controller_version': _get_controller_version(db) or _version('awx'),
            'metrics_utility_version': _version('metrics-utility'),  # version from setup.cfg
            'platform': {
                'dist': distro.linux_distribution(),
                'release': platform.release(),
                'system': platform.system(),
                'type': _get_install_type(),
            },
        }
    )


def _version(package):
    """Return the installed version string for *package*, or None if not found.

    Args:
        package: PyPI package name.

    Returns:
        Version string or None.
    """
    try:
        return version(package)
    except PackageNotFoundError:
        return None


def _get_install_type():
    """Detect the deployment type from environment variables.

    Returns:
        ``'openshift'``, ``'k8s'``, or ``'traditional'``.
    """
    if os.getenv('container') == 'oci':
        return 'openshift'

    if os.getenv('KUBERNETES_SERVICE_PORT'):
        return 'k8s'

    return 'traditional'


def _get_controller_settings(db, keys):
    """Get controller settings from database using parameterized queries to prevent SQL injection."""
    settings = {}
    with db.cursor() as cursor:
        # Use parameterized query to prevent SQL injection
        placeholders = ', '.join(['%s'] * len(keys))
        cursor.execute(f'SELECT key, value FROM conf_setting WHERE key IN ({placeholders})', keys)
        for key, value in cursor.fetchall():
            if value:
                settings[key] = json.loads(value, object_hook=_datetime_hook)
    return settings


def _get_controller_version(db):
    """Get AWX/Controller version from the main_instance DB table."""
    sql = """
        SELECT version
        FROM main_instance
        WHERE enabled = true
            AND version IS NOT NULL
            AND version != ''
        ORDER BY last_seen DESC
        LIMIT 1
    """
    with db.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
    return None


def _datetime_hook(d):
    """JSON object hook that converts ISO 8601 strings to datetime objects.

    Args:
        d: Dict produced by the JSON decoder.

    Returns:
        Dict with string values that parse as datetimes replaced by
        :class:`datetime.datetime` instances.
    """
    new_d = {}
    for key, value in d.items():
        try:
            new_d[key] = datetime.fromisoformat(value)
        except (TypeError, ValueError):
            new_d[key] = value
    return new_d
