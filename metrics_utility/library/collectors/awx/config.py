"""Collector for the AWX analytics configuration snapshot."""

import json
import os
import platform

from datetime import datetime
from importlib.metadata import PackageNotFoundError, version

import distro

from ..util import DictOutput, collector


_SETTINGS = (
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
)


@collector
def config(*, db=None, output=DictOutput()):
    """Return a point-in-time snapshot of AWX configuration and licensing."""
    settings = _get_settings(db)
    license_info = settings.get('LICENSE')
    license_info = license_info if isinstance(license_info, dict) else {}

    return output.dict(
        {
            'platform': {
                'system': platform.system(),
                'dist': (distro.name(), distro.version(), distro.codename()),
                'release': platform.release(),
                'type': _get_install_type(),
            },
            'install_uuid': settings.get('INSTALL_UUID'),
            'instance_uuid': settings.get('SYSTEM_UUID'),
            'tower_url_base': settings.get('TOWER_URL_BASE'),
            'tower_version': _get_awx_version(db),
            'license_type': license_info.get('license_type', 'UNLICENSED'),
            'license_date': license_info.get('license_date'),
            'subscription_name': license_info.get('subscription_name'),
            'sku': license_info.get('sku'),
            'support_level': license_info.get('support_level'),
            'usage': license_info.get('usage'),
            'product_name': license_info.get('product_name'),
            'valid_key': license_info.get('valid_key'),
            'satellite': license_info.get('satellite'),
            'pool_id': license_info.get('pool_id'),
            'subscription_id': license_info.get('subscription_id'),
            'account_number': license_info.get('account_number'),
            'current_instances': license_info.get('current_instances'),
            'automated_instances': license_info.get('automated_instances'),
            'automated_since': license_info.get('automated_since'),
            'trial': license_info.get('trial'),
            'grace_period_remaining': license_info.get('grace_period_remaining'),
            'compliant': license_info.get('compliant'),
            'date_warning': license_info.get('date_warning'),
            'date_expired': license_info.get('date_expired'),
            'subscription_usage_model': settings.get('SUBSCRIPTION_USAGE_MODEL', ''),
            'free_instances': license_info.get('free_instances', 0),
            'total_licensed_instances': license_info.get('instance_count', 0),
            'license_expiry': license_info.get('time_remaining', 0),
            'pendo_tracking': settings.get('PENDO_TRACKING_STATE'),
            'authentication_backends': settings.get('AUTHENTICATION_BACKENDS'),
            'logging_aggregators': settings.get('LOG_AGGREGATOR_LOGGERS'),
            'external_logger_enabled': settings.get('LOG_AGGREGATOR_ENABLED'),
            'external_logger_type': settings.get('LOG_AGGREGATOR_TYPE'),
        }
    )


def _get_install_type():
    if os.getenv('container') == 'oci':
        return 'openshift'
    if os.getenv('KUBERNETES_SERVICE_PORT'):
        return 'k8s'
    return 'traditional'


def _get_settings(db):
    if db is None:
        return {}

    placeholders = ', '.join(['%s'] * len(_SETTINGS))
    settings = {}
    with db.cursor() as cursor:
        cursor.execute(f'SELECT key, value FROM conf_setting WHERE key IN ({placeholders})', _SETTINGS)
        for key, value in cursor.fetchall():
            if value:
                settings[key] = _decode(value)
    return settings


def _get_awx_version(db):
    if db is not None:
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

    try:
        return version('awx')
    except PackageNotFoundError:
        return None


def _decode(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value, object_hook=_datetime_hook)
    except (TypeError, ValueError):
        return value


def _datetime_hook(data):
    return {key: _as_datetime(value) for key, value in data.items()}


def _as_datetime(value):
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return value
