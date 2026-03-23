import platform

import distro

from ..util import DictOutput, collector
from .config import _get_install_type, _version


@collector
def config_django(*, billing_provider_params={}, output=DictOutput()):
    from awx.conf.license import get_license
    from awx.main.utils import get_awx_version
    from django.conf import settings

    license_info = get_license()

    return output.dict(
        {
            # settings
            'authentication_backends': settings.AUTHENTICATION_BACKENDS,
            'controller_url_base': settings.TOWER_URL_BASE,
            'external_logger_enabled': settings.LOG_AGGREGATOR_ENABLED,
            'external_logger_type': getattr(settings, 'LOG_AGGREGATOR_TYPE', None),
            'install_uuid': settings.INSTALL_UUID,
            'instance_uuid': settings.SYSTEM_UUID,
            'logging_aggregators': settings.LOG_AGGREGATOR_LOGGERS,
            'pendo_tracking': settings.PENDO_TRACKING_STATE,
            'subscription_usage_model': getattr(settings, 'SUBSCRIPTION_USAGE_MODEL', ''),
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
            'controller_version': get_awx_version(),
            'metrics_utility_version': _version('metrics-utility'),
            'platform': {
                'dist': distro.linux_distribution(),
                'release': platform.release(),
                'system': platform.system(),
                'type': _get_install_type(),
            },
        }
    )
