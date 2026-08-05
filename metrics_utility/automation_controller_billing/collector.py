"""Billing-specific Collector implementation for Automation Controller metrics."""

import json
import os

from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection

from metrics_utility import base
from metrics_utility.automation_controller_billing.helpers import get_last_entries_from_db
from metrics_utility.automation_controller_billing.package.factory import Factory as PackageFactory
from metrics_utility.base.utils import bool_from_env
from metrics_utility.library.lock import lock
from metrics_utility.logger import logger


class Collector(base.Collector):
    """Billing-specific collector that ships Automation Controller metrics data.

    Extends the base :class:`~metrics_utility.base.collector.Collector` with
    billing-provider configuration, an advisory-lock name that avoids conflicts
    with the upstream Analytics collector, and optional last-gathered-entry
    persistence suppression.
    """

    def __init__(self, collection_type=base.Collector.SCHEDULED_COLLECTION, collector_module=None, ship_target=None, billing_provider_params=None):
        """Initialise the billing collector.

        Args:
            collection_type: One of ``Collector.MANUAL_COLLECTION``,
                ``Collector.DRY_RUN``, or ``Collector.SCHEDULED_COLLECTION``
                (default).
            collector_module: Python module containing ``@register``-decorated
                collector functions.  Defaults to the bundled collectors module.
            ship_target: Shipping destination (``'crc'``, ``'directory'``, or
                ``'s3'``).
            billing_provider_params: Extra parameters forwarded to the package
                (e.g. S3 credentials, billing account ID).
        """
        if collector_module is None:
            from metrics_utility.automation_controller_billing import collectors

            collector_module = collectors

        self.ship_target = ship_target
        self.billing_provider_params = billing_provider_params

        super().__init__(collection_type=collection_type, collector_module=collector_module)

    # TODO: extract advisory lock name in the superclass and log message, so we can change it here and then use
    # this method from superclass
    # TODO: extract to superclass ability to push extra params into config.json
    # FIXME: subset is only used for tests, mock registered collectors instead?
    def gather(self, dest=None, subset=None, since=None, until=None, billing_provider_params=None):
        """Entry point for gathering

        :param dest: (default: /tmp/awx-analytics-*) - directory for temp files
        :param subset: (list) collector_module's function names if only subset is required (typically tests)
        :param since: (datetime) - low threshold of data changes (max. and default - 28 days ago)
        :param until: (datetime) - high threshold of data changes (defaults to now)
        :return: None or list of paths to tarballs (.tar.gz)
        """

        key = 'gather_automation_controller_billing_lock'
        suffix = os.getenv('METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX')
        if suffix:
            key = f'gather_automation_controller_billing_{suffix}_lock'

        with lock(key, wait=False, db=connection) as acquired:
            if not acquired:
                logger.log(self.log_level, 'Not gathering Automation Controller billing data, another task holds lock')
                return None

            self._gather_initialize(dest, subset, since, until)

            if not self._gather_config():
                return None

            self._gather_json_collections()

            self._gather_csv_collections()

            self._process_packages()

            self._gather_finalize()

            self._gather_cleanup()

            return self.all_tar_paths()

    def _gather_config(self):
        if not super()._gather_config():
            return False

        # Extend the config collection to contain billing specific info.
        # Strip mTLS key material — it is only needed at ship time and must never be
        # included in the payload archive that is uploaded to the remote ingress endpoint.
        _SENSITIVE_BILLING_KEYS = {'candlepin_cert_pem', 'candlepin_key_pem'}
        safe_billing_provider_params = {k: v for k, v in (self.billing_provider_params or {}).items() if k not in _SENSITIVE_BILLING_KEYS}
        config_collection = self.collections['config']
        data = json.loads(config_collection.data)
        data['billing_provider_params'] = safe_billing_provider_params
        config_collection._save_gathering(data)

        return True

    @classmethod
    def registered_collectors(cls, module=None):
        """Return a dict of all collectors registered in the billing collectors module.

        Args:
            module: Ignored; always uses the built-in billing collectors module.

        Returns:
            Dict mapping collector key names to ``{'name', 'version'}`` dicts.
        """
        from metrics_utility.automation_controller_billing import collectors

        return base.Collector.registered_collectors(collectors)

    def _load_last_gathered_entries(self):
        """Load the last-gathered timestamps from the Controller database.

        Reads ``AUTOMATION_ANALYTICS_LAST_ENTRIES`` from the ``conf_setting``
        table, sharing the same persistence mechanism as the upstream Analytics
        collector.

        Returns:
            Dict mapping collector keys to their last-gathered datetimes.
        """
        # We are reusing Settings used by Analytics, so we don't have to backport changes into analytics
        # We can safely do this, by making sure we use the same lock as Analytics, before we persist
        # these settings.
        return get_last_entries_from_db()

    def _gather_finalize(self):
        """Persisting timestamps (manual/schedule mode only)"""
        if not self.ship:
            return

        if bool_from_env('METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES'):
            return

        # We need to wait on analytics lock, to update the last collected timestamp settings
        # so we don't clash with analytics job collection.
        with lock('gather_analytics_lock', wait=True, db=connection):
            # We need to load fresh settings again as we're obtaning the lock, since
            # Analytics job could have changed this on the background and we'd be resetting
            # the Analytics values here.
            self._load_last_gathered_entries()
            self._update_last_gathered_entries()

    def _save_last_gathered_entries(self, last_gathered_entries):
        """Persist last-gathered timestamps to the Django settings object.

        Args:
            last_gathered_entries: Dict mapping collector keys to their latest
                successfully-gathered datetimes.
        """
        settings.AUTOMATION_ANALYTICS_LAST_ENTRIES = json.dumps(last_gathered_entries, cls=DjangoJSONEncoder)

    def _package_class(self):
        """Return the Package class appropriate for the configured ship target.

        Returns:
            A :class:`~metrics_utility.base.package.Package` subclass.
        """
        return PackageFactory(ship_target=self.ship_target).create()
