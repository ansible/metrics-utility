import contextlib
import json
import logging
from django.conf import settings
from django.db import connection

import insights_analytics_collector as base

from django.core.serializers.json import DjangoJSONEncoder
# from awx.conf.license import get_license
# from awx.main.models import Job
# from awx.main.access import access_registry
# from rest_framework.exceptions import PermissionDenied
from metrics_utility.automation_controller_billing.package.factory import Factory as PackageFactory

from awx.main.utils import datetime_hook
from awx.main.utils.pglock import advisory_lock

logger = logging.getLogger('metrics_utility.collector')


class Collector(base.Collector):
    def __init__(self, collection_type=base.Collector.SCHEDULED_COLLECTION, collector_module=None,
                 ship_target=None, billing_provider_params=None):
        from metrics_utility.automation_controller_billing import collectors

        if collector_module is None:
            collector_module = collectors

        self.ship_target = ship_target
        self.billing_provider_params = billing_provider_params

        super(Collector, self).__init__(collection_type=collection_type, collector_module=collector_module, logger=logger)

    # TODO: extract advisory lock name in the superclass and log message, so we can change it here and then use
    # this method from superclass
    # TODO: extract to superclass ability to push extra params into config.json
    def gather(self, dest=None, subset=None, since=None, until=None, billing_provider_params=None):
        """Entry point for gathering

        :param dest: (default: /tmp/awx-analytics-*) - directory for temp files
        :param subset: (list) collector_module's function names if only subset is required (typically tests)
        :param since: (datetime) - low threshold of data changes (max. and default - 4 weeks ago)
        :param until: (datetime) - high threshold of data changes (defaults to now)
        :return: None or list of paths to tarballs (.tar.gz)
        """
        if not self.is_enabled():
            return None

        with self._pg_advisory_lock("gather_automation_controller_billing_lock", wait=False) as acquired:
            if not acquired:
                self.logger.log(
                    self.log_level, "Not gathering Automation Controller billing data, another task holds lock"
                )
                return None

            self._gather_initialize(dest, subset, since, until)

            if not self._gather_config():
                return None

            self._gather_json_collections()
            # Extend the config collection to contain billing specific info:
            config_collection = self.collections['config']
            data = json.loads(config_collection.data)
            data['billing_provider_params'] = billing_provider_params
            config_collection._save_gathering(data)
            # End of extension

            self._gather_csv_collections()

            self._process_packages()

            self._gather_finalize()

            self._gather_cleanup()

            return self.all_tar_paths()

    def _is_valid_license(self):
        # TODO: which license to check? Any license will do?
        #
        # try:
        #     if get_license().get('license_type', 'UNLICENSED') == 'open':
        #         return False
        #     access_registry[Job](None).check_license()
        # except PermissionDenied:
        #     logger.exception("A valid license was not found:")
        #     return False
        return True

    def _is_shipping_configured(self):
        if self.is_shipping_enabled():
            if self.ship_target == "crc":
                # TODO: should this be enable only with certain SKUs? or the check will be higher above
                # when integrating to Controller?

                if not (settings.AUTOMATION_ANALYTICS_URL and settings.REDHAT_USERNAME and settings.REDHAT_PASSWORD):
                    logger.log(self.log_level, "Not gathering Automation Controller billing data, configuration "\
                                            "is invalid. Set 'Red Hat customer username/password' under "\
                                            "'Miscellaneous System' settings in your Automation Controller. Or use "\
                                            "--dry-run to gather locally without sending.")
                    return False
            elif self.ship_target == "directory":
                # TODO add checks here
                pass

        return True

    @staticmethod
    def db_connection():
        return connection

    @classmethod
    def registered_collectors(cls, module=None):
        from metrics_utility.automation_controller_billing import collectors

        return base.Collector.registered_collectors(collectors)

    @contextlib.contextmanager
    def _pg_advisory_lock(self, key, wait=False):
        """Use awx specific implementation to pass tests with sqlite3"""
        with advisory_lock(key, wait=wait) as lock:
            yield lock

    def _last_gathering(self):
        # Not needed in this implementation, but we need to define an abstract method
        pass

    def _load_last_gathered_entries(self):
        # We are reusing Settings used by Analytics, so we don't have to backport changes into analytics
        # We can safely do this, by making sure we use the same lock as Analytics, before we persist
        # these settings.
        from awx.conf.models import Setting

        last_entries = Setting.objects.filter(key='AUTOMATION_ANALYTICS_LAST_ENTRIES').first()
        last_gathered_entries = json.loads((last_entries.value if last_entries is not None else '') or '{}', object_hook=datetime_hook)
        return last_gathered_entries

    def _gather_finalize(self):
        """Persisting timestamps (manual/schedule mode only)"""
        if self.is_shipping_enabled():
            # We need to wait on analytics lock, to update the last collected timestamp settings
            # so we don't clash with analytics job collection.
            with self._pg_advisory_lock("gather_analytics_lock", wait=True) as acquired:
                # We need to load fresh settings again as we're obtaning the lock, since
                # Analytics job could have changed this on the background and we'd be resetting
                # the Analytics values here.
                self._load_last_gathered_entries()
                self._update_last_gathered_entries()

    def _save_last_gathered_entries(self, last_gathered_entries):
        settings.AUTOMATION_ANALYTICS_LAST_ENTRIES = json.dumps(last_gathered_entries, cls=DjangoJSONEncoder)

    def _package_class(self):
        return PackageFactory(ship_target=self.ship_target).create()
