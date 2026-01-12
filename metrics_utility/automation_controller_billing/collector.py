import json
import os

from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection

import metrics_utility.base as base

from metrics_utility.automation_controller_billing.helpers import get_last_entries_from_db
from metrics_utility.automation_controller_billing.package.factory import Factory as PackageFactory
from metrics_utility.library.lock import lock
from metrics_utility.logger import logger


class Collector(base.Collector):
    def __init__(self, collection_type=base.Collector.SCHEDULED_COLLECTION, collector_module=None, ship_target=None, billing_provider_params=None):
        if collector_module is None:
            from metrics_utility.automation_controller_billing import collectors

            collector_module = collectors

        self.ship_target = ship_target
        self.billing_provider_params = billing_provider_params

        super(Collector, self).__init__(collection_type=collection_type, collector_module=collector_module)

    # TODO: extract advisory lock name in the superclass and log message, so we can change it here and then use
    # this method from superclass
    # TODO: extract to superclass ability to push extra params into config.json
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

        # Extend the config collection to contain billing specific info:
        config_collection = self.collections['config']
        data = json.loads(config_collection.data)
        data['billing_provider_params'] = self.billing_provider_params
        config_collection._save_gathering(data)

        return True

    @staticmethod
    def db_connection():
        return connection

    @classmethod
    def registered_collectors(cls, module=None):
        from metrics_utility.automation_controller_billing import collectors

        return base.Collector.registered_collectors(collectors)

    def _load_last_gathered_entries(self):
        # We are reusing Settings used by Analytics, so we don't have to backport changes into analytics
        # We can safely do this, by making sure we use the same lock as Analytics, before we persist
        # these settings.
        return get_last_entries_from_db()

    def _gather_finalize(self):
        """Persisting timestamps (manual/schedule mode only)"""

        disabled_str = os.getenv('METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES', 'false')
        disabled = False
        if disabled_str and (disabled_str.lower() == 'true'):
            disabled = True

        if self.ship and not disabled:
            # We need to wait on analytics lock, to update the last collected timestamp settings
            # so we don't clash with analytics job collection.
            with lock('gather_analytics_lock', wait=True, db=connection):
                # We need to load fresh settings again as we're obtaning the lock, since
                # Analytics job could have changed this on the background and we'd be resetting
                # the Analytics values here.
                self._load_last_gathered_entries()
                self._update_last_gathered_entries()

    def _save_last_gathered_entries(self, last_gathered_entries):
        settings.AUTOMATION_ANALYTICS_LAST_ENTRIES = json.dumps(last_gathered_entries, cls=DjangoJSONEncoder)

    def _package_class(self):
        return PackageFactory(ship_target=self.ship_target).create()
