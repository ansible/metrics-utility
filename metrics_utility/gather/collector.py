import inspect
import json
import logging
import os
import pathlib
import shutil
import tempfile

from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.utils.timezone import now, timedelta

from metrics_utility.gather.collection import Collection
from metrics_utility.gather.decorators import register
from metrics_utility.gather.package.package import Package
from metrics_utility.gather.utils import bool_from_env, get_last_entries_from_db, get_max_gather_period_days
from metrics_utility.library.collectors.controller.config import config
from metrics_utility.library.collectors.controller.config_django import config_django
from metrics_utility.library.lock import lock
from metrics_utility.logger import logger


@register('config', '2.0')
def cli_config(output, billing_provider_params=None, **_kwargs):
    try:
        collector = config_django(billing_provider_params=billing_provider_params or {})
        return output.as_dict(collector)
    except Exception:
        logger.info('config_django unavailable, falling back to DB-based config collector')
        collector = config(db=connection, billing_provider_params=billing_provider_params or {})
        return output.as_dict(collector)


class Collector:
    """Collector is an entry-point for gathering Automation Controller billing data.

    There are several params:
    - collection_type:
      - manual/scheduled - data are gathered and shipped, local timestamps about gathering are updated
      - dry-run - data are gathered, but not shipped, tarballs from /tmp not deleted (testing mode)
    - collector_module: module with functions with decorator `@register` - they define what data are collected
      - collector functions are wrapped by kind of Collection object
      - Collections are grouped by Package, and Packages are creating tarballs and shipping them.

    Data are gathered maximally 28 days ago and can be set to less (see gather(since, until,..))
    """

    MANUAL_COLLECTION = 'manual'
    DRY_RUN = 'dry-run'
    SCHEDULED_COLLECTION = 'scheduled'

    def __init__(self, collection_type=SCHEDULED_COLLECTION, collector_module=None, ship_target=None):
        if collector_module is None:
            from metrics_utility.gather import collectors

            collector_module = collectors

        self.ship_target = ship_target

        self.collector_module = collector_module
        self.collections = {}
        self.packages = {}

        self.last_gathered_entries = None
        self.log_level = logging.ERROR if collection_type != self.SCHEDULED_COLLECTION else logging.DEBUG
        self.ship = collection_type != self.DRY_RUN  # shipping is enabled in manual/scheduled mode

        self.tmp_dir = None
        self.gather_dir = None
        self.gather_since = None
        self.gather_until = None
        self.last_gather = None

    #
    # Public methods ----------------------------
    #
    def gather(self, dest=None, subset=None, since=None, until=None, billing_provider_params=None, ship_params=None):
        """Entry point for gathering

        :param dest: (default: /tmp/awx-analytics-*) - directory for temp files
        :param subset: (list) collector_module's function names if only subset is required (typically tests)
        :param since: (datetime) - low threshold of data changes (max. and default - 28 days ago)
        :param until: (datetime) - high threshold of data changes (defaults to now)
        :param billing_provider_params: (dict) billing provider metadata for config.json
        :param ship_params: (dict) shipping credentials (ship_path, S3 bucket params)
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

            if not self._gather_config(billing_provider_params):
                return None

            self._gather_json_collections(ship_params)

            self._gather_csv_collections(ship_params)

            self._process_packages(ship_params)

            self._gather_finalize()

            self._gather_cleanup()

            return self.all_tar_paths()

    def last_gathered_entry_for(self, key):
        return self.last_gathered_entries.get(key)

    def all_tar_paths(self):
        tar_paths = []
        for _, packages in self.packages.items():
            new_paths = [package.tar_path for package in packages if package.tar_path is not None]
            tar_paths += new_paths
        return tar_paths or []

    #
    # Private methods ---------------------------
    #
    def _calculate_collection_interval(self, since, until):
        _now = now()
        _max = get_max_gather_period_days()
        _timedelta = timedelta(days=_max)

        original_since = since
        original_until = until
        logger.info(f'Original since-until: {original_since} to {original_until}')

        # Make sure that the endpoints are not in the future.
        if until is not None and until > _now:
            until = _now
            logger.warning(f'End of the collection interval is in the future, setting to {_now}.')
        if since is not None and since > _now:
            since = _now
            logger.warning(f'Start of the collection interval is in the future, setting to {_now}.')

        # The value of `until` needs to be concrete, so resolve it.  If it wasn't passed in,
        # set it to `now`, but only if that isn't more than 28 days ahead of a passed-in
        # `since` parameter.
        if since is not None:
            if until is not None:
                if until > since + _timedelta:
                    until = since + _timedelta
                    logger.warning(f'End of the collection interval is greater than {_max} days from start, setting end to {until}.')
            else:  # until is None
                until = min(since + _timedelta, _now)
                logger.info(f'End of the collection interval set to {until}.')
        elif until is None:
            until = _now
            logger.info(f'End of the collection interval set to {until}.')

        # ensure since = until is valid and will not collect any data with timestamps
        if since and since > until:
            logger.warning('Start of the collection interval is later than the end, ignoring request.')
            raise ValueError

        # The ultimate beginning of the interval needs to be compared to 28 days prior to
        # `until`, but we want to keep `since` empty if it wasn't passed in because we use that
        # case to know whether to use the bookkeeping settings variables to decide the start of
        # the interval.
        horizon = until - _timedelta
        if since is not None and since < horizon:
            since = horizon
            logger.warning(f'Start of the collection interval is more than {_max} days prior to {until}, setting to {horizon}.')

        self.gather_since = since
        self.gather_until = until
        self.last_gather = horizon

        logger.info(f'Final since-until: {since or horizon} to {until}')

    def _find_available_package(self, group, key, requested_size=None):
        """Checks if there is a Package available for collection.
        Package can't contain collection with the same key and has to have enough free space

        :param group: finds or creates package for group strategy if not None
        :param requested_size: returns existing package, if there is enough free size

        :return: Package
        """
        available_package = None

        for package in self.packages.get(group) or []:
            if package.has_free_space(requested_size) and not package.is_key_used(key) and not package.processed:
                available_package = package
                break

        if available_package is None:
            available_package = self._create_package()
            self.packages[group] = self.packages.get(group) or []
            self.packages[group].append(available_package)

        return available_package

    def _gather_initialize(self, tmp_root_dir, collectors_subset, since, until):
        self.tmp_dir = pathlib.Path(tmp_root_dir or tempfile.mkdtemp(prefix='awx_analytics-'))
        self.gather_dir = self.tmp_dir.joinpath('stage')
        self.gather_dir.mkdir(mode=0o700)

        self.last_gathered_entries = self._load_last_gathered_entries()

        self._calculate_collection_interval(since, until)

        self.collections = {
            'json': [],
            'csv': [],
        }
        self.config_collection = None
        self.packages = {}

        self._create_collections(collectors_subset)

    def _gather_config(self, billing_provider_params):
        """Config is special collection, it's added to each Package"""
        if self.config_collection is None:
            logger.log(self.log_level, "'config' collector data is missing")
            return False

        if billing_provider_params:
            self.config_collection.gather_kwargs = {
                'billing_provider_params': billing_provider_params,
            }

        self.config_collection.gather()
        return True

    def _gather_json_collections(self, ship_params=None):
        """JSON collections are simpler, they're just gathered and added to the Package"""
        for collection in self.collections['json']:
            collection.gather()

            if collection.is_empty() or not collection.gathering_successful:
                continue

            self._add_collection_to_package(collection, ship_params)

    def _gather_csv_collections(self, ship_params=None):
        """CSV collections can contain sub-collections (big db tables).
        In that case they are shipped immediately, because:
         1) the temp file needs to be deleted to ensure enough disk space
         2) Collections with slicing function can produce duplicate filename
        """

        last_key = None
        logged_status = set()

        for collection in self.collections['csv']:
            if last_key != collection.key:
                logger.warning(f'Progress info: Now gathering {collection.key}')
                last_key = collection.key

            collection.gather()

            if collection.disabled:
                if collection.key not in logged_status:
                    logger.warning(f'Progress info: Disabled {collection.key}')
                    logged_status.add(collection.key)
                continue

            if not collection.gathering_successful:
                if collection.key not in logged_status:
                    logger.warning(f'Progress info: Failed {collection.key}')
                    logged_status.add(collection.key)
                continue

            if collection.is_empty():
                if collection.key not in logged_status:
                    logger.warning(f'Progress info: No data for {collection.key}')
                    logged_status.add(collection.key)
                continue

            # If collection has sub_collections (it means it collected more files)
            # ship them in their own package
            if len(collection.sub_collections):
                for sub_collection in collection.sub_collections:
                    self._add_collection_to_package(sub_collection, ship_params)
            else:
                self._add_collection_to_package(collection, ship_params)

    def _add_collection_to_package(self, collection, ship_params=None):
        """Adds collection to package and ships it if collection has slicing"""
        package = self._find_available_package('default', collection.key, collection.data_size())
        package.add_collection(collection)
        if collection.ship_immediately():
            self._process_package(package, ship_params)

    def _process_packages(self, ship_params=None):
        for group, packages in self.packages.items():
            for package in packages:
                self._process_package(package, ship_params)

    def _process_package(self, package, ship_params=None):
        """
        Processing of package can be called twice, skipping the 2nd call.
        If there is a custom slicing function,
        package has to be sent immediately after gathering data
        :see Collection.ship_immediately()

        :param package: Package
        """
        if not package.processed:
            package.make_tgz()
            if self.ship:
                package.ship(ship_params)
            package.delete_collected_files()
            package.processed = True

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

    def _gather_cleanup(self):
        """Deleting temp files"""
        shutil.rmtree(self.tmp_dir, ignore_errors=True)  # clean up individual artifact files
        if self.ship:
            for path in self.all_tar_paths():
                os.remove(path)

    def _load_last_gathered_entries(self):
        """Load the last-gathered timestamps from the Controller database.

        Reads AUTOMATION_ANALYTICS_LAST_ENTRIES from the conf_setting table,
        sharing the same persistence mechanism as the upstream Analytics collector.
        """
        # We are reusing Settings used by Analytics, so we don't have to backport changes into analytics
        # We can safely do this, by making sure we use the same lock as Analytics, before we persist
        # these settings.
        return get_last_entries_from_db()

    def _update_last_gathered_entries(self):
        last_gathered_updates = {'keys': {}, 'locked': set()}

        for _, packages in self.packages.items():
            for package in packages:
                package.update_last_gathered_entries(last_gathered_updates)

        # Locked key means that gathering wasn't successful at least once.
        # Full sync timestamp can't be updated (if present)
        for unsuccessful_key in last_gathered_updates['locked']:
            last_gathered_updates.pop(f'{unsuccessful_key}_full', None)

        self.last_gathered_entries.update(last_gathered_updates['keys'])

        settings.AUTOMATION_ANALYTICS_LAST_ENTRIES = json.dumps(self.last_gathered_entries, cls=DjangoJSONEncoder)

    def _create_collections(self, subset=None):
        """Creates Collections from decorated functions (by @register) from self.collector_module
        :param subset - array of function names which should be used.
                      - if None, all registered functions will be used
        """
        module_has_config = False

        for name, fnc in inspect.getmembers(self.collector_module):
            if not (
                inspect.isfunction(fnc)  # noqa
                and hasattr(fnc, '_register_key_')  # noqa
                and hasattr(fnc, '_register_output_format_')  # noqa
            ):
                continue

            if fnc._register_key_ == 'config':
                module_has_config = True
                if not subset or name in subset:
                    self.config_collection = self._create_collection(fnc)
                continue

            if subset and name not in subset:
                continue

            collection = self._create_collection(fnc)

            for since, until in collection.slices():
                collection.since = since
                collection.until = until
                self.collections[collection.output_format].append(collection)
                collection = self._create_collection(fnc)

        if not module_has_config:
            self.config_collection = self._create_collection(cli_config)

    def _create_collection(self, collector_fn):
        return Collection(self, collector_fn)

    def _create_package(self):
        return Package(self)
