import inspect
import logging
import os
import pathlib
import shutil
import tempfile

from abc import abstractmethod

from django.db import connection
from django.utils.timezone import now, timedelta

from metrics_utility.library.lock import lock
from metrics_utility.logger import logger

from .collection import Collection
from .collection_csv import CollectionCSV
from .collection_json import CollectionJSON
from .package import Package
from .utils import get_max_gather_period_days, get_optional_collectors


class Collector:
    """Abstract class. The Collector is an entry-point for gathering data
       from awx to cloud.
    Abstract and following methods has to be implemented:
    - _package_class() - reference to your implementation of Package

    There are several params:
    - collection_type:
      - manual/scheduled - data are gathered and shipped, local timestamps about gathering are updated
      - dry-run - data are gathered, but not shipped, tarballs from /tmp not deleted (testing mode)
    - collector_module: module with functions with decorator `@register` - they define what data are collected
      - collector functions are wrapped by kind of Collection object
      - Collections are grouped by Package, and Packages are creating tarballs and shipping them.

    Collector is an abstract class, example of implementation is in tests/classes

    Data are gathered maximally 28 days ago and can be set to less (see gather(since, until,..))
    """

    MANUAL_COLLECTION = 'manual'
    DRY_RUN = 'dry-run'
    SCHEDULED_COLLECTION = 'scheduled'

    def __init__(self, collection_type=DRY_RUN, collector_module=None):
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
    # Class methods -----------------------------
    #
    @classmethod
    def registered_collectors(cls, module):
        """
        Returns all functions in 'module' defined with "@register" decorator
        """
        return {
            func.__insights_analytics_key__: {
                'name': func.__insights_analytics_key__,
                'version': func.__insights_analytics_version__,
                'description': func.__insights_analytics_description__ or '',
            }
            for name, func in inspect.getmembers(module)
            if inspect.isfunction(func) and hasattr(func, '__insights_analytics_key__')
        }

    #
    # Public methods ----------------------------
    #
    def config_present(self):
        """
        Checks if collector_module contains 'config' method (required)
        :return: bool
        """

        return self.collections.get('config') is not None

    @staticmethod
    @abstractmethod
    def db_connection():
        """
        DB connection for advisory lock. Can be
        - django.db.connection or
        - sqlalchemy.engine.base.Engine.raw_connection()
        - etc.
        """
        pass

    def gather(self, dest=None, subset=None, since=None, until=None):
        """Entry point for gathering

        :param dest: (default: /tmp/awx-analytics-*) - directory for temp files
        :param subset: (list) collector_module's function names if only subset is required (typically tests)
        :param since: (datetime) - low threshold of data changes (max. and default - 28 days ago)
        :param until: (datetime) - high threshold of data changes (defaults to now)
        :return: None or list of paths to tarballs (.tar.gz)
        """
        with lock('gather_analytics_lock', wait=False, db=connection) as acquired:
            if not acquired:
                logger.log(self.log_level, 'Not gathering analytics, another task holds lock')
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

    def last_gathered_entry_for(self, key):
        return self.last_gathered_entries.get(key)

    def all_tar_paths(self):
        tar_paths = []
        for _, packages in self.packages.items():
            new_paths = [package.tar_path for package in packages if package.tar_path is not None]
            tar_paths += new_paths
        return tar_paths or []

    def delete_tarballs(self):
        for path in self.all_tar_paths():
            os.remove(path)

    #
    # Private methods ---------------------------
    #
    def _calculate_collection_interval(self, since, until):
        _now = now()
        original_since = since
        original_until = until
        logger.warning(f'Original since-until: {original_since} to {original_until}')

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
                if until > since + timedelta(days=get_max_gather_period_days()):
                    until = since + timedelta(days=get_max_gather_period_days())
                    logger.warning(
                        f'End of the collection interval is greater than {get_max_gather_period_days()} days from start, setting end to {until}.'
                    )
            else:  # until is None
                until = min(since + timedelta(days=get_max_gather_period_days()), _now)
        elif until is None:
            until = _now

        # ensure since = until is valid and will not collect any data with timestamps
        if since and since > until:
            logger.warning('Start of the collection interval is later than the end, ignoring request.')
            raise ValueError

        # The ultimate beginning of the interval needs to be compared to 28 days prior to
        # `until`, but we want to keep `since` empty if it wasn't passed in because we use that
        # case to know whether to use the bookkeeping settings variables to decide the start of
        # the interval.
        horizon = until - timedelta(days=get_max_gather_period_days())
        if since is not None and since < horizon:
            since = horizon
            logger.warning(
                f'Start of the collection interval is more than {get_max_gather_period_days()} days prior to {until}, setting to {horizon}.'
            )

        last_gather = horizon
        if last_gather < horizon:
            last_gather = horizon
            logger.warning(f'Last analytics run was more than {get_max_gather_period_days()} days prior to {until}, using {horizon} instead.')

        self.gather_since = since
        self.gather_until = until
        self.last_gather = last_gather

        logger.warning(f'Final since-until: {since} to {until}')

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
        self._init_tmp_dir(tmp_root_dir)

        self.last_gathered_entries = self._load_last_gathered_entries()

        self._calculate_collection_interval(since, until)

        self._reset_collections_and_packages()

        self._create_collections(collectors_subset)

    def _gather_config(self):
        """Config is special collection, it's added to each Package
        TODO: add "always" flag to @register decorator
        """
        if not self.config_present():
            logger.log(self.log_level, "'config' collector data is missing")
            return False
        else:
            self.collections['config'].gather(self._package_class().max_data_size())
            return True

    def _gather_json_collections(self):
        """JSON collections are simpler, they're just gathered and added to the Package"""
        for collection in self.collections[Collection.COLLECTION_TYPE_JSON]:
            collection.gather(self._package_class().max_data_size())

            if collection.is_empty() or not collection.gathering_successful:
                continue

            self._add_collection_to_package(collection)

    def _gather_csv_collections(self):
        """CSV collections can contain sub-collections (big db tables).
        In that case they are shipped immediately, because:
         1) the temp file needs to be deleted to ensure enough disk space
         2) Collections with slicing function can produce duplicate filename
        """

        last_key = None

        disable_job_host_summary_str = os.getenv('METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR', 'false')
        disable_job_host_summary = disable_job_host_summary_str.lower() == 'true'

        optional_collectors = get_optional_collectors()

        for collection in self.collections[Collection.COLLECTION_TYPE_CSV]:
            if last_key != collection.key:
                write_enabled = False

                if collection.key == 'job_host_summary' and not disable_job_host_summary:
                    write_enabled = True

                if collection.key in optional_collectors:
                    write_enabled = True

                if write_enabled:
                    logger.warning(f'Progress info: Now gathering {collection.key}')
                else:
                    logger.warning(f'Progress info: Skipping {collection.key} because it is not enabled.')

                last_key = collection.key

            collection.gather(self._package_class().max_data_size())

            if collection.is_empty() or not collection.gathering_successful:
                continue

            # If collection has sub_collections (it means it collected more files)
            # ship them in their own package
            if len(collection.sub_collections):
                for sub_collection in collection.sub_collections:
                    self._add_collection_to_package(sub_collection)
            else:
                self._add_collection_to_package(collection)

    def _add_collection_to_package(self, collection):
        """Adds collection to package and ships it if collection has slicing"""
        package = self._find_available_package(collection.shipping_group, collection.key, collection.data_size())
        package.add_collection(collection)
        if collection.ship_immediately():
            self._process_package(package)

    def _process_packages(self):
        for group, packages in self.packages.items():
            for package in packages:
                self._process_package(package)

    def _process_package(self, package):
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
                package.ship()
            package.delete_collected_files()
            package.processed = True

    def _gather_finalize(self):
        """Persisting timestamps (manual/schedule mode only)"""
        if not self.ship:
            return

        self._update_last_gathered_entries()

    def _gather_cleanup(self):
        """Deleting temp files"""
        shutil.rmtree(self.tmp_dir, ignore_errors=True)  # clean up individual artifact files
        if self.ship:
            self.delete_tarballs()

    def _init_tmp_dir(self, tmp_root_dir=None):
        self.tmp_dir = pathlib.Path(tmp_root_dir or tempfile.mkdtemp(prefix='awx_analytics-'))
        self.gather_dir = self.tmp_dir.joinpath('stage')
        self.gather_dir.mkdir(mode=0o700)

    @abstractmethod
    def _load_last_gathered_entries(self):
        """Loads persisted timestamps named by keys from collector_module
        Complement to the _save_last_gathered_entries()
        :return dict
        """
        pass

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

        self._save_last_gathered_entries(self.last_gathered_entries)

    @abstractmethod
    def _save_last_gathered_entries(self, last_gathered_entries):
        """Saves dictionary with timestamps to persistent storage
        Complement to the _load_last_gathered_entries()
        :param last_gathered_entries: dict
        """
        pass

    def _create_collections(self, subset=None):
        """Creates Collections from decorated functions (by @register) from self.collector_module
        :param subset - array of function names which should be used.
                      - if None, all registered functions will be used

        collections have following structure:
        {
            'json': []
            'csv': []
            'config': <Collection>
        }
        """
        for name, fnc in inspect.getmembers(self.collector_module):
            if (
                inspect.isfunction(fnc)  # noqa
                and hasattr(fnc, '__insights_analytics_key__')  # noqa
                and hasattr(fnc, '__insights_analytics_type__')  # noqa
                and (not subset or name in subset)  # noqa
            ):
                # Create collection by type
                collection = self._create_collection(fnc)

                if collection.is_config:
                    # It's supposed there is only one registered config
                    self.collections[Collection.COLLECTION_TYPE_CONFIG] = collection
                else:
                    for since, until in collection.slices():
                        collection.since = since
                        collection.until = until
                        self.collections[collection.data_type].append(collection)
                        collection = self._create_collection(fnc)

    def _create_collection(self, fnc_collecting):
        data_type = fnc_collecting.__insights_analytics_type__
        collection = None
        if data_type == 'json':
            collection = CollectionJSON(self, fnc_collecting)
        elif data_type == 'csv':
            collection = CollectionCSV(self, fnc_collecting)

        if collection is None:
            raise RuntimeError(f'Collection of type {data_type} not implemented')

        return collection

    def _create_package(self):
        package_class = self._package_class()
        return package_class(self)

    @staticmethod
    def _package_class():
        """Has to be redefined by your Package implementation"""
        return Package

    def _reset_collections_and_packages(self):
        self.collections = {
            Collection.COLLECTION_TYPE_JSON: [],
            Collection.COLLECTION_TYPE_CSV: [],
            Collection.COLLECTION_TYPE_CONFIG: None,
        }
        self.packages = {}
