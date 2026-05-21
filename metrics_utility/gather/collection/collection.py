from abc import abstractmethod

from django.utils.timezone import now, timedelta

from metrics_utility.gather.utils import get_max_gather_period_days
from metrics_utility.library.collectors.util import CollectionOutput
from metrics_utility.logger import logger


class Collection:
    """Wrapper for gathering function from Collector.collector_module
    Functions decorated with @register are wrapped by kind of this object.
    """

    COLLECTION_TYPE_CONFIG = 'config'
    COLLECTION_TYPE_JSON = 'json'
    COLLECTION_TYPE_CSV = 'csv'

    def __init__(self, collector, fnc_collecting):
        self.collector = collector
        self.fnc_collecting = fnc_collecting
        self.fnc_slicing = fnc_collecting.__insights_analytics_fnc_slicing__
        self.is_config = fnc_collecting.__insights_analytics_config__

        self.key = fnc_collecting.__insights_analytics_key__
        self.version = fnc_collecting.__insights_analytics_version__

        self.data_type = fnc_collecting.__insights_analytics_type__

        self.filename = f'{self.key}.{self.data_type}'
        self.since = None  # set by Collector._create_collections()
        self.until = None  # set by Collector._create_collections()

        self.gathering_started_at = None
        self.gathering_finished_at = None
        self.gathering_successful = None
        self.last_gathered_entry = self.collector.last_gathered_entry_for(self.key)

    @abstractmethod
    def add_to_tar(self, tar):
        pass

    def cleanup(self):
        """There is an action only for CollectionCSV"""
        pass

    @abstractmethod
    def data_size(self):
        pass

    def gather(self):
        self.gathering_started_at = now()

        output = CollectionOutput(self.collector.gather_dir)

        try:
            # This runs a collector function registered with `@register`
            result = self.fnc_collecting(
                since=self.since,
                until=self.until,
                output=output,
            )
            self._save_gathering(result)

            self.gathering_successful = True
        except Exception as e:
            logger.exception(f'Could not generate metric {self.filename}: {e}')
            self.gathering_successful = False
        finally:
            self._set_gathering_finished()

    @abstractmethod
    def is_empty(self):
        pass

    def slices(self):
        since = self.collector.gather_since
        until = self.collector.gather_until
        last_gather = self.collector.last_gather
        # These slicer functions may return a generator. The `since` parameter is
        # allowed to be None, and will fall back to LAST_ENTRIES[key] or to
        # LAST_GATHER (truncated appropriately to match the 28-day limit).
        if self.fnc_slicing:
            slices = self.fnc_slicing(self.key, last_gather, since=since, until=until)
        else:
            slices = [(self._gather_since(), self._gather_until())]

        return slices

    def ship_immediately(self):
        """
        Collection with fnc_slicing has to be shipped immediately.
        It may gather to the same file(s) as previous slice.
        Keeping more files is not wanted because of their potential size
        """
        return self.fnc_slicing is not None

    @abstractmethod
    def target(self):
        """Data attribute specific for collection"""
        pass

    def update_last_gathered_entries(self, updates_dict):
        if self.key in updates_dict['locked']:
            return

        if self.gathering_successful:
            self._update_last_gathered_key(updates_dict, self.key, self.until)
        else:
            # collections are ordered by time slices.
            # in case of error all collections with newer timestamp are ignored
            updates_dict['locked'].add(self.key)

    @staticmethod
    def _update_last_gathered_key(updates_dict, key, timestamp):
        previous = updates_dict['keys'].get(key, None)
        if previous is None:
            updates_dict['keys'][key] = timestamp
        else:
            updates_dict['keys'][key] = max(previous, timestamp)

    def _gather_since(self):
        """Start of gathering based on settings excluding slices"""

        last_entry = max(
            self.last_gathered_entry or self.collector.last_gather,
            self.collector.gather_until - timedelta(days=get_max_gather_period_days()),
        )
        return self.collector.gather_since or last_entry

    def _gather_until(self):
        """End of gathering based on settings excluding slices"""
        return self.collector.gather_until

    @abstractmethod
    def _save_gathering(self, data):
        pass

    def _set_gathering_finished(self):
        self.gathering_finished_at = now()
