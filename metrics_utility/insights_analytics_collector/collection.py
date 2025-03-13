from abc import abstractmethod

from django.utils.timezone import now, timedelta


class Collection:
    """Wrapper for gathering function from Collector.collector_module
    Functions decorated with @register are wrapped by kind of this object.
    """

    COLLECTION_TYPE_CONFIG = "config"
    COLLECTION_TYPE_JSON = "json"
    COLLECTION_TYPE_CSV = "csv"

    def __init__(self, collector, fnc_collecting):
        self.collector = collector
        self.logger = collector.logger
        self.fnc_collecting = fnc_collecting
        self.fnc_slicing = fnc_collecting.__insights_analytics_fnc_slicing__
        self.is_config = fnc_collecting.__insights_analytics_config__

        self.description = fnc_collecting.__insights_analytics_description__ or ""
        self.key = fnc_collecting.__insights_analytics_key__
        self.shipping_group = fnc_collecting.__insights_analytics_shipping_group__
        self.version = fnc_collecting.__insights_analytics_version__

        self.data_type = fnc_collecting.__insights_analytics_type__
        self.filename = f"{self.key}.{self.data_type}"
        # either since/until or full sync(if enabled)
        self.since = None  # set by Collector._create_collections()
        self.until = None  # set by Collector._create_collections()
        self.full_sync_enabled = self._is_full_sync_enabled(
            fnc_collecting.__insights_analytics_full_sync_interval_days__
        )

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

    def gather(self, max_data_size):
        self.gathering_started_at = now()

        try:
            # More collections with the same key (and different since/until)
            # have the same file names => overwriting! [error]
            result = self.fnc_collecting(
                since=self.since,
                until=self.until,
                max_data_size=max_data_size,
                full_path=self.collector.gather_dir,
                collection_type=self.collector.collection_type,
            )
            self._save_gathering(result)

            self.gathering_successful = True
        except Exception as e:
            self.logger.exception(f"Could not generate metric {self.filename}: {e}")
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
        # LAST_GATHER (truncated appropriately to match the 4-week limit).
        #
        # Or it can force full table sync if interval is given
        if self.fnc_slicing:
            if self.full_sync_enabled:
                slices = self.fnc_slicing(self.key, last_gather, full_sync_enabled=True)
            else:
                slices = self.fnc_slicing(
                    self.key, last_gather, since=since, until=until
                )
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
        if self.key in updates_dict["locked"]:
            return

        if self.gathering_successful:
            self._update_last_gathered_key(updates_dict, self.key, self.until)

            if self.full_sync_enabled:
                self._update_last_gathered_key(
                    updates_dict, f"{self.key}_full", self.gathering_finished_at
                )
        else:
            # collections are ordered by time slices.
            # in case of error all collections with newer timestamp are ignored
            updates_dict["locked"].add(self.key)

    #
    # Private methods ---------------------------
    #
    @staticmethod
    def _update_last_gathered_key(updates_dict, key, timestamp):
        previous = updates_dict["keys"].get(key, None)
        if previous is None:
            updates_dict["keys"][key] = timestamp
        else:
            updates_dict["keys"][key] = max(previous, timestamp)

    def _is_full_sync_enabled(self, interval_days):
        if not interval_days:
            return False

        last_full_sync = self.collector.last_gathered_entry_for(f"{self.key}_full")
        return not last_full_sync or last_full_sync < now() - timedelta(
            days=interval_days
        )

    def _gather_since(self):
        """Start of gathering based on settings excluding slices"""
        last_entry = max(
            self.last_gathered_entry or self.collector.last_gather,
            self.collector.gather_until - timedelta(weeks=4),
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
