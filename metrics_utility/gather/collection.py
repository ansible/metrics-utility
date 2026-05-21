import copy
import io
import json
import os
import tarfile

from django.utils.timezone import now, timedelta

from metrics_utility.gather.utils import get_max_gather_period_days
from metrics_utility.library.collectors.util import CollectionOutput
from metrics_utility.logger import logger


class Collection:
    """Wrapper for gathering functions decorated with @register.

    Handles both JSON and CSV collection types via self.data_type.
    """

    TYPE_JSON = 'json'
    TYPE_CSV = 'csv'

    def __init__(self, collector, fnc_collecting):
        self.collector = collector
        self.fnc_collecting = fnc_collecting
        self.fnc_slicing = fnc_collecting.__insights_analytics_fnc_slicing__

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

        self.gather_kwargs = {}

        # JSON storage
        self.data = None

        # CSV storage
        self.sub_collections = []
        self.data_filepath = None

    def add_to_tar(self, tar):
        if self.data_type == 'json':
            buf = self.target().encode('utf-8')
            logger.debug(f'Collection.add_to_tar: | {self.key}.json | Size: {self.data_size()}')
            info = tarfile.TarInfo(f'./{self.filename}')
            info.size = len(buf)
            info.mtime = self.collector.gather_until.timestamp()
            tar.addfile(info, fileobj=io.BytesIO(buf))
        else:
            logger.debug(f'Collection.add_to_tar: | {self.key}.csv | Size: {self.data_size()}')
            tar.add(self.target(), arcname=f'./{self.filename}')

    def cleanup(self):
        if self.data_type == 'csv':
            if self.data_filepath and os.path.exists(self.data_filepath):
                os.remove(self.data_filepath)
            for collection in self.sub_collections:
                collection.cleanup()

    def data_size(self):
        if self.data_type == 'json':
            return len(self.data) if self.data else 0

        if self.data_filepath is None:
            return 0

        data_size = 0
        try:
            if os.path.exists(self.data_filepath):
                data_size = os.path.getsize(self.data_filepath)
        except OSError as e:
            logger.error(f"Can't get size of CSV file: {e}")

        return data_size

    def gather(self):
        self.gathering_started_at = now()

        output = CollectionOutput(self.collector.gather_dir)

        try:
            result = self.fnc_collecting(
                since=self.since,
                until=self.until,
                output=output,
                **self.gather_kwargs,
            )
            self._save_gathering(result)

            self.gathering_successful = True
        except Exception as e:
            logger.exception(f'Could not generate metric {self.filename}: {e}')
            self.gathering_successful = False
        finally:
            self._set_gathering_finished()

    def is_empty(self):
        if self.data_type == 'json':
            return self.data is None or self.data == 'null'

        if self.sub_collections:
            return all(c.is_empty() for c in self.sub_collections)
        return self.data_filepath is None

    def slices(self):
        since = self.collector.gather_since
        until = self.collector.gather_until
        last_gather = self.collector.last_gather
        if self.fnc_slicing:
            slices = self.fnc_slicing(self.key, last_gather, since=since, until=until)
        else:
            slices = [(self._gather_since(), self.collector.gather_until)]

        return slices

    def ship_immediately(self):
        return self.fnc_slicing is not None

    def target(self):
        if self.data_type == 'json':
            return self.data
        return self.data_filepath

    def update_last_gathered_entries(self, updates_dict):
        if self.data_type == 'csv' and self.sub_collections:
            for collection in self.sub_collections:
                collection.update_last_gathered_entries(updates_dict)
            return

        if self.key in updates_dict['locked']:
            return

        if self.gathering_successful:
            self._update_last_gathered_key(updates_dict, self.key, self.until)
        else:
            updates_dict['locked'].add(self.key)

    @staticmethod
    def _update_last_gathered_key(updates_dict, key, timestamp):
        previous = updates_dict['keys'].get(key, None)
        if previous is None:
            updates_dict['keys'][key] = timestamp
        else:
            updates_dict['keys'][key] = max(previous, timestamp)

    def _gather_since(self):
        last_entry = max(
            self.last_gathered_entry or self.collector.last_gather,
            self.collector.gather_until - timedelta(days=get_max_gather_period_days()),
        )
        return self.collector.gather_since or last_entry

    def _save_gathering(self, data):
        if self.data_type == 'json':
            self.data = json.dumps(data)
            return

        # CSV: handle multiple files via sub_collections
        if isinstance(data, list) and len(data) > 1:
            for fpath in data:
                sub_collection = copy.copy(self)
                sub_collection.sub_collections = []
                sub_collection.data_filepath = fpath
                sub_collection.gathering_successful = True
                self.sub_collections.append(sub_collection)
        elif isinstance(data, list) and len(data) == 1:
            self.data_filepath = data[0]
        elif isinstance(data, str):
            self.data_filepath = data

    def _set_gathering_finished(self):
        _now = now()
        self.gathering_finished_at = _now
        for sub_collection in self.sub_collections:
            sub_collection.gathering_finished_at = _now
