import copy
import os

from django.utils.timezone import now

from .collection import Collection


class CollectionCSV(Collection):
    """Collection for CSV-outputting collecting functions (decorated by @register)
    Collecting functions can return 0+ paths to tmpfiles
    - result of gather() is stored in self.data_filepath
    - In case of multiple files, object clones itself to sub-collections,
      one for each file
    """

    def __init__(self, collector, fnc_collecting):
        super().__init__(collector, fnc_collecting)
        # Large db tables handled by fnc_collecting can be split to multiple files,
        # this is covered by sub_collections
        self.sub_collections = []
        self.data_filepath = None

    def add_to_tar(self, tar):
        """Adds CSV file to the tar(tgz) archive"""
        self.logger.debug(
            f"CollectionCSV._add_to_tar: | {self.key}.csv | Size: {self.data_size()}"
        )
        tar.add(self.target(), arcname=f"./{self.filename}")

    def cleanup(self):
        """Removes CSV files from /tmp"""
        if self.data_filepath and os.path.exists(self.data_filepath):
            os.remove(self.data_filepath)

        for collection in self.sub_collections:
            collection.cleanup()

    def data_size(self):
        """Gets size of tmp csv file. Sub-collections NOT computed."""
        if self.data_filepath is None:
            return 0

        data_size = 0
        try:
            if os.path.exists(self.data_filepath):
                data_size = os.path.getsize(self.data_filepath)
        except OSError as e:
            self.logger.error(f"Can't get size of CSV file: {e}")

        return data_size

    def is_empty(self):
        """
        Leaf checks if data are collected.
        Collection with sub-collections looks for any non-empty sub-collection
        """
        if len(self.sub_collections):
            for collection in self.sub_collections:
                if not collection.is_empty():
                    return False
            return True
        else:
            return self.data_filepath is None

    def target(self):
        """Data attribute specific for this data type"""
        return self.data_filepath

    def update_last_gathered_entries(self, updates_dict):
        """Get last successful gathering or ask sub-collections"""
        if len(self.sub_collections):
            for collection in self.sub_collections:
                collection.update_last_gathered_entries(updates_dict)
        else:
            super().update_last_gathered_entries(updates_dict)

    #
    # Private methods ---------------------------
    #
    def _save_gathering(self, data):
        """
        Saves data (paths to CSV files).
        If there are more files, sub-collections are created
        """
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
