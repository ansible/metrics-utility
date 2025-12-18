import glob
import os
import shutil

from contextlib import contextmanager

from .util import date_filter, dict_to_json_file
from .helpers import load_csv, load_json, load_parquet


class StorageDirectory:
    def __init__(self, **settings):
        self.base_path = settings.get('base_path')

        if not self.base_path:
            raise Exception('StorageDirectory: base_path not set')

    # FIXME: used by ExtractorDirectory for now, replace with glob
    def list_files(self, relative_prefix):
        try:
            prefix = os.path.join(self.base_path, relative_prefix)
            return [os.path.join(prefix, f) for f in os.listdir(prefix) if os.path.isfile(os.path.join(prefix, f))]
        except FileNotFoundError:
            return []

    def glob(self, pattern, since=None, until=None):
        full_pattern = self._path(pattern)
        globbed = glob.glob(full_pattern)

        # Convert absolute paths back to relative paths (remove base_path prefix)
        relative_paths = [os.path.relpath(path, self.base_path) for path in globbed]

        if not since and not until:
            return relative_paths

        return [filename for filename in relative_paths if date_filter(filename, since, until)]

    @contextmanager
    def get(self, remote):
        yield self._path(remote)

    def get_data(self, remote, format='auto'):
        """
        Retrieve data from storage and return it parsed.

        Args:
            remote: Path to the file in storage
            format: Format of the data - 'auto' (detect from extension),
                   'json', 'csv', or 'parquet'

        Returns:
            For JSON: dict or list
            For CSV: list of dicts
            For Parquet: pandas DataFrame

        Raises:
            ValueError: If format is unsupported or cannot be auto-detected
            FileNotFoundError: If the file doesn't exist
        """
        if not self.exists(remote):
            raise FileNotFoundError(f"File not found in storage: {remote}")

        # Auto-detect format from file extension
        if format == 'auto':
            remote_lower = remote.lower()
            if remote_lower.endswith('.json'):
                format = 'json'
            elif remote_lower.endswith('.csv'):
                format = 'csv'
            elif remote_lower.endswith('.parquet'):
                format = 'parquet'
            else:
                raise ValueError(
                    f"Cannot auto-detect format for '{remote}'. "
                    f"Please specify format explicitly: 'json', 'csv', or 'parquet'"
                )

        # Load the data using the appropriate helper
        with self.get(remote) as filename:
            if format == 'json':
                return load_json(filename)
            elif format == 'csv':
                return load_csv(filename)
            elif format == 'parquet':
                return load_parquet(filename)
            else:
                raise ValueError(
                    f"Unsupported format: '{format}'. "
                    f"Supported formats: 'auto', 'json', 'csv', 'parquet'"
                )

    def put(self, remote, *, filename=None, fileobj=None, dict=None):
        full_path = self._path(remote)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        if filename:
            self._put_filename(full_path, filename)

        if fileobj:
            self._put_fileobj(full_path, fileobj)

        if dict:
            with dict_to_json_file(dict) as filename:
                self._put_filename(full_path, filename)

    def exists(self, remote):
        return os.path.exists(self._path(remote))

    def remove(self, remote):
        os.remove(self._path(remote))

    def _path(self, remote):
        return os.path.join(self.base_path, remote)

    def _put_filename(self, full_path, filename):
        shutil.copyfile(filename, full_path)

    def _put_fileobj(self, full_path, fileobj):
        with open(full_path, 'wb') as f:
            shutil.copyfileobj(fileobj, f)
