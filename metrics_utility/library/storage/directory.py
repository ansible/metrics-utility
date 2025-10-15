import glob
import os
import shutil

from contextlib import contextmanager

from .util import date_filter, dict_to_json_file


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
