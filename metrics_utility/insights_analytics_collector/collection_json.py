import io
import json
import tarfile

from .collection import Collection


class CollectionJSON(Collection):
    """Collection for JSON-outputting collecting functions (decorated by @register)
    Collecting functions returns dict() convertable to JSON
    - result of gather() is stored in self.data
    """

    def __init__(self, collector, func):
        super().__init__(collector, func)
        self.data = None  # gathered data

    def _save_gathering(self, data):
        self.data = json.dumps(data)

    def data_size(self):
        return len(self.data) if self.data else 0

    def is_empty(self):
        return self.data is None

    def target(self):
        """Data attribute specific for this data type"""
        return self.data

    def add_to_tar(self, tar):
        """Adds JSON data to TAR(tgz) archive"""
        buf = self.target().encode("utf-8")
        self.logger.debug(
            f"CollectionJSON._add_to_tar: | {self.key}.json | Size: {self.data_size()}"
        )
        info = tarfile.TarInfo(f"./{self.filename}")
        info.size = len(buf)
        info.mtime = self.collector.gather_until.timestamp()
        tar.addfile(info, fileobj=io.BytesIO(buf))
