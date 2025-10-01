from contextlib import contextmanager


class StorageCRCMutual:
    def __init__(self, settings):
        print("library.storage StorageCRCMutual.__init__")

    def ship(self, data):
        print("library.storage StorageCRCMutual.ship")
        return True


class StorageS3:
    def __init__(self, settings):
        print("library.storage StorageS3.__init__")

    def ship(self, tarball):
        print("library.storage StorageS3.ship")
        return True

    def glob(self, glob, since=None, until=None):
        print("library.storage StorageS3.glob")
        return [f"fake-file-{i}.tar.gz" for i in range(3)]

    @contextmanager
    def get(self, remote):
        print("library.storage StorageS3.get")
        yield f"/tmp/local-{remote}"

    def put(self, path=None, data=None, file=None):
        print("library.storage StorageS3.put")
        return True

    def remove(self, files):
        print("library.storage StorageS3.remove")
        return True