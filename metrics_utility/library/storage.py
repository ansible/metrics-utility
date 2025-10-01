from contextlib import contextmanager


class StorageCRCMutual:
    def __init__(self, settings):
        print("library.storage StorageCRCMutual.__init__")
        self.settings = settings

    def ship(self, data):
        print("library.storage StorageCRCMutual.ship")
        return True


class StorageS3:
    def __init__(self, settings):
        print("library.storage StorageS3.__init__")
        self.settings = settings

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


class StorageCRC:
    def __init__(self, settings):
        print("library.storage StorageCRC.__init__")
        self.settings = settings

    def ship(self, data):
        print("library.storage StorageCRC.ship")
        return True


class StorageDirectory:
    def __init__(self, settings):
        print("library.storage StorageDirectory.__init__")
        self.settings = settings

    def ship(self, data):
        print("library.storage StorageDirectory.ship")
        return True

    def glob(self, glob, since=None, until=None):
        print("library.storage StorageDirectory.glob")
        return [f"fake-file-{i}.tar.gz" for i in range(3)]

    @contextmanager
    def get(self, remote):
        print("library.storage StorageDirectory.get")
        yield f"/tmp/local-{remote}"

    def put(self, path=None, data=None, file=None):
        print("library.storage StorageDirectory.put")
        return True

    def remove(self, files):
        print("library.storage StorageDirectory.remove")
        return True