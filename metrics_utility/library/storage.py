from contextlib import contextmanager

from .debug import indent, log


class StorageCRCMutual:
    def __init__(self, settings):
        log('library.storage StorageCRCMutual.__init__')
        self.settings = settings

    def ship(self, data):
        log('library.storage StorageCRCMutual.ship')
        return True


class StorageS3:
    def __init__(self, settings):
        log('library.storage StorageS3.__init__')
        self.settings = settings

    def ship(self, tarball):
        log('library.storage StorageS3.ship')
        return True

    def glob(self, glob, since=None, until=None):
        log('library.storage StorageS3.glob')
        return [f'fake-file-{i}.tar.gz' for i in range(3)]

    @contextmanager
    def get(self, remote):
        log('library.storage StorageS3.get')
        indent(1)
        yield f'/tmp/local-{remote}'
        indent(-1)
        log('/library.storage StorageS3.get')

    def put(self, path=None, data=None, file=None):
        log('library.storage StorageS3.put')
        return True

    def remove(self, files):
        log('library.storage StorageS3.remove')
        return True


class StorageCRC:
    def __init__(self, settings):
        log('library.storage StorageCRC.__init__')
        self.settings = settings

    def ship(self, data):
        log('library.storage StorageCRC.ship')
        return True


class StorageDirectory:
    def __init__(self, settings):
        log('library.storage StorageDirectory.__init__')
        self.settings = settings

    def ship(self, data):
        log('library.storage StorageDirectory.ship')
        return True

    def glob(self, glob, since=None, until=None):
        log('library.storage StorageDirectory.glob')
        return [f'fake-file-{i}.tar.gz' for i in range(3)]

    @contextmanager
    def get(self, remote):
        log('library.storage StorageDirectory.get')
        indent(1)
        yield f'/tmp/local-{remote}'
        indent(-1)
        log('/library.storage StorageDirectory.get')

    def put(self, path=None, data=None, file=None):
        log('library.storage StorageDirectory.put')
        return True

    def remove(self, files):
        log('library.storage StorageDirectory.remove')
        return True
