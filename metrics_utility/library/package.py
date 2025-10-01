from contextlib import contextmanager

from .debug import indent, log


class PackageTarballs:
    def __init__(self, config=None, collectors=None, max_size=None, tarball_format=None, payload_format=None):
        log('library.package PackageTarballs.__init__')
        self.config = config
        self.collectors = collectors
        self.max_size = max_size
        self.tarball_format = tarball_format
        self.payload_format = payload_format
        self._done = False
        self._counter = 0

    def done(self):
        log('library.package PackageTarballs.done')
        self._counter += 1
        if self._counter >= 3:
            self._done = True
        return self._done

    @contextmanager
    def next(self):
        log('library.package PackageTarballs.next')
        indent(1)
        yield f'/tmp/fake-tarball-{self._counter}.tar.gz'
        indent(-1)
        log('/library.package PackageTarballs.next')
