"""Stub tarball-package implementation used by the library layer in tests."""

from contextlib import contextmanager

from .debug import indent, log


class PackageTarballs:
    """Stub tarball package that simulates multi-tarball packaging for testing."""

    def __init__(self, config=None, collectors=None, max_size=None, tarball_format=None, payload_format=None):
        """Initialise the stub package.

        Args:
            config: Optional configuration dict (not used in stub).
            collectors: Optional list of collector instances (not used in stub).
            max_size: Maximum tarball size (not used in stub).
            tarball_format: Tarball naming format (not used in stub).
            payload_format: Payload content-type format (not used in stub).
        """
        log('library.package PackageTarballs.__init__')
        self.config = config
        self.collectors = collectors
        self.max_size = max_size
        self.tarball_format = tarball_format
        self.payload_format = payload_format
        self._done = False
        self._counter = 0

    def done(self):
        """Return True after three calls, simulating a multi-package scenario.

        Returns:
            bool indicating whether packaging is complete.
        """
        log('library.package PackageTarballs.done')
        self._counter += 1
        if self._counter >= 3:
            self._done = True
        return self._done

    @contextmanager
    def next(self):
        """Yield a fake tarball path for the next package in the sequence."""
        log('library.package PackageTarballs.next')
        indent(1)
        yield f'/tmp/fake-tarball-{self._counter}.tar.gz'
        indent(-1)
        log('/library.package PackageTarballs.next')
