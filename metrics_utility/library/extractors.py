"""Stub extractors used in testing scenarios."""

from .debug import log


class FakeCSV:
    """Minimal CSV-like stub that records a tarball name, used in test extractors."""

    def __init__(self, tarname):
        """Initialise with the tarball name.

        Args:
            tarname: Path string stored on ``self.tarname``.
        """
        self.tarname = tarname


class ExtractorTarballs:
    """Stub extractor that yields :class:`FakeCSV` objects for testing purposes."""

    def __init__(self):
        log('library.extractors ExtractorTarballs.__init__')

    def extract(self, _local, only=None):
        """Yield fake CSV objects based on *only*.

        Args:
            _local: Unused local path parameter.
            only: A filename string, list of filenames, or None (yields a default).

        Yields:
            :class:`FakeCSV` instances.
        """
        log('library.extractors ExtractorTarballs.extract')
        if isinstance(only, str):
            yield FakeCSV(f'./{only}')
        elif isinstance(only, list):
            for filename in only:
                yield FakeCSV(f'./{filename}')
        else:
            yield FakeCSV('./default.csv')
