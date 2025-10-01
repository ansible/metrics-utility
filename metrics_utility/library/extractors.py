from .debug import log


class FakeCSV:
    def __init__(self, tarname):
        self.tarname = tarname


class ExtractorTarballs:
    def __init__(self):
        log('library.extractors ExtractorTarballs.__init__')

    def extract(self, local, only=None):
        log('library.extractors ExtractorTarballs.extract')
        if isinstance(only, str):
            yield FakeCSV(f'./{only}')
        elif isinstance(only, list):
            for filename in only:
                yield FakeCSV(f'./{filename}')
        else:
            yield FakeCSV('./default.csv')
