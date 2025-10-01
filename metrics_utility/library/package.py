from contextlib import contextmanager


class PackageTarballs:
    def __init__(self, config=None, collectors=None, max_size=None, tarball_format=None, payload_format=None):
        print("library.package PackageTarballs.__init__")
        self._done = False
        self._counter = 0

    def done(self):
        print("library.package PackageTarballs.done")
        self._counter += 1
        if self._counter >= 3:
            self._done = True
        return self._done

    @contextmanager
    def next(self):
        print("library.package PackageTarballs.next")
        yield f"/tmp/fake-tarball-{self._counter}.tar.gz"