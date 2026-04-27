"""Manifest collection that records the version of every file in a tarball."""

from .collection_json import CollectionJSON
from .decorators import register


class CollectionManifest(CollectionJSON):
    """JSON collection that tracks the filename→version mapping for a Package tarball."""

    def __init__(self, collector):
        """Create the manifest for *collector*'s current gathering window.

        Args:
            collector: The active :class:`~metrics_utility.base.collector.Collector` instance.
        """
        super().__init__(collector, self.collecting)

        self.data = {}
        self.since = collector.gather_since
        self.until = collector.gather_until

    @register('manifest', '1.0', format='json')
    def collecting(self, **kwargs):
        """Collecting function is skipped"""
        return self.data

    def add_collection(self, collection):
        """Register a collection's filename and version in the manifest.

        Args:
            collection: A :class:`~metrics_utility.base.collection.Collection` instance
                whose filename and version should be recorded.
        """
        self.data[collection.filename] = collection.version
