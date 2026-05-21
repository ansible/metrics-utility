"""Manifest collection that records the version of every file in a tarball."""

from metrics_utility.gather.decorators import register

from .collection_json import CollectionJSON


class CollectionManifest(CollectionJSON):
    """JSON collection that tracks the filename→version mapping for a Package tarball."""

    def __init__(self, collector):
        """Create the manifest for *collector*'s current gathering window.

        Args:
            collector: The active :class:`~metrics_utility.gather.collector.Collector` instance.
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
            collection: A :class:`~metrics_utility.gather.collection.collection.Collection` instance
                whose filename and version should be recorded.
        """
        self.data[collection.filename] = collection.version
