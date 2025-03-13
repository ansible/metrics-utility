from .collection_json import CollectionJSON
from .decorators import register


class CollectionManifest(CollectionJSON):
    def __init__(self, collector):
        super().__init__(collector, self.collecting)

        self.data = {}
        self.since = collector.gather_since
        self.until = collector.gather_until

    @register("manifest", "1.0", format="json", description="Manifest file")
    def collecting(self, **kwargs):
        """Collecting function is skipped"""
        return self.data

    def add_collection(self, collection):
        self.data[collection.filename] = collection.version
