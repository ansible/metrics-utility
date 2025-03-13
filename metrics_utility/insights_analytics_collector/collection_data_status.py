import csv
import os

from .collection_csv import CollectionCSV
from .decorators import register


class CollectionDataStatus(CollectionCSV):
    def __init__(self, collector, package):
        super().__init__(collector, self.data_collection_status)

        self.package = package

    @register(
        "data_collection_status",
        "1.0",
        format="csv",
        description="Data collection status",
    )
    def data_collection_status(self, full_path, **kwargs):
        file_path = os.path.join(full_path, self.filename)
        with open(file_path, "w", newline="") as csvfile:
            fieldnames = [
                "collection_start_timestamp",
                "since",
                "until",
                "file_name",
                "status",
                "elapsed",
            ]
            writer = csv.DictWriter(csvfile, delimiter=",", fieldnames=fieldnames)
            writer.writeheader()

            for collection in self.package.collections:
                status = "ok" if collection.gathering_successful else "failed"
                elapsed = 0
                if collection.gathering_started_at and collection.gathering_finished_at:
                    elapsed = (
                        collection.gathering_finished_at
                        - collection.gathering_started_at
                    ).seconds

                writer.writerow(
                    {
                        "collection_start_timestamp": collection.gathering_started_at,
                        "since": collection.since,
                        "until": collection.until,
                        "file_name": collection.filename,
                        "status": status,
                        "elapsed": elapsed,
                    }
                )
        return [file_path]
