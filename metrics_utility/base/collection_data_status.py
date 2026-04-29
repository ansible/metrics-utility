"""CSV collection that records per-collection gathering status for a package."""

import csv
import os

from .collection_csv import CollectionCSV
from .decorators import register


class CollectionDataStatus(CollectionCSV):
    """CSV collection written to every Package tarball describing the status of each
    collection (success/failure, elapsed time, time range covered)."""

    def __init__(self, collector, package):
        """Initialise status collection for the given package.

        Args:
            collector: The active :class:`~metrics_utility.base.collector.Collector` instance.
            package: The :class:`~metrics_utility.base.package.Package` whose
                collections will be summarised.
        """
        super().__init__(collector, self.data_collection_status)

        self.package = package

    @register('data_collection_status', '1.0', format='csv')
    def data_collection_status(self, since, until, output):
        """Write per-collection status rows to a CSV file and return the file path list.

        Args:
            since: Start of the collection window (datetime).
            until: End of the collection window (datetime).
            output: :class:`~metrics_utility.library.collectors.util.CollectionOutput`
                instance used to resolve the output directory.

        Returns:
            List containing the path to the written CSV file.
        """
        file_path = os.path.join(output.full_path, self.filename)
        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = [
                'collection_start_timestamp',
                'since',
                'until',
                'file_name',
                'status',
                'elapsed',
            ]
            writer = csv.DictWriter(csvfile, delimiter=',', fieldnames=fieldnames)
            writer.writeheader()

            for collection in self.package.collections:
                status = 'ok' if collection.gathering_successful else 'failed'
                elapsed = 0
                if collection.gathering_started_at and collection.gathering_finished_at:
                    elapsed = (collection.gathering_finished_at - collection.gathering_started_at).seconds

                writer.writerow(
                    {
                        'collection_start_timestamp': collection.gathering_started_at,
                        'since': collection.since,
                        'until': collection.until,
                        'file_name': collection.filename,
                        'status': status,
                        'elapsed': elapsed,
                    }
                )

        return output.files([file_path])
