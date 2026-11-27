"""Extractor that reads billing tarballs from the local filesystem."""

import os
import tempfile

from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.logger import logger


class ExtractorDirectory(Base):
    """Extracts billing data tarballs from a local directory partition."""

    LOG_PREFIX = '[ExtractorDirectory]'

    def iter_batches(self, date, collections, optional):
        """Yield per-tarball data dicts for the given date.

        Args:
            date: :class:`datetime.date` identifying the day partition to read.
            collections: Required collector names to extract from each archive.
            optional: List of additional collector names to include if present.

        Yields:
            Dict from :meth:`~.base.Base.process_tarballs` for each tarball found.
        """
        # Read tarball in memory in batches
        logger.debug(f'{self.LOG_PREFIX} Processing {date}')
        paths = self.fetch_partition_paths(date, collections)

        for path in paths:
            if not path.endswith('.tar.gz'):
                continue

            with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                try:
                    yield self.process_tarballs(path, temp_dir, enabled_set=(collections or []) + (optional or []))

                except Exception as e:
                    logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {path} failed with {e}')

    def fetch_partition_paths(self, date, collections):
        """Return filtered paths to all tarballs in the date partition directory.

        Args:
            date: :class:`datetime.date` used to construct the partition path.
            collections: Requested collector names, or None for all.

        Returns:
            Candidate paths. A filename alone cannot identify every collection
            in a mixed archive.
        """
        prefix = self.get_path_prefix(date)

        try:
            paths = [os.path.join(prefix, f) for f in os.listdir(prefix) if os.path.isfile(os.path.join(prefix, f))]
        except FileNotFoundError:
            paths = []

        return self.filter_tarball_paths(paths, collections)
