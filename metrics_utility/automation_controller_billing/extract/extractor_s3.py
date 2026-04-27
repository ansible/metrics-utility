"""Extractor that reads billing tarballs from an S3-compatible object store."""

import os
import tempfile

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler
from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.logger import logger


class ExtractorS3(Base):
    """Extracts billing data tarballs by downloading them from S3."""

    LOG_PREFIX = '[ExtractorS3]'

    def __init__(self, extra_params):
        """Initialise the S3 extractor and create an :class:`~.base.s3_handler.S3Handler`.

        Args:
            extra_params: Dict containing S3 credentials and ``'ship_path'``.
        """
        super().__init__(extra_params)

        self.s3_handler = S3Handler(params=self.extra_params)

    def iter_batches(self, date, collections, optional):
        """Download and yield per-tarball data dicts for the given date.

        Args:
            date: :class:`datetime.date` identifying the day partition in S3.
            collections: List of required collector names (used to filter S3 keys).
            optional: List of additional collector names to include if present.

        Yields:
            Dict from :meth:`~.base.Base.process_tarballs` for each downloaded
            tarball.
        """
        # Read tarball in memory in batches
        logger.debug(f'{self.LOG_PREFIX} Processing {date}')
        s3_paths = self.fetch_partition_paths(date, collections)

        for s3_path in s3_paths:
            with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                try:
                    local_path = os.path.join(temp_dir, 'source_tarball')
                    self.s3_handler.download_file(s3_path, local_path)

                    yield self.process_tarballs(local_path, temp_dir, enabled_set=(collections or []) + (optional or []))

                except Exception as e:
                    logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {s3_path} failed with {e}')

    def fetch_partition_paths(self, date, collections):
        """List and filter S3 keys in the date partition.

        Args:
            date: :class:`datetime.date` used to construct the S3 prefix.
            collections: List of collector names to filter by (or None for all).

        Returns:
            List of S3 key strings matching the requested collections.
        """
        # FIXME: apply collections= filtering, so we don't download files from S3 if we know they don't have the right thing
        prefix = self.get_path_prefix(date)
        paths = self.s3_handler.list_files(prefix)

        return self.filter_tarball_paths(paths, collections)
