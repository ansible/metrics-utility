import os
import tempfile

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler
from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.logger import logger


class ExtractorS3(Base):
    LOG_PREFIX = '[ExtractorS3]'

    def __init__(self, extra_params):
        super().__init__(extra_params)

        self.s3_handler = S3Handler(params=self.extra_params)

    def iter_batches(self, date, collections, optional):
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
        # FIXME: apply collections= filtering, so we don't download files from S3 if we know they don't have the right thing
        prefix = self.get_path_prefix(date)
        paths = self.s3_handler.list_files(prefix)

        return self.filter_tarball_paths(paths, collections)
