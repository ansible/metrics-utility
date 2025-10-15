import tempfile

from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.library.storage import StorageS3
from metrics_utility.logger import logger


class ExtractorS3(Base):
    LOG_PREFIX = '[ExtractorS3]'

    def __init__(self, extra_params):
        super().__init__(extra_params)

        self.storage = StorageS3(
            bucket=extra_params.get('bucket_name'),
            endpoint=extra_params.get('bucket_endpoint'),
            region=extra_params.get('bucket_region'),
            access_key=extra_params.get('bucket_access_key'),
            secret_key=extra_params.get('bucket_secret_key'),
        )

    def iter_batches(self, date, collections, optional):
        # Read tarball in memory in batches
        logger.debug(f'{self.LOG_PREFIX} Processing {date}')
        enabled_set = (collections or []) + (optional or [])

        for s3_path in self._fetch_partition_paths(date, collections):
            try:
                with self.storage.get(s3_path) as local_path:
                    with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                        yield self.process_tarballs(local_path, temp_dir, enabled_set=enabled_set)
            except Exception as e:
                logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {s3_path} failed with {e}')

    def _fetch_partition_paths(self, date, collections):
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        ship_path = self.extra_params.get('ship_path')
        prefix = f'{ship_path}/data/{year}/{month}/{day}'

        paths = self.storage.list_files(prefix)
        return self.filter_tarball_paths(paths, collections)
