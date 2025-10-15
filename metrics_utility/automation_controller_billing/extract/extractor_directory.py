import tempfile

from metrics_utility.automation_controller_billing.extract.base import Base
from metrics_utility.library.storage import StorageDirectory
from metrics_utility.logger import logger


class ExtractorDirectory(Base):
    LOG_PREFIX = '[ExtractorDirectory]'

    def __init__(self, extra_params):
        super().__init__(extra_params)

        self.storage = StorageDirectory(base_path=extra_params.get('ship_path'))

    def iter_batches(self, date, collections, optional):
        # Read tarball in memory in batches
        logger.debug(f'{self.LOG_PREFIX} Processing {date}')
        enabled_set = (collections or []) + (optional or [])

        for path in self._fetch_partition_paths(date, collections):
            try:
                with self.storage.get(path) as local_path:
                    with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                        yield self.process_tarballs(local_path, temp_dir, enabled_set=enabled_set)
            except Exception as e:
                logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {path} failed with {e}')

    def _fetch_partition_paths(self, date, collections):
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        # relative to storage base_path (=ship_path)
        prefix = f'data/{year}/{month}/{day}'

        paths = self.storage.list_files(prefix)
        return self.filter_tarball_paths(paths, collections)
