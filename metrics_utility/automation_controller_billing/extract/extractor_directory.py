import logging
import os
import tempfile

from metrics_utility.automation_controller_billing.extract.base import Base


class ExtractorDirectory(Base):
    LOG_PREFIX = '[ExtractorDirectory]'

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        super().__init__(logger=logger)

        self.path = extra_params['ship_path']
        self.extra_params = extra_params

    def _get_path_prefix(self, date):
        path_prefix = f'{self.path}/data'

        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        path = f'{path_prefix}/{year}/{month}/{day}'

        return path

    def get_report_path(self, date):
        path_prefix = f'{self.path}/reports'

        year = date.strftime('%Y')
        month = date.strftime('%m')

        path = f'{path_prefix}/{year}/{month}'

        return path

    def iter_batches(self, date, columns=None, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size()

        # Read tarball in memory in batches
        self.logger.info(f'{self.LOG_PREFIX} Processing {date}')
        paths = self.fetch_partition_paths(date)

        if batch_size is None:
            batch_size = self.batch_size()

        for path in paths:
            if not path.endswith('.tar.gz'):
                continue

            with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                try:
                    yield self.process_tarballs(path, temp_dir)

                except Exception as e:
                    self.logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {path} failed with {e}')

    def fetch_partition_paths(self, date):
        prefix = self._get_path_prefix(date)

        try:
            paths = [os.path.join(prefix, f) for f in os.listdir(prefix) if os.path.isfile(os.path.join(prefix, f))]
        except FileNotFoundError:
            paths = []

        return paths

    @staticmethod
    def batch_size():
        return 100000
