import json
import logging
import os
import tempfile

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler
from metrics_utility.automation_controller_billing.extract.base import Base


class ExtractorS3(Base):
    LOG_PREFIX = '[ExtractorS3]'

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        super().__init__()

        self.extension = 'parquet'
        self.path = extra_params['ship_path']
        self.extra_params = extra_params
        self.logger = logger

        self.s3_handler = S3Handler(params=self.extra_params)

    def _create_date_string(self, date):
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        return {year, month, day}

    def _get_path_prefix(self, date):
        data_path_prefix = f'{self.path}/data'

        year, month, day = self._create_date_string(date)
        path = f'{data_path_prefix}/{year}/{month}/{day}'

        return path

    def get_report_path(self, date):
        report_path_prefix = f'{self.path}/reports'

        year, month = self._create_date_string(date)
        path = f'{report_path_prefix}/{year}/{month}'

        return path

    def iter_batches(self, date, columns=None, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size()
        # Read parquet in memory in batches
        self.logger.info(f'{self.LOG_PREFIX} Processing {date}')
        s3_paths = self.fetch_partition_paths(date)

        if batch_size is None:
            batch_size = self.batch_size()

        for s3_path in s3_paths:
            with tempfile.TemporaryDirectory(prefix='automation_controller_billing_data_') as temp_dir:
                try:
                    local_path = os.path.join(temp_dir, 'source_tarball')
                    self.s3_handler.download_file(s3_path, local_path)

                    yield self.process_tarballs(self, s3_path, temp_dir)

                except Exception as e:
                    self.logger.exception(f'{self.LOG_PREFIX} ERROR: Extracting {s3_path} failed with {e}')

    def load_config(self, file_path):
        try:
            with open(file_path) as f:
                config_data = json.loads(f.read())
            return config_data
        except FileNotFoundError:
            self.logger.warn(f'{self.LOG_PREFIX} missing required file under path: {self.path} and date: {self.date}')
            # raise MissingRequiredFile(self.filename) from e

    def fetch_partition_paths(self, date):
        prefix = self._get_path_prefix(date)

        paths = [file for file in self.s3_handler.list_files(prefix)]
        return paths

    @staticmethod
    def batch_size():
        return 100000
