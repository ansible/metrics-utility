import io
import logging
import os
import tempfile

import pandas as pd

from metrics_utility.automation_controller_billing.extract.base import Base


class ExtractorDirectory(Base):
    LOG_PREFIX = '[ExtractorDirectory]'

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        super().__init__(logger=logger)

        self.extension = 'parquet'
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
        # Read parquet in memory in batches
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

    def mapping(self, normalized_table, id_column='id', value_column='value'):
        try:
            mapping = self.read_parquet_file(f'data/parquet/{self.tenant_id}/{normalized_table}/{normalized_table}.parquet')
            mapping.set_index(id_column, inplace=True)
            return mapping[value_column].astype(str).to_dict()
        except Exception:
            return {}

    def read_parquet_file(self, obj, columns=None, raise_exception=False, silence_exception=False):
        try:
            buffer = io.BytesIO()
            object = self.s3.Object(self.s3_bucket_name, obj)
            object.download_fileobj(buffer)
            if columns:
                return pd.read_parquet(buffer, columns=columns)
            else:
                return pd.read_parquet(buffer)
        except Exception as e:
            if not silence_exception:
                self.logger.error(f'ERROR: {obj} failed with {e}')
            if raise_exception:
                raise e

    def read_parquet_files(self, objs, columns=None, raise_exception=False, silence_exception=False):
        dfs = [self.read_parquet_file(obj, columns, raise_exception, silence_exception) for obj in objs if obj is not None]
        dfs = [df for df in dfs if df is not None]
        if len(dfs) > 0:
            return pd.concat(dfs, ignore_index=True)
        else:
            return None

    @staticmethod
    def batch_size():
        return 100000
