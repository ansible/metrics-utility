import io
import json
import logging
import os
import tarfile
import tempfile

import pandas as pd

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler


class ExtractorS3():
    LOG_PREFIX = "[ExtractorS3]"

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        super().__init__()

        self.extension = "parquet"
        self.path = extra_params["ship_path"]
        self.extra_params = extra_params

        self.logger = logger

        self.s3_handler = S3Handler(params=self.extra_params)

    def _get_path_prefix(self, date):
        path_prefix = f"{self.path}/data"

        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")

        path = f"{path_prefix}/{year}/{month}/{day}"

        return path

    def get_report_path(self, date):
        path_prefix = f"{self.path}/reports"

        year = date.strftime("%Y")
        month = date.strftime("%m")

        path = f"{path_prefix}/{year}/{month}"

        return path

    def iter_batches(self, date, columns=None, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size()
        # Read parquet in memory in batches
        self.logger.info(f"{self.LOG_PREFIX} Processing {date}")
        s3_paths = self.fetch_partition_paths(date)

        if batch_size is None:
            batch_size = self.batch_size()

        for s3_path in s3_paths:
            with tempfile.TemporaryDirectory(prefix="automation_controller_billing_data_") as temp_dir:
                try:
                    local_path = os.path.join(temp_dir, 'source_tarball')
                    self.s3_handler.download_file(s3_path, local_path)

                    tar = tarfile.open(local_path)

                    try:
                        # The filter param is available in Python 3.9.17
                        tar.extractall(path=temp_dir, filter='data', members=self.tarball_sanitize_members(tar))
                    except TypeError:
                        # Trying without filter for older python versions
                        tar.extractall(path=temp_dir, members=self.tarball_sanitize_members(tar))
                    finally:
                        tar.close()

                    config = self.load_config(os.path.join(temp_dir, 'config.json'))

                    # # TODO: read the csvs in batches
                    # for chunk in pd.read_csv(filename, chunksize=chunksize):
                    # # chunk is a DataFrame. To "process" the rows in the chunk:
                    # for index, row in chunk.iterrows():
                    #     print(row)

                    if os.path.exists(os.path.join(temp_dir, 'job_host_summary.csv')):
                        job_host_summary = pd.read_csv(os.path.join(temp_dir, 'job_host_summary.csv'))
                    else:
                        job_host_summary = pd.DataFrame([{}])

                    if os.path.exists(os.path.join(temp_dir, 'main_jobevent.csv')):
                        main_jobevent = pd.read_csv(os.path.join(temp_dir, 'main_jobevent.csv'))
                    else:
                        main_jobevent = pd.DataFrame([{}])

                    yield {'main_jobevent': main_jobevent,
                           'job_host_summary': job_host_summary,
                           'config': config}

                except Exception as e:
                    self.logger.exception(f"{self.LOG_PREFIX} ERROR: Extracting {s3_path} failed with {e}")

    @staticmethod
    def tarball_sanitize_members(tar):
        members = []
        for member in tar.getmembers():
            if member.isdir():
                continue
            if member.name.endswith("json") is False and member.name.endswith("csv") is False:
                continue
            if ".." in member.path:
                continue

            members.append(member)
        return members

    def load_config(self, file_path):
        try:
            with open(file_path) as f:
                config_data = json.loads(f.read())
            return config_data
        except FileNotFoundError as e:
            self.logger.warn(f"{self.LOG_PREFIX} missing required file under path: {self.path} and date: {self.date}")
            # raise MissingRequiredFile(self.filename) from e

    def fetch_partition_paths(self, date):
        prefix = self._get_path_prefix(date)

        paths = [file for file in self.s3_handler.list_files(prefix)]
        return paths

    @staticmethod
    def batch_size():
        return 100000