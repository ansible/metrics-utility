import os
import tempfile

from metrics_utility.library.storage import StorageS3
from metrics_utility.logger import logger


class ReportSaverS3:
    LOG_PREFIX = '[ReportSaverS3]'

    def __init__(self, extra_params):
        self.extra_params = extra_params

        # FIXME: remove once build_report no longer uses it
        self.report_spreadsheet_destination_path = self.extra_params['report_spreadsheet_destination_path']

        self.dest_path = extra_params['report_spreadsheet_destination_path']
        self.storage = StorageS3(
            bucket=extra_params.get('bucket_name'),
            endpoint=extra_params.get('bucket_endpoint'),
            region=extra_params.get('bucket_region'),
            access_key=extra_params.get('bucket_access_key'),
            secret_key=extra_params.get('bucket_secret_key'),
        )

    def report_exist(self):
        return self.storage.exists(self.dest_path)

    def save(self, report_spreadsheet):
        with tempfile.TemporaryDirectory(prefix='report_saver_billing_data_') as temp_dir:
            try:
                local_report_path = os.path.join(temp_dir, 'report')
                report_spreadsheet.save(local_report_path)
                self.storage.put(self.dest_path, filename=local_report_path)
            except Exception as e:
                logger.exception(f'{self.LOG_PREFIX} ERROR: Saving report to S3 into path {self.dest_path} failed with {e}')

        logger.info(f'Report sent into S3 bucket into path: {self.dest_path}')
