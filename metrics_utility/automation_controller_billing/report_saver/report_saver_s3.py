import io
import json
import logging
import os
import tarfile
import tempfile

import pandas as pd

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler


class ReportSaverS3():
    LOG_PREFIX = "[ReportSaverS3]"

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        self.extra_params = extra_params
        self.report_spreadsheet_destination_path = self.extra_params["report_spreadsheet_destination_path"]

        self.logger = logger

        self.s3_handler = S3Handler(params=self.extra_params)

    def report_exist(self):
        if len([file for file in self.s3_handler.list_files(self.report_spreadsheet_destination_path)]) > 0:
            return True
        return False

    def save(self, report_spreadsheet):
        with tempfile.TemporaryDirectory(prefix="report_saver_billing_data_") as temp_dir:
            try:
                local_report_path = os.path.join(temp_dir, 'report')
                report_spreadsheet.save(local_report_path)

                self.s3_handler.upload_file(local_report_path, self.report_spreadsheet_destination_path)

            except Exception as e:
                self.logger.exception(f"{self.LOG_PREFIX} ERROR: Saving report to S3 into path {self.report_spreadsheet_destination_path} failed with {e}")

        self.logger.info(f"Report sent into S3 bucket into path: {self.report_spreadsheet_destination_path}")