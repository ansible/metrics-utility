import io
import json
import logging
import os
import tarfile
import tempfile

import pandas as pd


class ReportSaverDirectory():
    LOG_PREFIX = "[ExtractorDirectory]"

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        self.extra_params = extra_params

        self.logger = logger

        self.report_spreadsheet_destination_path = self.extra_params["report_spreadsheet_destination_path"]

    def report_exist(self):
        if os.path.exists(self.report_spreadsheet_destination_path):
            return True
        return False

    def save(self, report_spreadsheet):
        # Create the dir structure for the final report
        os.makedirs(os.path.dirname(self.report_spreadsheet_destination_path), exist_ok=True)

        report_spreadsheet.save(self.report_spreadsheet_destination_path)

        self.logger.info(f"Report generated into: {self.report_spreadsheet_destination_path}")