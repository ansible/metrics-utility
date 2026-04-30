"""Report saver that writes the XLSX spreadsheet to the local filesystem."""

import os


class ReportSaverDirectory:
    """Saves the generated XLSX report to a path on the local filesystem."""

    def __init__(self, extra_params):
        """Initialise the directory report saver.

        Args:
            extra_params: Dict containing ``'report_spreadsheet_destination_path'``.
        """
        self.extra_params = extra_params

        self.report_spreadsheet_destination_path = self.extra_params['report_spreadsheet_destination_path']

    def report_exist(self):
        """Check whether the report file already exists at the destination path.

        Returns:
            True if the file exists, False otherwise.
        """
        return os.path.exists(self.report_spreadsheet_destination_path)

    def save(self, report_spreadsheet):
        """Save the openpyxl Workbook to the configured destination path.

        Creates any intermediate directories as needed.

        Args:
            report_spreadsheet: An :class:`openpyxl.workbook.workbook.Workbook` instance.
        """
        # Create the dir structure for the final report
        os.makedirs(os.path.dirname(self.report_spreadsheet_destination_path), exist_ok=True)

        report_spreadsheet.save(self.report_spreadsheet_destination_path)
