import os


class ReportSaverDirectory:
    def __init__(self, extra_params):
        self.extra_params = extra_params

        self.report_spreadsheet_destination_path = self.extra_params['report_spreadsheet_destination_path']

    def report_exist(self):
        return os.path.exists(self.report_spreadsheet_destination_path)

    def save(self, report_spreadsheet):
        # Create the dir structure for the final report
        os.makedirs(os.path.dirname(self.report_spreadsheet_destination_path), exist_ok=True)

        report_spreadsheet.save(self.report_spreadsheet_destination_path)
