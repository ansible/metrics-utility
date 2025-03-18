from metrics_utility.automation_controller_billing.report_saver.report_saver_directory import ReportSaverDirectory
from metrics_utility.automation_controller_billing.report_saver.report_saver_s3 import ReportSaverS3
from metrics_utility.exceptions import NotSupportedFactory


class Factory:
    def __init__(self, ship_target, extra_params):
        self.ship_target = ship_target
        self.extra_params = extra_params

    def create(self):
        if self.ship_target in ['directory', 'controller_db']:
            return self._get_report_saver_directory()
        elif self.ship_target == 's3':
            return self._get_report_saver_s3()
        else:
            raise NotSupportedFactory(f'Factory for {self.ship_target} not supported')

    def _get_report_saver_directory(self):
        # Return default S3 loader
        return ReportSaverDirectory(self.extra_params)

    def _get_report_saver_s3(self):
        # Return default S3 loader
        return ReportSaverS3(self.extra_params)
