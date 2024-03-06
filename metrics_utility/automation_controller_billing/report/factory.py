from metrics_utility.automation_controller_billing.report.report_ccsp import ReportCCSP

class Factory:
    def __init__(self, report_period, report_dataframe, ship_target, extra_params):
        self.report_period = report_period
        self.report_dataframe = report_dataframe

        self.ship_target = ship_target

        self.report_type = extra_params["report_type"]
        self.extra_params = extra_params

    def create(self):
        if self.report_type == "CCSP":
            return self._get_report_ccsp()

    def _get_report_ccsp(self):
        # Return default S3 loader
        return ReportCCSP(self.report_dataframe, self.report_period, self.extra_params)
