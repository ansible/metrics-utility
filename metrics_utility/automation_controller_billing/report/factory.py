from metrics_utility.automation_controller_billing.report.report_ccsp import ReportCCSP
from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance


class Factory:
    def __init__(self, report_dataframe, extra_params):
        self.report_dataframe = report_dataframe
        self.extra_params = extra_params

        self.report_type = extra_params['report_type']

    def create(self):
        if self.report_type == 'CCSP':
            return self._get_report_ccsp()
        elif self.report_type == 'CCSPv2':
            return self._get_report_ccsp_v2()
        elif self.report_type == 'RENEWAL_GUIDANCE':
            return self._get_report_renewal_guidance()

    def _get_report_ccsp(self):
        # Return default S3 loader
        return ReportCCSP(self.report_dataframe, self.extra_params)

    def _get_report_ccsp_v2(self):
        # Return default S3 loader
        return ReportCCSPv2(self.report_dataframe, self.extra_params)

    def _get_report_renewal_guidance(self):
        # Return default S3 loader
        return ReportRenewalGuidance(self.report_dataframe, self.extra_params)
