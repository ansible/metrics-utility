from metrics_utility.automation_controller_billing.report.report_ccsp import ReportCCSP
from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance
from metrics_utility.automation_controller_billing.report.report_renewal_guidance_v2 import ReportRenewalGuidanceV2



class Factory:
    def __init__(self, report_period, report_dataframe, ship_target, extra_params):
        if extra_params.get('report_period_range') is not None:
            self.report_period = extra_params.get('report_period_range')
        else:
            self.report_period = report_period

        self.report_dataframe = report_dataframe

        self.ship_target = ship_target

        self.report_type = extra_params["report_type"]
        self.extra_params = extra_params

    def create(self):
        if self.report_type == "CCSP":
            return self._get_report_ccsp()
        elif self.report_type == "CCSPv2":
            return self._get_report_ccsp_v2()
        elif self.report_type == "RENEWAL_GUIDANCE":
            return self._get_report_renewal_guidance()
        elif self.report_type == "RENEWAL_GUIDANCEv2":
            return self._get_report_renewal_guidance()

    def _get_report_ccsp(self):
        # Return default S3 loader
        return ReportCCSP(self.report_dataframe, self.report_period, self.extra_params)

    def _get_report_ccsp_v2(self):
        # Return default S3 loader
        return ReportCCSPv2(self.report_dataframe, self.report_period, self.extra_params)

    def _get_report_renewal_guidance(self):
        # Return default S3 loader
        return ReportRenewalGuidance(self.report_dataframe, self.report_period, self.extra_params)

    def _get_report_renewal_guidancev2(self):
        return ReportRenewalGuidanceV2(self.report_dataframe, self.report_period, self.extra_params)
