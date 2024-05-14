from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_summarized_by_org_and_host \
    import DataframeSummarizedByOrgAndHost

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_summarized_by_host \
    import DataframeSummarizedByHost

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_summarized_by_org_and_host_v2 \
    import DataframeSummarizedByOrgAndHostv2

class Factory:
    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        self.report_type = extra_params["report_type"]

    def create(self):
        if self.report_type == "CCSP":
            return self._get_dataframe_summarized_by_org_and_host()
        elif self.report_type == "CCSPv2":
            return self._get_dataframe_summarized_by_host()


    def _get_dataframe_summarized_by_org_and_host(self):
        # Return default S3 loader
        return DataframeSummarizedByOrgAndHost(
            extractor=self.extractor,
            month=self.month,
            extra_params=self.extra_params)

    def _get_dataframe_summarized_by_host(self):
        # Return default S3 loader
        # return DataframeSummarizedByHost(
        return DataframeSummarizedByOrgAndHostv2(
            extractor=self.extractor,
            month=self.month,
            extra_params=self.extra_params)
