from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_summarized_by_org_and_host \
    import DataframeSummarizedByOrgAndHost

class Factory:
    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        self.report_type = extra_params["report_type"]

    def create(self):
        if self.report_type == "CCSP":
            return self._get_dataframe_summarized_by_org_and_host()

    def _get_dataframe_summarized_by_org_and_host(self):
        # Return default S3 loader
        return DataframeSummarizedByOrgAndHost(
            extractor=self.extractor,
            month=self.month,
            extra_params=self.extra_params)
