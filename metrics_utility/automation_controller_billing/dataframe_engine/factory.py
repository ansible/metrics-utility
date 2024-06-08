from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage \
    import DataframeJobhostSummaryUsage

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage \
    import DataframeContentUsage

from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric \
    import DBDataframeHostMetric

class Factory:
    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        self.report_type = extra_params["report_type"]

    def create(self):
        if self.report_type == "CCSP":
            return (self._get_dataframe_jobhost_summary_usage().build_dataframe(),
                    self._get_dataframe_content_usage().build_dataframe())
        elif self.report_type == "CCSPv2":
            return (self._get_dataframe_jobhost_summary_usage().build_dataframe(),
                    self._get_dataframe_content_usage().build_dataframe())
        elif self.report_type == "RENEWAL_GUIDANCE":
            return (self._get_db_dataframe_host_metric_usage().build_dataframe(),)


    def _get_dataframe_jobhost_summary_usage(self):
        # Return default S3 loader
        return DataframeJobhostSummaryUsage(
            extractor=self.extractor,
            month=self.month,
            extra_params=self.extra_params)

    def _get_dataframe_content_usage(self):
        # Return default S3 loader
        return DataframeContentUsage(
        # return DataframeSummarizedByOrgAndHostv2(
            extractor=self.extractor,
            month=self.month,
            extra_params=self.extra_params)

    def _get_db_dataframe_host_metric_usage(self):
        # Return default S3 loader
        return DBDataframeHostMetric(
        # return DataframeSummarizedByOrgAndHostv2(
            extractor=self.extractor,
            month=self.month,
            extra_params=self.extra_params)
