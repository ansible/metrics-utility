from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_collection_status import DataframeCollectionStatus
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import DataframeInventoryScope
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import DataframeJobhostSummaryUsage
from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import DBDataframeHostMetric
from metrics_utility.exceptions import NotSupportedFactory


class Factory:  # DataframeFactory
    def __init__(self, extractor, month, extra_params):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

    def create(self):
        report_type = self.extra_params['report_type']

        kwargs = {
            'extractor': self.extractor,
            'month': self.month,
            'extra_params': self.extra_params,
        }

        if report_type == 'CCSP':
            return {
                'job_host_summary': DataframeJobhostSummaryUsage(**kwargs),
                'main_jobevent': DataframeContentUsage(**kwargs),
                'main_host': DataframeInventoryScope(**kwargs),
            }

        if report_type == 'CCSPv2':
            return {
                'job_host_summary': DataframeJobhostSummaryUsage(**kwargs),
                'main_jobevent': DataframeContentUsage(**kwargs),
                'main_host': DataframeInventoryScope(**kwargs),
                'data_collection_status': DataframeCollectionStatus(**kwargs),
            }

        if report_type == 'RENEWAL_GUIDANCE':
            return {'host_metric': DBDataframeHostMetric(**kwargs)}

        raise NotSupportedFactory(f'Factory for {report_type} not supported')
