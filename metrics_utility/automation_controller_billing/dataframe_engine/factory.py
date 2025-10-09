from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.exceptions import NotSupportedFactory
from metrics_utility.library.dataframes import (
    DataframeDataCollectionStatus,
    DataframeHostMetric,
    DataframeJobHostSummary,
    DataframeMainHost,
    DataframeMainJobevent,
)


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
                'job_host_summary': Base(**kwargs, klass=DataframeJobHostSummary),
                'main_jobevent': Base(**kwargs, klass=DataframeMainJobevent),
                'main_host': Base(**kwargs, klass=DataframeMainHost),
            }

        if report_type == 'CCSPv2':
            return {
                'job_host_summary': Base(**kwargs, klass=DataframeJobHostSummary),
                'main_jobevent': Base(**kwargs, klass=DataframeMainJobevent),
                'main_host': Base(**kwargs, klass=DataframeMainHost),
                'data_collection_status': Base(**kwargs, klass=DataframeDataCollectionStatus),
            }

        if report_type == 'RENEWAL_GUIDANCE':
            return {'host_metric': Base(**kwargs, klass=DataframeHostMetric)}

        raise NotSupportedFactory(f'Factory for {report_type} not supported')
