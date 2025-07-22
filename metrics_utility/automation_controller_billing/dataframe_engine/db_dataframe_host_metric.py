import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base


# dataframe for host_metric
class DBDataframeHostMetric(Base):
    def build_dataframe(self):
        host_metric_concat = None

        ###############################
        # Start a daily rollup code here
        ###############################
        for data in self.extractor.iter_batches():
            # If the dataframe is empty, skip additional processing
            host_metric = data['host_metric']
            if host_metric.empty:
                continue

            # Spreadsheet doesn't support timezones
            host_metric['first_automation'] = pd.to_datetime(host_metric['first_automation'], format='ISO8601').dt.tz_localize(None)
            host_metric['last_automation'] = pd.to_datetime(host_metric['last_automation'], format='ISO8601').dt.tz_localize(None)
            host_metric['last_deleted'] = pd.to_datetime(host_metric['last_deleted'], format='ISO8601').dt.tz_localize(None)

            if host_metric_concat is None:
                host_metric_concat = host_metric
            else:
                host_metric_concat = pd.concat([host_metric_concat, host_metric], ignore_index=True)

        if host_metric_concat is None:
            return None

        return host_metric_concat.reset_index()
