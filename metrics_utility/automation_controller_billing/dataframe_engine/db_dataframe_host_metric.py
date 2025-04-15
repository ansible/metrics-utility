import logging

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base


logger = logging.getLogger(__name__)

#######################################
# Code for building of the dataframe report based on HostMetric table
######################################


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

            # host_metric['install_uuid'] = data['config']['install_uuid']
            host_metric['last_deleted'] = pd.to_datetime(host_metric['last_deleted'], format='ISO8601')

            if host_metric_concat is None:
                host_metric_concat = host_metric
            else:
                host_metric_concat = pd.concat([host_metric_concat, host_metric], ignore_index=True)

        if host_metric_concat is None:
            return None

        return host_metric_concat.reset_index()
