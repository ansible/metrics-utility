import logging
import datetime
import pandas as pd
import re
from dateutil.relativedelta import relativedelta

from metrics_utility.automation_controller_billing.dataframe_engine.base \
    import Base, list_dates, granularity_cast

logger = logging.getLogger(__name__)

#######################################
# Code for building of the dataframe report based on Event table
######################################

class DBDataframeHostMetric(Base):
    LOG_PREFIX = "[AAPBillingReport] "

    def build_dataframe(self):
        host_metric_rollup = None
        # TODO pass since here

        ###############################
        # Start a daily rollup code here
        ###############################
        for data in self.extractor.iter_batches():
            # If the dataframe is empty, skip additional processing
            host_metric = data['host_metric']
            if host_metric.empty:
                continue

            # events['install_uuid'] = data['config']['install_uuid']
            host_metric["last_deleted"] = pd.to_datetime(host_metric['last_deleted'])

            ################################
            # Do the aggregation
            ################################
            host_metric_group = host_metric.groupby(
                self.unique_index_columns(), dropna=False
            ).agg(
                first_automation=('first_automation', 'min'),
                last_automation=('last_automation', 'max'),
                last_deleted=('last_deleted', 'max'),
                automated_counter=('automated_counter', 'sum'),
                deleted_counter=('deleted_counter', 'sum'),
                deleted=('deleted', 'min')
            )

            # Tweak types to match the table
            host_metric_group = self.cast_dataframe(host_metric_group, self.cast_types())
            # Need to remove locatization, comparing localized and not localized (None, NaT) fails
            host_metric_group["last_deleted"] = pd.to_datetime(host_metric_group['last_deleted']).dt.tz_localize(None)

            ################################
            # Merge aggregations of multiple batches
            ################################
            if host_metric_rollup is None:
                host_metric_rollup = host_metric_group
            else:
                # Multipart collection, merge the dataframes and sum counts
                host_metric_rollup = pd.merge(
                    host_metric_rollup.loc[:, ],
                    host_metric_group.loc[:, ],
                    on=self.unique_index_columns(),
                    how='outer')

                host_metric_rollup = self.summarize_merged_dataframes(
                    host_metric_rollup, self.data_columns(),
                    operations = {"first_automation": "min",
                                  "last_automation": "max",
                                  "last_deleted": "max",
                                  "deleted": "min"})

                # Tweak types to match the table
                host_metric_rollup = self.cast_dataframe(
                    host_metric_rollup, self.cast_types())

                # host_metric_rollup["last_deleted"] = pd.to_datetime(host_metric_rollup['last_deleted'])

        if host_metric_rollup is None:
            return None

        return host_metric_rollup.reset_index()

    @staticmethod
    def unique_index_columns():
        return ['hostname']

    @staticmethod
    def data_columns():
        return ['first_automation', 'last_automation', 'automated_counter',
                'deleted_counter', 'last_deleted', 'deleted']

    @staticmethod
    def cast_types():
        return {
            'first_automation': "datetime64[ns, UTC]",
            'last_automation': "datetime64[ns, UTC]",
            'automated_counter': 'int64',
            'deleted_counter':'int64',
            'deleted': 'bool'}
