import pandas as pd

from metrics_utility.library.dataframes.base_dataframe import BaseDataframe


# FIXME: create a collector from Extract*DB so that this actually can be read from a csv
class DataframeHostMetric(BaseDataframe):
    TARBALL_NAMES = ['host_metric.csv']

    def prepare(self, host_metric):
        # Spreadsheet doesn't support timezones
        host_metric['first_automation'] = pd.to_datetime(host_metric['first_automation'], format='ISO8601').dt.tz_localize(None)
        host_metric['last_automation'] = pd.to_datetime(host_metric['last_automation'], format='ISO8601').dt.tz_localize(None)
        host_metric['last_deleted'] = pd.to_datetime(host_metric['last_deleted'], format='ISO8601').dt.tz_localize(None)
        return super().prepare(host_metric)
