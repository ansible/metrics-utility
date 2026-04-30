"""Dataframe for host metric data used in the Renewal Guidance report."""

import pandas as pd

from metrics_utility.library.dataframes.base_dataframe import BaseDataframe


# FIXME: create a collector from Extract*DB so that this actually can be read from a csv
class DataframeHostMetric(BaseDataframe):
    """Dataframe that reads host_metric.csv and coerces timestamp columns."""

    TARBALL_NAMES = ['host_metric.csv']

    def prepare(self, host_metric):
        """Convert ISO 8601 timestamp columns to naive datetimes.

        Args:
            host_metric: Raw host_metric DataFrame from a tarball.

        Returns:
            DataFrame with ``first_automation``, ``last_automation``, and
            ``last_deleted`` converted to timezone-naive datetimes.
        """
        # Spreadsheet doesn't support timezones
        host_metric['first_automation'] = pd.to_datetime(host_metric['first_automation'], format='ISO8601').dt.tz_localize(None)
        host_metric['last_automation'] = pd.to_datetime(host_metric['last_automation'], format='ISO8601').dt.tz_localize(None)
        host_metric['last_deleted'] = pd.to_datetime(host_metric['last_deleted'], format='ISO8601').dt.tz_localize(None)
        return super().prepare(host_metric)
