"""Dataframe engine for host_metric data fetched directly from the Controller database."""

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base


# dataframe for host_metric
class DBDataframeHostMetric(Base):
    """Reads host_metric batches from :class:`~.extract.extractor_controller_db.ExtractorControllerDB`."""

    def build_dataframe(self):
        """Build the host_metric DataFrame by iterating over DB extractor batches.

        Returns:
            Concatenated DataFrame with timestamp columns coerced to timezone-naive
            datetimes, or None if no batches were returned.
        """
        batches = []
        since = pd.Timestamp(self.extractor.extra_params['opt_since'])
        since = since.tz_localize('UTC') if since.tzinfo is None else since.tz_convert('UTC')

        ###############################
        # Start a daily rollup code here
        ###############################
        for data in self.extractor.iter_batches():
            # If the dataframe is empty, skip additional processing
            host_metric = data['host_metric']
            if host_metric.empty:
                continue

            # Re-filter to automation scope: the DB query returns rows where
            # last_automation OR last_deleted >= since, but downstream only
            # wants rows where last_automation is in range.
            last_automation = pd.to_datetime(host_metric['last_automation'], format='ISO8601', utc=True)
            mask = last_automation >= since
            # Handle batches that lack first_automation (time-range probe with
            # only last_automation column) — skip entirely when no rows match.
            if not mask.any():
                continue
            host_metric = host_metric.loc[mask].copy() if len(mask) != mask.sum() else host_metric
            # Spreadsheet doesn't support timezones
            host_metric['first_automation'] = pd.to_datetime(host_metric['first_automation'], format='ISO8601').dt.tz_localize(None)
            host_metric['last_automation'] = pd.to_datetime(host_metric['last_automation'], format='ISO8601').dt.tz_localize(None)
            host_metric['last_deleted'] = pd.to_datetime(host_metric['last_deleted'], format='ISO8601').dt.tz_localize(None)

            batches.append(host_metric)

        if not batches:
            return None

        host_metric_concat = pd.concat(batches, ignore_index=True) if len(batches) > 1 else batches[0]

        return host_metric_concat.reset_index()
