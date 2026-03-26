import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


# Known collector types and their expected daily execution counts.
# Hourly collectors run once per hour → 24 expected per day.
# Snapshot collectors run once per day → 1 expected per day.
# Pipeline tasks run once per day → 1 expected per day.
_HOURLY_COLLECTORS = frozenset({
    'unified_jobs',
    'job_host_summary_service',
    'credentials_service',
    'main_jobevent_service',
})
_SNAPSHOT_COLLECTORS = frozenset({
    'execution_environments',
    'table_metadata',
    'controller_version_service',
    'feature_flags_service',
})
_EXPECTED_EXECUTIONS_PER_DAY = {
    **{c: 24 for c in _HOURLY_COLLECTORS},
    **{c: 1 for c in _SNAPSHOT_COLLECTORS},
}

# Default expected count for unknown collector types (assume hourly).
_DEFAULT_EXPECTED = 24


class TaskExecutionsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Rollup for task_executions_service collector data.

    Produces observability statistics per collector type so that the recipient
    of the anonymized report can see how well the collector pipeline is running.

    For each collector_type the rollup reports:
      - executions_total            – number of recorded executions
      - executions_failed_total     – number of failed executions
      - executions_missing_total    – expected - actual (≥ 0)
      - execution_duration_total_seconds   – sum of execution_time_seconds
      - execution_duration_minimum_seconds – minimum execution_time_seconds
      - execution_duration_maximum_seconds – maximum execution_time_seconds

    For snapshot collectors exactly one execution is expected per day, so
    min == max == total (unification with the hourly case is intentional).
    """

    def __init__(self):
        super().__init__('task_executions')
        self.collector_names = ['task_executions']

    def prepare(self, dataframe):
        """
        Aggregate raw collector-execution rows into per-collector-type statistics.
        """
        if dataframe is None or (isinstance(dataframe, pd.DataFrame) and dataframe.empty):
            return sanitize_json([])

        result = []

        for collector_type, group in dataframe.groupby('collector_type'):
            executions_total = len(group)
            executions_failed_total = int((group['status'] == 'failed').sum())

            expected = _EXPECTED_EXECUTIONS_PER_DAY.get(str(collector_type), _DEFAULT_EXPECTED)
            executions_missing_total = max(0, expected - executions_total)

            times = group['execution_time_seconds'].dropna()
            if len(times) > 0:
                duration_total = float(times.sum())
                duration_min = float(times.min())
                duration_max = float(times.max())
            else:
                duration_total = None
                duration_min = None
                duration_max = None

            result.append({
                'collector_type': str(collector_type),
                'executions_total': executions_total,
                'executions_failed_total': executions_failed_total,
                'executions_missing_total': executions_missing_total,
                'execution_duration_total_seconds': duration_total,
                'execution_duration_minimum_seconds': duration_min,
                'execution_duration_maximum_seconds': duration_max,
            })

        return sanitize_json(result)

    def merge(self, data_all, data_new):
        """
        Snapshot collector: always replace with the latest data (no accumulation).
        """
        return data_new

    def base(self, data):
        """
        Return the final JSON representation.

        Accepts either the list produced by prepare() or a raw DataFrame.
        """
        if data is None:
            return {'json': []}
        if isinstance(data, pd.DataFrame):
            data = self.prepare(data)
        return {'json': data}
