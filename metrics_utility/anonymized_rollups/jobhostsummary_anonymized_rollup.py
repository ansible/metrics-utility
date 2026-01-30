import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class JobHostSummaryAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - job_host_summary_service collector data
    """

    def __init__(self):
        super().__init__('job_host_summary')
        self.collector_names = ['job_host_summary_service']

    def merge(self, data_all, data_new):
        """
        Override merge to handle the new structure with jobhostsummary_total and aggregated data.
        Concatenates aggregated dataframes and sums jobhostsummary_total.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Concatenate aggregated dataframes and sum jobhostsummary_totals
        return {
            'jobhostsummary_total': data_all['jobhostsummary_total'] + data_new['jobhostsummary_total'],
            'aggregated': pd.concat([data_all['aggregated'], data_new['aggregated']], ignore_index=True),
        }

    # prepare is called for each batch of data
    # result of prepare is concatenated with other batches into one dataframe
    # each dataframe in prepare should reduce the number of rows as much as possible
    # dataframe has:
    # job_remote_id
    # job_template_name
    # host_name
    # dark
    # failures
    # ok
    # skipped
    # ignored
    # rescued
    # model (job_type)

    def prepare(self, dataframe):
        # Count all records before processing
        jobhostsummary_total = len(dataframe)

        # Group by job_type (model) and sum task columns to reduce data volume early
        # This significantly improves performance when processing large batches
        if dataframe.empty:
            return {
                'jobhostsummary_total': jobhostsummary_total,
                'aggregated': dataframe,
            }

        # Check if model column exists (for backward compatibility)
        if 'model' not in dataframe.columns:
            # If model is missing, create a default 'unknown' value
            dataframe['model'] = 'unknown'

        # Group by job_type (model) and sum task columns
        aggregated = (
            dataframe.groupby('model')
            .agg(
                dark_total=('dark', 'sum'),
                failures_total=('failures', 'sum'),
                ok_total=('ok', 'sum'),
                skipped_total=('skipped', 'sum'),
                ignored_total=('ignored', 'sum'),
                rescued_total=('rescued', 'sum'),
                unique_hosts=('host_name', lambda x: set(x)),
            )
            .reset_index()
            .rename(columns={'model': 'job_type'})
        )

        return {
            'jobhostsummary_total': jobhostsummary_total,
            'aggregated': aggregated,
        }

    def base(self, data):
        """
        Aggregations grouped by job_type (model):
        - Number of tasks executed (sum of all tasks executed per job_type)
        - Success ratio of tasks executed (ratio between ok and failed tasks (and others))
        - Unique hosts per job_type

        Success rate and average - this can compute SaaS team from the metrics

        data is a dict with 'jobhostsummary_total' and 'aggregated' dataframe
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {
                    'by_job_type': [],
                },
                'rollup': {'aggregated': pd.DataFrame(), 'jobhostsummary_total': 0},
            }

        # Extract jobhostsummary_total and aggregated dataframe from the data structure
        jobhostsummary_total = data.get('jobhostsummary_total', 0)
        dataframe = data.get('aggregated', pd.DataFrame())

        # Return empty result if dataframe is empty
        if dataframe.empty:
            return {
                'json': {
                    'by_job_type': [],
                },
                'rollup': {'aggregated': dataframe, 'jobhostsummary_total': jobhostsummary_total},
            }

        # Group by job_type and aggregate across all batches
        # Union unique_hosts sets for each job_type
        def union_hosts(series):
            """Union all sets in the series"""
            result = set()
            for hosts_set in series:
                if isinstance(hosts_set, set):
                    result.update(hosts_set)
                elif hosts_set is not None:
                    result.update(hosts_set)
            return result

        aggregations_by_job_type = (
            dataframe.groupby('job_type')
            .agg(
                dark_total=('dark_total', 'sum'),
                failures_total=('failures_total', 'sum'),
                ok_total=('ok_total', 'sum'),
                skipped_total=('skipped_total', 'sum'),
                ignored_total=('ignored_total', 'sum'),
                rescued_total=('rescued_total', 'sum'),
                unique_hosts=('unique_hosts', union_hosts),
            )
            .reset_index()
            .assign(unique_hosts_total=lambda x: x['unique_hosts'].apply(len))
            .drop(columns=['unique_hosts'])
        )

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregations_by_job_type': aggregations_by_job_type,
            'jobhostsummary_total': jobhostsummary_total,
        }

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_job_type': aggregations_by_job_type.to_dict(orient='records'),
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
