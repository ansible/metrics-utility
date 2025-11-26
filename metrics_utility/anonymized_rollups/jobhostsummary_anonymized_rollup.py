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
        Override merge to handle the new structure with jobhostsummary_count and aggregated data.
        Concatenates aggregated dataframes and sums jobhostsummary_count.
        """
        # Handle initial empty DataFrame case (first iteration from load_anonymized_rollup_data)
        if isinstance(data_all, pd.DataFrame) and data_all.empty:
            return data_new

        # Handle initial empty dict case
        if isinstance(data_all, dict) and not data_all:
            return data_new

        # Concatenate aggregated dataframes and sum jobhostsummary_counts
        return {
            'jobhostsummary_count': data_all['jobhostsummary_count'] + data_new['jobhostsummary_count'],
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

    def prepare(self, dataframe):
        # Count all records before processing
        jobhostsummary_count = len(dataframe)

        # Aggregate by job_template_name and host_name to reduce data volume early
        # This significantly improves performance when processing large batches
        if dataframe.empty:
            return {
                'jobhostsummary_count': jobhostsummary_count,
                'aggregated': dataframe,
            }

        # Group by job_template_name and host_name, sum task columns, count jobs
        aggregated = (
            dataframe.groupby(['job_template_name'])
            .agg(
                dark_total=('dark', 'sum'),
                failures_total=('failures', 'sum'),
                ok_total=('ok', 'sum'),
                skipped_total=('skipped', 'sum'),
                ignored_total=('ignored', 'sum'),
                rescued_total=('rescued', 'sum'),
                # keep unique hosts as set
                unique_hosts=('host_name', lambda x: set(x)),
            )
            .reset_index()
        )

        return {
            'jobhostsummary_count': jobhostsummary_count,
            'aggregated': aggregated,
        }

    def base(self, data):
        """
        Avg tasks by template (column job_template_name)
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks executed (ratio between ok and failed tasks (and others))

        Success rate and average - this can compute SaaS team from the metrics

        data is a dict with 'jobhostsummary_count' and 'aggregated' dataframe
        """

        # Extract jobhostsummary_count and aggregated dataframe from the data structure
        jobhostsummary_count = data.get('jobhostsummary_count', 0)
        dataframe = data.get('aggregated', pd.DataFrame())

        # Return empty result if dataframe is empty
        # TODO - ensure all columns are present in the dataframe, then let analysis run with empty data
        if dataframe.empty:
            return {
                'json': {'jobhostsummary_count': jobhostsummary_count},
                'rollup': {'aggregated': dataframe, 'jobhostsummary_count': jobhostsummary_count},
            }

        # Re-aggregate in case multiple batches had overlapping template+host combinations
        aggregated = (
            dataframe.groupby(['job_template_name'])
            .agg(
                dark_total=('dark_total', 'sum'),
                failures_total=('failures_total', 'sum'),
                ok_total=('ok_total', 'sum'),
                skipped_total=('skipped_total', 'sum'),
                ignored_total=('ignored_total', 'sum'),
                rescued_total=('rescued_total', 'sum'),
                unique_hosts=('unique_hosts', lambda x: set().union(*x)),
            )
            .reset_index()
        )

        total_unique_hosts = set().union(*aggregated['unique_hosts'])
        # drop unique_hosts column
        aggregated = aggregated.drop(columns=['unique_hosts'])

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregated': aggregated,
            'jobhostsummary_count': jobhostsummary_count,
        }

        # Prepare JSON data (converted to list of dicts)
        # json_data = aggregated.to_dict(orient='records')

        json_data = {
            'total_unique_hosts': len(total_unique_hosts),
            'aggregated': aggregated.to_dict(orient='records'),
            'jobhostsummary_count': jobhostsummary_count,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
