from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class JobHostSummaryAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - job_host_summary_service collector data
    """

    def __init__(self):
        super().__init__('job_host_summary')
        self.collector_names = ['job_host_summary_service']

    def base(self, dataframe):
        """
        Avg tasks by template (column job_template_name)
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks executed (ratio between ok and failed tasks (and others))

        Success rate and average - this can compute SaaS team from the metrics
        """

        task_columns = ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']

        # Return empty result if dataframe is empty
        # TODO - ensure all columns are present in the dataframe, then let analysis run with empty data
        if dataframe.empty:
            return {
                'json': [],
                'rollup': {'aggregated': dataframe},
            }

        dataframe['tasks_executed'] = dataframe[task_columns].sum(axis=1)

        aggregated = (
            dataframe.groupby('job_template_name')
            .agg(
                jobs_total=('job_remote_id', 'nunique'),
                dark_total=('dark', 'sum'),
                failures_total=('failures', 'sum'),
                ok_total=('ok', 'sum'),
                skipped_total=('skipped', 'sum'),
                ignored_total=('ignored', 'sum'),
                rescued_total=('rescued', 'sum'),
                hosts_total=('host_name', 'nunique'),
            )
            .reset_index()
        )

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregated': aggregated,
        }

        # Prepare JSON data (converted to list of dicts)
        json_data = aggregated.to_dict(orient='records')

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
