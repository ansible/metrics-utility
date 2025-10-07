class JobHostSummaryAnonymizedRollup:
    """
    Collector - job_host_summary_service collector data
    """

    # TODO - will reuse the jobhostsummary rollup for CCSP
    @staticmethod
    def base(dataframe):
        """
        Avg tasks by template (column job_template_name)
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks executed (ratio between ok and failed tasks (and others))

        Success rate and average - this can compute SaaS team from the metrics
        """

        task_columns = ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']

        dataframe['tasks_executed'] = dataframe[task_columns].sum(axis=1)

        aggregated = (
            dataframe.groupby('job_template_name')
            .agg(
                jobs_total=('job_id', 'nunique'),
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

        return aggregated.to_dict(orient='records')
