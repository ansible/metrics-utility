class JobHostSummary_Anonymized_Rollup:
    """
    Collector - job_host_summary_service collector data
    """

    # TODO - will probably reuse the jobhostsummary CCSP rollup
    @staticmethod
    def base(dataframe):
        """
        Avg tasks by template (column job_template_name)
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks executed (ratio between ok and failed tasks (and others))
        """

        task_columns = ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']

        dataframe['tasks_executed'] = dataframe[task_columns].sum(axis=1)

        aggregated = (
            dataframe.groupby('job_template_name')
            .agg(
                total_jobs=('job_id', 'nunique'),
                total_dark=('dark', 'sum'),
                total_failures=('failures', 'sum'),
                total_ok=('ok', 'sum'),
                total_skipped=('skipped', 'sum'),
                total_ignored=('ignored', 'sum'),
                total_rescued=('rescued', 'sum'),
                average_tasks_executed=('tasks_executed', 'mean'),
            )
            .reset_index()
        )

        return aggregated.to_dict(orient='records')
