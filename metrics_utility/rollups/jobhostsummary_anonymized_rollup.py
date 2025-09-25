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

        dataframe['executed'] = dataframe[task_columns].sum(axis=1)

        total_jobs_per_template = dataframe.groupby('job_template_name')['job_id'].nunique().reset_index(name='total_jobs')
        total_dark_per_template = dataframe.groupby('job_template_name')['dark'].sum().reset_index(name='total_dark')
        total_failures_per_template = dataframe.groupby('job_template_name')['failures'].sum().reset_index(name='total_failures')
        total_ok_per_template = dataframe.groupby('job_template_name')['ok'].sum().reset_index(name='total_ok')
        total_skipped_per_template = dataframe.groupby('job_template_name')['skipped'].sum().reset_index(name='total_skipped')
        total_ignored_per_template = dataframe.groupby('job_template_name')['ignored'].sum().reset_index(name='total_ignored')
        total_rescued_per_template = dataframe.groupby('job_template_name')['rescued'].sum().reset_index(name='total_rescued')

        average_executed_per_template = dataframe.groupby('job_template_name')['executed'].mean().reset_index(name='average_executed')

        return {
            'total_jobs_per_template': total_jobs_per_template.to_dict(orient='records'),
            'total_dark_per_template': total_dark_per_template.to_dict(orient='records'),
            'total_failures_per_template': total_failures_per_template.to_dict(orient='records'),
            'total_ok_per_template': total_ok_per_template.to_dict(orient='records'),
            'total_skipped_per_template': total_skipped_per_template.to_dict(orient='records'),
            'total_ignored_per_template': total_ignored_per_template.to_dict(orient='records'),
            'total_rescued_per_template': total_rescued_per_template.to_dict(orient='records'),
            'average_executed_per_template': average_executed_per_template.to_dict(orient='records'),
        }
