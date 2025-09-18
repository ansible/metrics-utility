class JobHostSummary_Anonymized_Rollup:
    """
    Collector - job_host_summary_service collector data
    """

    def base(dataframe):
        """
        Avg tasks by template (column job_template_name)
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks exectuted (ratio between ok and failed tasks (and others))
        """

        task_columns = [
            'dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued'
        ]

        dataframe["executed"] = dataframe[task_columns].sum(axis=1)

        avg_tasks_by_template = (
            dataframe.groupby("job_template_name")["executed"]
            .mean()
            .reset_index(name="avg_tasks")
        )

        average_tasks_over_all_templates = avg_tasks_by_template["avg_tasks"].mean()

        total_tasks_executed = dataframe["executed"].sum()

        success_ratio = dataframe["ok"] / dataframe["executed"]

        # avg tasks by template should be converted to json
        return {
            'average_tasks_over_all_templates': average_tasks_over_all_templates,
            'total_tasks_executed': total_tasks_executed,
            'avg_tasks_by_template': avg_tasks_by_template.to_dict(orient='records'),
            'success_ratio': success_ratio.mean()
        }










        
        