class Jobs_Anonymized_Rollup:
    """
    Collector - unified_jobs collector data
    """

    def base(dataframe):
        """
        Avg tasks by template (column job_template_name)
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks exectuted (ratio between ok and failed tasks (and others))
        """

        task_columns = [
            "changed", "dark", "failures", "ok",
            "processed", "skipped", "failed",
            "ignored", "rescued"
        ]

        df["total_tasks"] = df[task_columns].sum(axis=1)

        avg_tasks_by_template = (
            df.groupby("job_template_name")["total_tasks"]
            .mean()
            .reset_index(name="avg_tasks")
        )

        
        