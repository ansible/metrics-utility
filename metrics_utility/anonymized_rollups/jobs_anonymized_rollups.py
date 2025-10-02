class Jobs_Anonymized_Rollups:
    """
    Collector - unified_jobs collector data
    """

    @staticmethod
    def base(dataframe):
        """
        This function will create first level aggregation of the job dataframe, the result is json

        Number of jobs executed
        Number of jobs failed
        Number of jobs that succeeded

        Job duration average in seconds - by template
        Job duration maximum seconds- by template
        Job duration minimum seconds - by template
        Job total seconds by template
        The same as above but for waiting times
        Number of jobs by template

        Active number of customer by Controller Version - this will be skipped for now
        Active number of Customers - this will be skipped for now
        Active number of Customers (anonymized? - the same as above?) - this will be skipped for now
        Number of templates executed by company - this will be skipped for now
        """

        # create view from dataframe where finished is not null and started is not null
        dataframe = dataframe[dataframe['finished'].notna() & dataframe['started'].notna()]

        # compute job duration in seconds
        dataframe['duration'] = (dataframe['finished'] - dataframe['started']) / 1000
        dataframe['waiting_time'] = (dataframe['started'] - dataframe['job_created']) / 1000

        aggregations_by_template = dataframe.groupby('job_template_name').agg(
            number_of_jobs_executed=('job_id', 'nunique'),
            number_of_jobs_failed=('job_failed', 'sum'),
            job_duration_average_in_seconds=('duration', 'mean'),
            job_duration_maximum_in_seconds=('duration', 'max'),
            job_duration_minimum_in_seconds=('duration', 'min'),
            job_duration_total_in_seconds=('duration', 'sum'),
            job_duration_median_in_seconds=('duration', 'median'),
            job_waiting_time_average_in_seconds=('waiting_time', 'mean'),
            job_waiting_time_maximum_in_seconds=('waiting_time', 'max'),
            job_waiting_time_minimum_in_seconds=('waiting_time', 'min'),
            job_waiting_time_total_in_seconds=('waiting_time', 'sum'),
            job_waiting_time_median_in_seconds=('waiting_time', 'median'),
        )

        # return as object that can be converted to json
        return aggregations_by_template.to_dict(orient='records')
