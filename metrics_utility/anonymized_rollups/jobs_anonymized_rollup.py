import pandas as pd


class JobsAnonymizedRollups:
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

        dataframe corresponds to jobs
        """

        # Coerce datetime-like columns to pandas datetimes (timezone-aware if possible)
        # This allows inputs like '2025-09-29 13:16:53.637988+00'
        for col in ['started', 'finished', 'job_created']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        # create view from dataframe where finished is not null and started is not null
        dataframe = dataframe[dataframe['finished'].notna() & dataframe['started'].notna()]

        # compute job duration in seconds, .dt.total_seconds()
        dataframe['job_duration_seconds'] = (dataframe['finished'] - dataframe['started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['started'] - dataframe['job_created']).dt.total_seconds()

        # guard against negative times
        dataframe = dataframe[dataframe['job_duration_seconds'] >= 0]
        dataframe = dataframe[dataframe['job_waiting_time_seconds'] >= 0]

        aggregations_by_template = (
            dataframe.groupby('job_template_name')
            .agg(
                number_of_jobs_executed=('id', 'nunique'),
                number_of_jobs_failed=('failed', 'sum'),
                job_duration_average_in_seconds=('job_duration_seconds', 'mean'),
                job_duration_maximum_in_seconds=('job_duration_seconds', 'max'),
                job_duration_minimum_in_seconds=('job_duration_seconds', 'min'),
                job_duration_total_in_seconds=('job_duration_seconds', 'sum'),
                job_duration_median_in_seconds=('job_duration_seconds', 'median'),
                job_waiting_time_average_in_seconds=('job_waiting_time_seconds', 'mean'),
                job_waiting_time_maximum_in_seconds=('job_waiting_time_seconds', 'max'),
                job_waiting_time_minimum_in_seconds=('job_waiting_time_seconds', 'min'),
                job_waiting_time_total_in_seconds=('job_waiting_time_seconds', 'sum'),
                job_waiting_time_median_in_seconds=('job_waiting_time_seconds', 'median'),
            )
            .reset_index()
            .assign(number_of_jobs_succeeded=lambda x: x['number_of_jobs_executed'] - x['number_of_jobs_failed'])
        )

        # return as object that can be converted to json
        return aggregations_by_template.to_dict(orient='records')
