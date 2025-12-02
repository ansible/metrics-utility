import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class JobsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - unified_jobs collector data
    """

    def prepare(self, dataframe):
        # filter out jobs that are not finished
        dataframe = dataframe[dataframe['finished'].notna()]
        return dataframe

    def __init__(self):
        super().__init__('jobs')
        self.collector_names = ['unified_jobs']

    def base(self, dataframe):
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

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            return {
                'json': {},
                'rollup': {'aggregated': dataframe},
            }

        # Coerce datetime-like columns to pandas datetimes (timezone-aware if possible)
        # This allows inputs like '2025-09-29 13:16:53.637988+00'
        for col in ['started', 'finished', 'created']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        # Convert failed column to boolean (handle PostgreSQL 't'/'f' representation)
        if 'failed' in dataframe.columns:
            dataframe['failed'] = dataframe['failed'].replace({'t': True, 'f': False}).fillna(False).astype(bool)

        # compute job duration in seconds, .dt.total_seconds()
        dataframe['job_duration_seconds'] = (dataframe['finished'] - dataframe['started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['started'] - dataframe['created']).dt.total_seconds()

        jobs_total = dataframe['id'].nunique()

        aggregations_by_template = (
            dataframe.groupby('job_template_name')
            .agg(
                number_of_jobs_executed=('id', 'nunique'),
                number_of_jobs_failed=('failed', 'sum'),
                number_of_jobs_never_started=('started', lambda x: x.isna().sum()),
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

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregations_by_template': aggregations_by_template,
        }

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_template': aggregations_by_template.to_dict(orient='records'),
            'jobs_total': jobs_total,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
