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

        Aggregations grouped by job_type (model):
        - Number of jobs executed
        - Number of jobs failed
        - Number of jobs that succeeded
        - Number of distinct templates

        Job duration maximum seconds - by job_type
        Job duration minimum seconds - by job_type
        Job total seconds by job_type
        The same as above but for waiting times

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

        aggregations_by_job_type = (
            dataframe.groupby('model')
            .agg(
                jobs_total=('id', 'nunique'),
                jobs_failed_total=('failed', 'sum'),
                jobs_never_started_total=('started', lambda x: x.isna().sum()),
                job_duration_maximum_seconds=('job_duration_seconds', 'max'),
                job_duration_minimum_seconds=('job_duration_seconds', 'min'),
                job_duration_total_seconds=('job_duration_seconds', 'sum'),
                job_waiting_time_maximum_seconds=('job_waiting_time_seconds', 'max'),
                job_waiting_time_minimum_seconds=('job_waiting_time_seconds', 'min'),
                job_waiting_time_total_seconds=('job_waiting_time_seconds', 'sum'),
                templates_total=('job_template_name', 'nunique'),
                launch_type_manual_total=('launch_type', lambda x: (x == 'manual').sum()),
                launch_type_relaunch_total=('launch_type', lambda x: (x == 'relaunch').sum()),
                launch_type_callback_total=('launch_type', lambda x: (x == 'callback').sum()),
                launch_type_scheduled_total=('launch_type', lambda x: (x == 'scheduled').sum()),
                launch_type_dependency_total=('launch_type', lambda x: (x == 'dependency').sum()),
                launch_type_workflow_total=('launch_type', lambda x: (x == 'workflow').sum()),
                launch_type_webhook_total=('launch_type', lambda x: (x == 'webhook').sum()),
                launch_type_sync_total=('launch_type', lambda x: (x == 'sync').sum()),
                launch_type_scm_total=('launch_type', lambda x: (x == 'scm').sum()),
                launch_type_api_total=('launch_type', lambda x: (x == 'api').sum()),
                launch_type_system_total=('launch_type', lambda x: (x == 'system').sum()),
                launch_type_unknown_total=('launch_type', lambda x: (x == 'unknown').sum()),
                # inventory name
                inventories_total=('inventory_name', 'nunique'),
                # jobs using projects by scm types
                jobs_using_scm_type_git_total=('scm_type', lambda x: (x == 'git').sum()),
                jobs_using_scm_type_hg_total=('scm_type', lambda x: (x == 'hg').sum()),
                jobs_using_scm_type_svn_total=('scm_type', lambda x: (x == 'svn').sum()),
                jobs_using_scm_type_insights_total=('scm_type', lambda x: (x == 'insights').sum()),
                jobs_using_scm_type_archive_total=('scm_type', lambda x: (x == 'archive').sum()),
                jobs_using_scm_type_manual_total=('scm_type', lambda x: ((x == '') | (x.isna())).sum()),
                jobs_using_scm_type_unknown_total=('scm_type', lambda x: (~x.isin(['git', 'hg', 'svn', 'insights', 'archive', '']) & x.notna()).sum()),
          )
            .reset_index()
            .rename(columns={'model': 'job_type'})
            .assign(jobs_succeeded_total=lambda x: x['jobs_total'] - x['jobs_failed_total'])
        )

        organizations_total = dataframe['organization_name'].nunique()
        ansible_version = dataframe['ansible_version'].iloc[0] if len(dataframe) > 0 else None
        forks_total = int(dataframe['forks'].sum())  # Convert numpy int64 to Python int for JSON serialization
        jobs_total = int(dataframe['id'].nunique())  # Convert numpy int64 to Python int for JSON serialization
        
        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregations_by_job_type': aggregations_by_job_type,
            'organizations_total': organizations_total,
            'ansible_version': ansible_version,
            'forks_total': forks_total,
            'jobs_total': jobs_total,
        }

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_job_type': aggregations_by_job_type.to_dict(orient='records'),
            'organizations_total': organizations_total,
            'ansible_version': ansible_version,
            'forks_total': forks_total,
            'jobs_total': jobs_total,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
