import json

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class JobsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - unified_jobs collector data
    """

    def prepare(self, dataframe):
        # filter out jobs that are not finished
        dataframe = dataframe[dataframe['finished'].notna()]
                
        # Return both the filtered dataframe and collections statistics
        return {
            'jobs': dataframe,
        }
    
    
    def merge(self, data_all, data_new):
        # Simply merge the jobs dataframe
        return pd.concat([data_all['jobs'], data_new['jobs']], ignore_index=True)

    def __init__(self):
        super().__init__('jobs')
        self.collector_names = ['unified_jobs']

    def base(self, data):
        """
        This function will create first level aggregation of the job dataframe, the result is json

        Creates three groupings:
        1. Aggregations grouped by job_type (model):
           - Number of jobs executed
           - Number of jobs failed
           - Number of jobs that succeeded
           - Number of distinct templates
           - Launch type counts (manual, scheduled, etc.)
           - Job duration and waiting time statistics

        2. Aggregations grouped by launch_type:
           - Same statistics as above
           - Job type count (distinct job types per launch type)

        3. Aggregations grouped by ansible_version:
           - Same statistics as above
           - Job type count (distinct job types per ansible version)
           - Launch type counts (manual, scheduled, etc.)

        Job duration maximum seconds - by grouping
        Job duration minimum seconds - by grouping
        Job total seconds by grouping
        The same as above but for waiting times

        Active number of customer by Controller Version - this will be skipped for now
        Active number of Customers - this will be skipped for now
        Active number of Customers (anonymized? - the same as above?) - this will be skipped for now
        Number of templates executed by company - this will be skipped for now

        Also includes installed collections statistics:
        - Collection name and version with job counts

        data is a dict with 'jobs' (DataFrame) and 'collections' (DataFrame)
        """

        # Coerce datetime-like columns to pandas datetimes (timezone-aware if possible)
        # This allows inputs like '2025-09-29 13:16:53.637988+00'
        for col in ['started', 'finished', 'created']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        # Convert failed column to boolean (handle PostgreSQL 't'/'f' representation)
        if 'failed' in dataframe.columns:
            dataframe['failed'] = dataframe['failed'].replace({'t': True, 'f': False}).fillna(False).astype(bool)

        # Normalize ansible_version: treat empty strings as NaN for consistent grouping
        # pandas groupby will group all NaN values together
        if 'ansible_version' in dataframe.columns:
            dataframe['ansible_version'] = dataframe['ansible_version'].replace('', pd.NA)

        # compute job duration in seconds, .dt.total_seconds()
        dataframe['job_duration_seconds'] = (dataframe['finished'] - dataframe['started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['started'] - dataframe['created']).dt.total_seconds()

        # Build common aggregation dictionary shared by both groupings
        common_aggregations = {
            'jobs_total': ('id', 'nunique'),
            'jobs_failed_total': ('failed', 'sum'),
            'jobs_successful_total': ('failed', lambda x: (~x).sum()),
            'jobs_never_started_total': ('started', lambda x: x.isna().sum()),
            'job_duration_maximum_seconds': ('job_duration_seconds', 'max'),
            'job_duration_minimum_seconds': ('job_duration_seconds', 'min'),
            'job_duration_total_seconds': ('job_duration_seconds', 'sum'),
            'jobs_successful_duration_total_seconds': (
                'job_duration_seconds',
                lambda x: x[~dataframe.loc[x.index, 'failed']].sum(),
            ),
            'jobs_failed_duration_total_seconds': (
                'job_duration_seconds',
                lambda x: x[dataframe.loc[x.index, 'failed']].sum(),
            ),
            'job_waiting_time_maximum_seconds': ('job_waiting_time_seconds', 'max'),
            'job_waiting_time_minimum_seconds': ('job_waiting_time_seconds', 'min'),
            'job_waiting_time_total_seconds': ('job_waiting_time_seconds', 'sum'),
            'templates_total': ('job_template_name', 'nunique'),
            # inventory name
            'inventories_total': ('inventory_name', 'nunique'),
            # jobs using projects by scm types
        }

        # Launch type aggregations - reusable across multiple groupings
        launch_type_aggregations = {
            'launch_type_manual_total': ('launch_type', lambda x: (x == 'manual').sum()),
            'launch_type_relaunch_total': ('launch_type', lambda x: (x == 'relaunch').sum()),
            'launch_type_callback_total': ('launch_type', lambda x: (x == 'callback').sum()),
            'launch_type_scheduled_total': ('launch_type', lambda x: (x == 'scheduled').sum()),
            'launch_type_dependency_total': ('launch_type', lambda x: (x == 'dependency').sum()),
            'launch_type_workflow_total': ('launch_type', lambda x: (x == 'workflow').sum()),
            'launch_type_webhook_total': ('launch_type', lambda x: (x == 'webhook').sum()),
            'launch_type_sync_total': ('launch_type', lambda x: (x == 'sync').sum()),
            'launch_type_scm_total': ('launch_type', lambda x: (x == 'scm').sum()),
            'launch_type_api_total': ('launch_type', lambda x: (x == 'api').sum()),
            'launch_type_system_total': ('launch_type', lambda x: (x == 'system').sum()),
            'launch_type_unknown_total': ('launch_type', lambda x: (x == 'unknown').sum()),
        }

        # Ansible versions aggregation - array of unique versions
        ansible_versions_aggregation = {
            'ansible_versions': ('ansible_version', lambda x: sorted([str(v) for v in x.dropna().unique() if pd.notna(v)])),
        }

        # Aggregations grouped by job_type (model)
        # Add launch_type counts specific to job_type grouping
        aggregations_by_job_type_dict = common_aggregations.copy()
        aggregations_by_job_type_dict.update(launch_type_aggregations)
        aggregations_by_job_type_dict.update(ansible_versions_aggregation)

        aggregations_by_job_type = (
            dataframe.groupby('model')
            .agg(**aggregations_by_job_type_dict)
            .reset_index()
            .rename(columns={'model': 'job_type'})
        )

        # Aggregations grouped by launch_type
        # Add job_type_total specific to launch_type grouping
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict.update({
            'job_type_total': ('model', 'nunique'),  # Count distinct job types instead of launch types
        })
        aggregations_by_launch_type_dict.update(ansible_versions_aggregation)

        aggregations_by_launch_type = (
            dataframe.groupby('launch_type')
            .agg(**aggregations_by_launch_type_dict)
            .reset_index()
        )

        # Aggregations grouped by ansible_version
        # Add both job_type_total and launch_type counts (since we're grouping by ansible_version)
        aggregations_by_ansible_version_dict = common_aggregations.copy()
        aggregations_by_ansible_version_dict.update({
            'job_type_total': ('model', 'nunique'),  # Count distinct job types
        })
        aggregations_by_ansible_version_dict.update(launch_type_aggregations)

        aggregations_by_ansible_version = (
            dataframe.groupby('ansible_version')
            .agg(**aggregations_by_ansible_version_dict)
            .reset_index()
        )

        organizations_total = dataframe['organization_name'].nunique()
        ansible_version = dataframe['ansible_version'].iloc[0] if len(dataframe) > 0 else None
        forks_total = int(dataframe['forks'].sum())  # Convert numpy int64 to Python int for JSON serialization
        jobs_total = int(dataframe['id'].nunique())  # Convert numpy int64 to Python int for JSON serialization

        # Process collections statistics
        collections_stats = self._process_collections(collections_df)

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregations_by_job_type': aggregations_by_job_type,
            'aggregations_by_launch_type': aggregations_by_launch_type,
            'aggregations_by_ansible_version': aggregations_by_ansible_version,
            'organizations_total': organizations_total,
            'ansible_version': ansible_version,
            'forks_total': forks_total,
            'jobs_total': jobs_total,
            'installed_collections': collections_df,  # DataFrame with collection statistics
        }

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_job_type': aggregations_by_job_type.to_dict(orient='records'),
            'by_launch_type': aggregations_by_launch_type.to_dict(orient='records'),
            'by_ansible_version': aggregations_by_ansible_version.to_dict(orient='records'),
            'organizations_total': organizations_total,
            'ansible_version': ansible_version,
            'forks_total': forks_total,
            'jobs_total': jobs_total,
            'installed_collections': collections_stats,  # List of dicts with collection statistics
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
