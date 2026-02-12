import json

from collections import Counter

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
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Ensure data_all is a dict with 'jobs' key
        # Handle case where data_all might be a DataFrame (backward compatibility)
        if isinstance(data_all, pd.DataFrame):
            data_all = {'jobs': data_all}

        # Simply merge the jobs dataframe
        return {'jobs': pd.concat([data_all['jobs'], data_new['jobs']], ignore_index=True)}

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

        3. Aggregations grouped by controller_version:
           - Same statistics as above
           - Job type count (distinct job types per controller version)
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

        data is a dict with 'jobs' (DataFrame) or None if no data
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_controller_version': [],
                    'organizations_total': None,
                    'forks_total': None,
                    'jobs_total': None,
                    'installed_collections': [],
                    'scm_types': [],
                },
                'rollup': {
                    'aggregations_by_job_type': pd.DataFrame(),
                    'aggregations_by_launch_type': pd.DataFrame(),
                    'aggregations_by_controller_version': pd.DataFrame(),
                    'organizations_total': None,
                    'forks_total': None,
                    'jobs_total': None,
                    'installed_collections': pd.DataFrame(),
                },
            }

        # Extract jobs dataframe from data
        dataframe = data.get('jobs', pd.DataFrame())

        # Handle empty dataframe
        if dataframe.empty:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_controller_version': [],
                    'organizations_total': 0,
                    'forks_total': 0,
                    'jobs_total': 0,
                    'installed_collections': [],
                    'scm_types': [],
                },
                'rollup': {
                    'aggregations_by_job_type': pd.DataFrame(),
                    'aggregations_by_launch_type': pd.DataFrame(),
                    'aggregations_by_controller_version': pd.DataFrame(),
                    'organizations_total': 0,
                    'forks_total': 0,
                    'jobs_total': 0,
                    'installed_collections': pd.DataFrame(),
                },
            }

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
        # Note: We keep ansible_version in the dataframe (as collected from SQL), but rename it to controller_version in output
        if 'ansible_version' in dataframe.columns:
            dataframe['ansible_version'] = dataframe['ansible_version'].replace('', pd.NA)

        # compute job duration in seconds, .dt.total_seconds()
        dataframe['job_duration_seconds'] = (dataframe['finished'] - dataframe['started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['started'] - dataframe['created']).dt.total_seconds()

        # Pre-compute boolean columns for efficient aggregations (avoids lambda functions)
        dataframe['jobs_successful'] = ~dataframe['failed']
        dataframe['jobs_never_started'] = dataframe['started'].isna()
        dataframe['job_duration_successful_seconds'] = dataframe['job_duration_seconds'].where(dataframe['jobs_successful'], 0)
        dataframe['job_duration_failed_seconds'] = dataframe['job_duration_seconds'].where(dataframe['failed'], 0)

        # Pre-compute launch_type boolean columns for vectorized aggregations
        launch_types = ['manual', 'relaunch', 'callback', 'scheduled', 'dependency', 'workflow', 'webhook', 'sync', 'scm', 'api', 'system', 'unknown']
        for launch_type in launch_types:
            dataframe[f'launch_type_{launch_type}'] = dataframe['launch_type'] == launch_type

        # Build common aggregation dictionary shared by both groupings
        # Using pre-computed columns instead of lambdas for better performance
        common_aggregations = {
            'jobs_total': ('id', 'nunique'),
            'jobs_failed_total': ('failed', 'sum'),
            'jobs_successful_total': ('jobs_successful', 'sum'),
            'jobs_never_started_total': ('jobs_never_started', 'sum'),
            'job_duration_maximum_seconds': ('job_duration_seconds', 'max'),
            'job_duration_minimum_seconds': ('job_duration_seconds', 'min'),
            'jobs_duration_total_seconds': ('job_duration_seconds', 'sum'),
            'jobs_successful_duration_total_seconds': ('job_duration_successful_seconds', 'sum'),
            'jobs_failed_duration_total_seconds': ('job_duration_failed_seconds', 'sum'),
            'job_waiting_time_maximum_seconds': ('job_waiting_time_seconds', 'max'),
            'job_waiting_time_minimum_seconds': ('job_waiting_time_seconds', 'min'),
            'job_waiting_time_total_seconds': ('job_waiting_time_seconds', 'sum'),
            'templates_total': ('job_template_name', 'nunique'),
            # inventory name
            'inventories_total': ('inventory_name', 'nunique'),
            # jobs using projects by scm types
        }

        # Launch type aggregations - using pre-computed boolean columns for vectorized operations
        launch_type_aggregations = {
            'launch_type_manual_total': ('launch_type_manual', 'sum'),
            'launch_type_relaunch_total': ('launch_type_relaunch', 'sum'),
            'launch_type_callback_total': ('launch_type_callback', 'sum'),
            'launch_type_scheduled_total': ('launch_type_scheduled', 'sum'),
            'launch_type_dependency_total': ('launch_type_dependency', 'sum'),
            'launch_type_workflow_total': ('launch_type_workflow', 'sum'),
            'launch_type_webhook_total': ('launch_type_webhook', 'sum'),
            'launch_type_sync_total': ('launch_type_sync', 'sum'),
            'launch_type_scm_total': ('launch_type_scm', 'sum'),
            'launch_type_api_total': ('launch_type_api', 'sum'),
            'launch_type_system_total': ('launch_type_system', 'sum'),
            'launch_type_unknown_total': ('launch_type_unknown', 'sum'),
        }

        # Controller versions aggregation - optimized to avoid lambda
        # We'll compute this separately after groupby for better performance
        def get_controller_versions(grouped_series):
            """Helper function to extract sorted unique controller versions from a group"""
            unique_versions = grouped_series.dropna().unique()
            return sorted([str(v) for v in unique_versions if pd.notna(v)])

        controller_versions_aggregation = {
            'controller_versions': ('ansible_version', get_controller_versions),
        }

        # Aggregations grouped by job_type (model)
        # Add launch_type counts specific to job_type grouping
        aggregations_by_job_type_dict = common_aggregations.copy()
        aggregations_by_job_type_dict.update(launch_type_aggregations)
        aggregations_by_job_type_dict.update(controller_versions_aggregation)

        aggregations_by_job_type = dataframe.groupby('model').agg(**aggregations_by_job_type_dict).reset_index().rename(columns={'model': 'job_type'})

        # Add is_automation field: True if job_type is 'job', False otherwise
        aggregations_by_job_type['is_automation'] = aggregations_by_job_type['job_type'] == 'job'

        # Aggregations grouped by launch_type
        # Add job_type_total specific to launch_type grouping
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict.update(
            {
                'job_type_total': ('model', 'nunique'),  # Count distinct job types instead of launch types
            }
        )
        aggregations_by_launch_type_dict.update(controller_versions_aggregation)

        aggregations_by_launch_type = dataframe.groupby('launch_type').agg(**aggregations_by_launch_type_dict).reset_index()

        # Aggregations grouped by controller_version
        # Add both job_type_total and launch_type counts (since we're grouping by controller_version)
        aggregations_by_controller_version_dict = common_aggregations.copy()
        aggregations_by_controller_version_dict.update(
            {
                'job_type_total': ('model', 'nunique'),  # Count distinct job types
            }
        )
        aggregations_by_controller_version_dict.update(launch_type_aggregations)

        aggregations_by_controller_version = (
            dataframe.groupby('ansible_version')
            .agg(**aggregations_by_controller_version_dict)
            .reset_index()
            .rename(columns={'ansible_version': 'controller_version'})
        )

        organizations_total = dataframe['organization_name'].nunique()
        forks_total = int(dataframe['forks'].sum())  # Convert numpy int64 to Python int for JSON serialization
        jobs_total = int(dataframe['id'].nunique())  # Convert numpy int64 to Python int for JSON serialization

        # Extract unique scm_type values from dataframe
        scm_types = []
        if 'scm_type' in dataframe.columns:
            scm_types = sorted([str(v) for v in dataframe['scm_type'].dropna().unique() if pd.notna(v) and str(v).strip()])

        # Process collections statistics from jobs dataframe
        collections_stats = self._process_collections_from_jobs(dataframe)

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_job_type': aggregations_by_job_type.to_dict(orient='records'),
            'by_launch_type': aggregations_by_launch_type.to_dict(orient='records'),
            'by_controller_version': aggregations_by_controller_version.to_dict(orient='records'),
            'organizations_total': organizations_total,
            'forks_total': forks_total,
            'jobs_total': jobs_total,
            'installed_collections': collections_stats,  # List of dicts with collection statistics
            'scm_types': scm_types,  # List of unique scm_type values
        }

        return {
            'json': json_data,
        }

    def _process_collections_from_jobs(self, dataframe):
        """
        Extract unique collection name and version pairs from jobs dataframe.
        Count how many jobs use each unique collection+version combination.

        Optimized version using itertuples() and Counter for better performance.

        Returns a list of dicts with:
        - collection_name: str
        - collection_version: str
        - job_count: int
        """
        if 'installed_collections' not in dataframe.columns:
            return []

        # Use Counter for efficient counting
        collections_counter = Counter()

        # Use itertuples() for fastest row iteration (10-100x faster than iterrows)
        # itertuples() creates namedtuples with column names as attributes
        # Column names with special characters are sanitized, but 'installed_collections' should work fine
        for row in dataframe.itertuples(index=False):
            installed_collections_data = getattr(row, 'installed_collections', None)

            # Skip if missing or empty (fast check)
            if pd.isna(installed_collections_data) or not installed_collections_data:
                continue

            # Parse JSON string if needed
            try:
                if isinstance(installed_collections_data, str):
                    collections_data = json.loads(installed_collections_data)
                elif isinstance(installed_collections_data, dict):
                    collections_data = installed_collections_data
                else:
                    continue
            except (json.JSONDecodeError, TypeError):
                continue

            # Extract collection name and version pairs
            if not isinstance(collections_data, dict):
                continue

            # Process all collections for this job
            for collection_name, collection_info in collections_data.items():
                if not isinstance(collection_info, dict):
                    continue

                version = collection_info.get('version', '')
                if not version:
                    continue

                # Use Counter for efficient counting
                collections_counter[(collection_name, str(version))] += 1

        # Convert Counter to list of dicts
        collections_stats = [
            {
                'collection_name': collection_name,
                'collection_version': collection_version,
                'job_count': job_count,
            }
            for (collection_name, collection_version), job_count in collections_counter.items()
        ]

        # Sort by collection_name, then by collection_version for consistent output
        collections_stats.sort(key=lambda x: (x['collection_name'], x['collection_version']))

        return collections_stats
