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
        
        # Extract installed collections statistics from this batch
        collections_df = self._extract_collections_from_batch(dataframe)
        
        # Return both the filtered dataframe and collections statistics
        return {
            'jobs': dataframe,
            'collections': collections_df,
        }
    
    def _extract_collections_from_batch(self, dataframe):
        """
        Extract collection-version pairs from installed_collections JSON field.
        Returns a DataFrame with columns: collection_name, collection_version, job_count
        where job_count is the number of jobs in this batch that had this collection-version.
        """
        if dataframe is None or dataframe.empty or 'installed_collections' not in dataframe.columns:
            return pd.DataFrame(columns=['collection_name', 'collection_version', 'job_count'])
        
        collection_rows = []
        
        for idx, row in dataframe.iterrows():
            installed_collections = row.get('installed_collections')
            
            # Skip if installed_collections is None, empty, or not a valid JSON
            if pd.isna(installed_collections) or installed_collections == '':
                continue
            
            # Parse JSON if it's a string, otherwise use as-is
            try:
                if isinstance(installed_collections, str):
                    collections_dict = json.loads(installed_collections)
                else:
                    collections_dict = installed_collections
                
                # Handle case where it's already a dict (from pandas JSON parsing)
                if not isinstance(collections_dict, dict):
                    continue
                
                # Extract collection name and version
                for collection_name, collection_info in collections_dict.items():
                    if isinstance(collection_info, dict) and 'version' in collection_info:
                        collection_version = collection_info['version']
                        collection_rows.append({
                            'collection_name': collection_name,
                            'collection_version': collection_version,
                            'job_count': 1,  # Each job contributes 1 to the count
                        })
            except (json.JSONDecodeError, TypeError, AttributeError):
                # Skip invalid JSON
                continue
        
        if not collection_rows:
            return pd.DataFrame(columns=['collection_name', 'collection_version', 'job_count'])
        
        # Create DataFrame and aggregate by collection_name and collection_version
        collections_df = pd.DataFrame(collection_rows)
        collections_agg = (
            collections_df.groupby(['collection_name', 'collection_version'])
            .agg(job_count=('job_count', 'sum'))
            .reset_index()
        )
        
        return collections_agg
    
    def _process_collections(self, collections_df):
        """
        Process collections dataframe and return as list of dicts for JSON output.
        Collections are already aggregated (summed) from all batches.
        """
        if collections_df is None or collections_df.empty:
            return []
        
        # Convert to list of dicts, ensuring job_count is an int for JSON serialization
        collections_list = collections_df.copy()
        collections_list['job_count'] = collections_list['job_count'].astype(int)
        return collections_list.to_dict(orient='records')
    
    def merge(self, data_all, data_new):
        """
        Override merge to handle both jobs dataframe and collections dataframe.
        """
        # Handle initial None case
        if data_all is None:
            return data_new
        
        # Merge jobs dataframes
        merged_jobs = pd.concat([data_all['jobs'], data_new['jobs']], ignore_index=True)
        
        # Merge collections dataframes and sum job counts
        merged_collections = pd.concat([data_all['collections'], data_new['collections']], ignore_index=True)
        if not merged_collections.empty:
            merged_collections = (
                merged_collections.groupby(['collection_name', 'collection_version'])
                .agg(job_count=('job_count', 'sum'))
                .reset_index()
            )
        else:
            merged_collections = pd.DataFrame(columns=['collection_name', 'collection_version', 'job_count'])
        
        return {
            'jobs': merged_jobs,
            'collections': merged_collections,
        }

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

        # Extract jobs dataframe and collections dataframe
        if isinstance(data, dict):
            dataframe = data.get('jobs', pd.DataFrame())
            collections_df = data.get('collections', pd.DataFrame())
        else:
            # Backward compatibility: if data is a DataFrame directly, use it
            dataframe = data
            collections_df = pd.DataFrame(columns=['collection_name', 'collection_version', 'job_count'])

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            # Still process collections if available
            collections_stats = self._process_collections(collections_df)
            return {
                'json': {'installed_collections': collections_stats},
                'rollup': {
                    'aggregated': pd.DataFrame(),
                    'installed_collections': collections_df,
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
            'jobs_using_scm_type_git_total': ('scm_type', lambda x: (x == 'git').sum()),
            'jobs_using_scm_type_hg_total': ('scm_type', lambda x: (x == 'hg').sum()),
            'jobs_using_scm_type_svn_total': ('scm_type', lambda x: (x == 'svn').sum()),
            'jobs_using_scm_type_insights_total': ('scm_type', lambda x: (x == 'insights').sum()),
            'jobs_using_scm_type_archive_total': ('scm_type', lambda x: (x == 'archive').sum()),
            'jobs_using_scm_type_manual_total': ('scm_type', lambda x: ((x == '') | (x.isna())).sum()),
            'jobs_using_scm_type_unknown_total': (
                'scm_type',
                lambda x: (~x.isin(['git', 'hg', 'svn', 'insights', 'archive', '']) & x.notna()).sum(),
            ),
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

        # Aggregations grouped by job_type (model)
        # Add launch_type counts specific to job_type grouping
        aggregations_by_job_type_dict = common_aggregations.copy()
        aggregations_by_job_type_dict.update(launch_type_aggregations)

        aggregations_by_job_type = (
            dataframe.groupby('model')
            .agg(**aggregations_by_job_type_dict)
            .reset_index()
            .rename(columns={'model': 'job_type'})
            .assign(jobs_succeeded_total=lambda x: x['jobs_total'] - x['jobs_failed_total'])
        )

        # Aggregations grouped by launch_type
        # Add job_type_total specific to launch_type grouping
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict.update({
            'job_type_total': ('model', 'nunique'),  # Count distinct job types instead of launch types
        })

        aggregations_by_launch_type = (
            dataframe.groupby('launch_type')
            .agg(**aggregations_by_launch_type_dict)
            .reset_index()
            .assign(jobs_succeeded_total=lambda x: x['jobs_total'] - x['jobs_failed_total'])
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
            .assign(jobs_succeeded_total=lambda x: x['jobs_total'] - x['jobs_failed_total'])
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
