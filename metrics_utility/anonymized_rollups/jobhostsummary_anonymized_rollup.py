import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class JobHostSummaryAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - job_host_summary_service collector data
    """

    def __init__(self):
        super().__init__('job_host_summary')
        self.collector_names = ['job_host_summary_service']

    def merge(self, data_all, data_new):
        """
        Override merge to handle the new structure with job_host_pairs_total and aggregated data.
        Concatenates aggregated dataframes and sums job_host_pairs_total.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Concatenate aggregated dataframes and sum job_host_pairs_totals
        return {
            'job_host_pairs_total': data_all['job_host_pairs_total'] + data_new['job_host_pairs_total'],
            'aggregated': pd.concat([data_all['aggregated'], data_new['aggregated']], ignore_index=True),
        }

    # prepare is called for each batch of data
    # result of prepare is concatenated with other batches into one dataframe
    # each dataframe in prepare should reduce the number of rows as much as possible
    # dataframe has:
    # job_remote_id
    # job_template_name
    # host_name
    # dark
    # failures
    # ok
    # skipped
    # ignored
    # rescued
    # model (job_type)
    # controller_version
    # launch_type

    def prepare(self, dataframe):
        # Count all records before processing
        job_host_pairs_total = len(dataframe)

        # Group by job_type (model), launch_type, and controller_version and sum task columns to reduce data volume early
        # This significantly improves performance when processing large batches
        if dataframe.empty:
            return {
                'job_host_pairs_total': job_host_pairs_total,
                'aggregated': dataframe,
            }

        # Check if job_remote_id column exists
        if 'job_remote_id' not in dataframe.columns:
            # If job_remote_id is missing, create a default value
            dataframe['job_remote_id'] = None

        # Check if model column exists (for backward compatibility)
        if 'model' not in dataframe.columns:
            # If model is missing, create a default 'unknown' value
            dataframe['model'] = 'unknown'

        # Normalize ansible_version: treat empty strings as NaN for consistent grouping
        # Note: We keep ansible_version in the dataframe (as collected from SQL), but rename it to controller_version in output
        if 'ansible_version' in dataframe.columns:
            dataframe['ansible_version'] = dataframe['ansible_version'].replace('', pd.NA)
        else:
            dataframe['ansible_version'] = pd.NA

        # Handle launch_type: if missing, set to 'unknown'
        if 'launch_type' not in dataframe.columns:
            dataframe['launch_type'] = 'unknown'

        # rename column ansible_version to controller_version
        dataframe.rename(columns={'ansible_version': 'controller_version'}, inplace=True)

        dataframe['host_outcome'] = 'successful'
        dataframe.loc[dataframe['failures'] > 0, 'host_outcome'] = 'failed'
        dataframe.loc[dataframe['dark'] > 0, 'host_outcome'] = 'unreachable'

        # Group by job_remote_id, model, launch_type, and ansible_version to preserve all dimensions
        # Note: We keep ansible_version in the dataframe (as collected from SQL), but rename it to controller_version in output
        # This allows us to aggregate by each dimension separately in base() while tracking jobs
        aggregated = (
            dataframe.groupby(['job_remote_id', 'model', 'launch_type', 'ansible_version'])
            .agg(
                dark_total=('dark', 'sum'),
                failures_total=('failures', 'sum'),
                ok_total=('ok', 'sum'),
                skipped_total=('skipped', 'sum'),
                ignored_total=('ignored', 'sum'),
                rescued_total=('rescued', 'sum'),
                unique_hosts=('host_name', lambda x: set(x)),
                hosts_successful_total=('host_outcome', lambda x: (x == 'successful').sum()),
                hosts_failed_total=('host_outcome', lambda x: (x == 'failed').sum()),
                hosts_unreachable_total=('host_outcome', lambda x: (x == 'unreachable').sum()),
            )
            .reset_index()
            .rename(columns={'model': 'job_type'})  # Keep ansible_version in dataframe, rename to controller_version in output
        )

        return {
            'job_host_pairs_total': job_host_pairs_total,
            'aggregated': aggregated,
        }

    def base(self, data):
        """
        Aggregations grouped by:
        1. job_type (model):
           - Number of tasks executed (sum of all tasks executed per job_type)
           - Success ratio of tasks executed (ratio between ok and failed tasks (and others))
           - Unique hosts per job_type

        2. launch_type:
           - Same statistics as above
           - Job type count (distinct job types per launch type)

        3. controller_version:
           - Same statistics as above
           - Job type count (distinct job types per controller version)
           - Launch type counts

        Success rate and average - this can compute SaaS team from the metrics

        data is a dict with 'job_host_pairs_total' and 'aggregated' dataframe
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_controller_version': [],
                    'job_host_pairs_total': 0,
                },
                'rollup': {'aggregated': pd.DataFrame(), 'job_host_pairs_total': 0},
            }

        # Extract job_host_pairs_total and aggregated dataframe from the data structure
        job_host_pairs_total = data.get('job_host_pairs_total', 0)
        dataframe = data.get('aggregated', pd.DataFrame())

        # Return empty result if dataframe is empty
        if dataframe.empty:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_controller_version': [],
                    'job_host_pairs_total': job_host_pairs_total,
                },
                'rollup': {'aggregated': dataframe, 'job_host_pairs_total': job_host_pairs_total},
            }

        # Union unique_hosts sets for aggregation
        def union_hosts(series):
            """Union all sets in the series"""
            result = set()
            for hosts_set in series:
                if isinstance(hosts_set, set):
                    result.update(hosts_set)
                elif hosts_set is not None:
                    result.update(hosts_set)
            return result

        # Common aggregation dictionary
        common_aggregations = {
            'dark_total': ('dark_total', 'sum'),
            'failures_total': ('failures_total', 'sum'),
            'ok_total': ('ok_total', 'sum'),
            'skipped_total': ('skipped_total', 'sum'),
            'ignored_total': ('ignored_total', 'sum'),
            'rescued_total': ('rescued_total', 'sum'),
            'unique_hosts': ('unique_hosts', union_hosts),
            'hosts_successful_total': ('hosts_successful_total', 'sum'),
            'hosts_failed_total': ('hosts_failed_total', 'sum'),
            'hosts_unreachable_total': ('hosts_unreachable_total', 'sum'),
        }

        # Aggregations grouped by job_type
        aggregations_by_job_type = (
            dataframe.groupby('job_type')
            .agg(**common_aggregations)
            .reset_index()
            .assign(unique_hosts_total=lambda x: x['unique_hosts'].apply(len))
            .drop(columns=['unique_hosts'])
        )

        # Aggregations grouped by launch_type
        # Add job_type_total to count distinct job types per launch type
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict['job_type_total'] = ('job_type', 'nunique')

        aggregations_by_launch_type = (
            dataframe.groupby('launch_type')
            .agg(**aggregations_by_launch_type_dict)
            .reset_index()
            .assign(unique_hosts_total=lambda x: x['unique_hosts'].apply(len))
            .drop(columns=['unique_hosts'])
        )

        # Aggregations grouped by controller_version
        # Add job_type_total and launch_type counts
        aggregations_by_controller_version_dict = common_aggregations.copy()
        aggregations_by_controller_version_dict['job_type_total'] = ('job_type', 'nunique')
        aggregations_by_controller_version_dict['launch_type_total'] = ('launch_type', 'nunique')

        aggregations_by_controller_version = (
            dataframe.groupby('ansible_version')
            .agg(**aggregations_by_controller_version_dict)
            .reset_index()
            .rename(columns={'ansible_version': 'controller_version'})  # Rename to controller_version in output
            .assign(unique_hosts_total=lambda x: x['unique_hosts'].apply(len))
            .drop(columns=['unique_hosts'])
        )

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_job_type': aggregations_by_job_type.to_dict(orient='records'),
            'by_launch_type': aggregations_by_launch_type.to_dict(orient='records'),
            'by_controller_version': aggregations_by_controller_version.to_dict(orient='records'),
            'job_host_pairs_total': job_host_pairs_total,
        }

        return {
            'json': json_data,
        }
