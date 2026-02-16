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
        Merge JSON structures from batches by summing numeric columns and unioning lists.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        def merge_stats_json(stats_all, stats_new, groupby_col):
            """Merge two stats JSON lists by summing numeric columns and unioning lists."""
            if not stats_all:
                return stats_new if stats_new else []
            if not stats_new:
                return stats_all if stats_all else []

            # Create lookup dictionaries keyed by grouping column
            all_dict = {}
            for item in stats_all:
                key = item.get(groupby_col)
                all_dict[key] = item.copy()

            new_dict = {}
            for item in stats_new:
                key = item.get(groupby_col)
                new_dict[key] = item.copy()

            # Merge items
            merged_list = []
            all_keys = set(all_dict.keys()) | set(new_dict.keys())

            # Numeric columns to sum
            numeric_cols = [
                'dark_total',
                'failures_total',
                'ok_total',
                'skipped_total',
                'ignored_total',
                'rescued_total',
                'successful_hosts_total',
                'failed_hosts_total',
                'unreachable_hosts_total',
                'unique_hosts_total',
                'job_type_total',
                'launch_type_total',
            ]

            # List columns to union
            list_cols = ['unique_hosts', 'job_remote_ids', 'job_types', 'launch_types']

            for key in all_keys:
                item_all = all_dict.get(key, {})
                item_new = new_dict.get(key, {})

                # Start with item_all or item_new
                if item_all:
                    merged_item = item_all.copy()
                elif item_new:
                    merged_item = item_new.copy()
                else:
                    continue

                # If both exist, merge them
                if item_all and item_new:
                    # Sum numeric columns
                    for col in numeric_cols:
                        val_all = item_all.get(col) if item_all.get(col) is not None else 0
                        val_new = item_new.get(col) if item_new.get(col) is not None else 0
                        merged_item[col] = val_all + val_new

                    # Union list columns
                    for col in list_cols:
                        list_all = item_all.get(col) if item_all.get(col) is not None else []
                        list_new = item_new.get(col) if item_new.get(col) is not None else []
                        # Convert to sets, union, convert back to sorted list
                        set_all = set(list_all) if isinstance(list_all, list) else set()
                        set_new = set(list_new) if isinstance(list_new, list) else set()
                        merged_item[col] = sorted(list(set_all.union(set_new)))

                    # Recompute totals from lists
                    if 'unique_hosts' in merged_item:
                        merged_item['unique_hosts_total'] = len(merged_item['unique_hosts'])
                    if 'job_types' in merged_item:
                        merged_item['job_type_total'] = len(merged_item['job_types'])
                    if 'launch_types' in merged_item:
                        merged_item['launch_type_total'] = len(merged_item['launch_types'])

                merged_list.append(merged_item)

            return merged_list

        # Merge by_job_type, by_launch_type, by_controller_version
        by_job_type = merge_stats_json(data_all.get('by_job_type', []), data_new.get('by_job_type', []), 'job_type')

        by_launch_type = merge_stats_json(data_all.get('by_launch_type', []), data_new.get('by_launch_type', []), 'launch_type')

        by_controller_version = merge_stats_json(
            data_all.get('by_controller_version', []), data_new.get('by_controller_version', []), 'controller_version'
        )

        # Sum job_host_pairs_total
        job_host_pairs_total = data_all.get('job_host_pairs_total', 0) + data_new.get('job_host_pairs_total', 0)

        return {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_controller_version': by_controller_version,
            'job_host_pairs_total': job_host_pairs_total,
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

        # Handle empty dataframe
        if dataframe.empty:
            return {
                'by_job_type': [],
                'by_launch_type': [],
                'by_controller_version': [],
                'job_host_pairs_total': job_host_pairs_total,
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

        # First aggregate by job_remote_id to merge hosts for the same job
        # This ensures each job is properly aggregated even if hosts are split across batches
        if 'job_remote_id' in dataframe.columns:
            aggregated_by_job = (
                dataframe.groupby('job_remote_id')
                .agg(
                    dark_total=('dark', 'sum'),
                    failures_total=('failures', 'sum'),
                    ok_total=('ok', 'sum'),
                    skipped_total=('skipped', 'sum'),
                    ignored_total=('ignored', 'sum'),
                    rescued_total=('rescued', 'sum'),
                    unique_hosts=('host_name', lambda x: sorted(list(set(x.dropna())))),
                    successful_hosts_total=('host_outcome', lambda x: (x == 'successful').sum()),
                    failed_hosts_total=('host_outcome', lambda x: (x == 'failed').sum()),
                    unreachable_hosts_total=('host_outcome', lambda x: (x == 'unreachable').sum()),
                    # Preserve constant fields per job_remote_id
                    job_type=('model', 'first'),
                    launch_type=('launch_type', 'first'),
                    controller_version=('controller_version', 'first'),
                )
                .reset_index()
            )
        else:
            # If no job_remote_id, aggregate directly by job_type, launch_type, controller_version
            aggregated_by_job = (
                dataframe.groupby(['model', 'launch_type', 'controller_version'])
                .agg(
                    dark_total=('dark', 'sum'),
                    failures_total=('failures', 'sum'),
                    ok_total=('ok', 'sum'),
                    skipped_total=('skipped', 'sum'),
                    ignored_total=('ignored', 'sum'),
                    rescued_total=('rescued', 'sum'),
                    unique_hosts=('host_name', lambda x: sorted(list(set(x.dropna())))),
                    successful_hosts_total=('host_outcome', lambda x: (x == 'successful').sum()),
                    failed_hosts_total=('host_outcome', lambda x: (x == 'failed').sum()),
                    unreachable_hosts_total=('host_outcome', lambda x: (x == 'unreachable').sum()),
                )
                .reset_index()
                .rename(columns={'model': 'job_type'})
            )

        # Common aggregation dictionary
        def union_host_lists(series):
            """Union all host lists in the series"""
            result = set()
            for host_list in series:
                if isinstance(host_list, list):
                    result.update(host_list)
                elif isinstance(host_list, set):
                    result.update(host_list)
            return sorted(list(result))

        common_aggregations = {
            'dark_total': ('dark_total', 'sum'),
            'failures_total': ('failures_total', 'sum'),
            'ok_total': ('ok_total', 'sum'),
            'skipped_total': ('skipped_total', 'sum'),
            'ignored_total': ('ignored_total', 'sum'),
            'rescued_total': ('rescued_total', 'sum'),
            'unique_hosts': ('unique_hosts', union_host_lists),
            'successful_hosts_total': ('successful_hosts_total', 'sum'),
            'failed_hosts_total': ('failed_hosts_total', 'sum'),
            'unreachable_hosts_total': ('unreachable_hosts_total', 'sum'),
            'job_remote_ids': ('job_remote_id', lambda x: sorted(list(set(x.dropna())))),
        }

        # Aggregations grouped by job_type
        aggregations_by_job_type = aggregated_by_job.groupby('job_type').agg(**common_aggregations).reset_index()
        aggregations_by_job_type['unique_hosts_total'] = aggregations_by_job_type['unique_hosts'].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        # Aggregations grouped by launch_type
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict['job_types'] = ('job_type', lambda x: sorted(list(set(x.dropna()))))

        aggregations_by_launch_type = aggregated_by_job.groupby('launch_type').agg(**aggregations_by_launch_type_dict).reset_index()
        aggregations_by_launch_type['unique_hosts_total'] = aggregations_by_launch_type['unique_hosts'].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        aggregations_by_launch_type['job_type_total'] = aggregations_by_launch_type['job_types'].apply(lambda x: len(x) if isinstance(x, list) else 0)

        # Aggregations grouped by controller_version
        aggregations_by_controller_version_dict = common_aggregations.copy()
        aggregations_by_controller_version_dict['job_types'] = ('job_type', lambda x: sorted(list(set(x.dropna()))))
        aggregations_by_controller_version_dict['launch_types'] = ('launch_type', lambda x: sorted(list(set(x.dropna()))))

        aggregations_by_controller_version = (
            aggregated_by_job.groupby('controller_version').agg(**aggregations_by_controller_version_dict).reset_index()
        )
        aggregations_by_controller_version['unique_hosts_total'] = aggregations_by_controller_version['unique_hosts'].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        aggregations_by_controller_version['job_type_total'] = aggregations_by_controller_version['job_types'].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        aggregations_by_controller_version['launch_type_total'] = aggregations_by_controller_version['launch_types'].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        # Convert DataFrames to JSON (list of dicts)
        by_job_type = aggregations_by_job_type.to_dict(orient='records')
        by_launch_type = aggregations_by_launch_type.to_dict(orient='records')
        by_controller_version = aggregations_by_controller_version.to_dict(orient='records')

        return {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_controller_version': by_controller_version,
            'job_host_pairs_total': job_host_pairs_total,
        }

    def base(self, data):
        """
        Returns the already-aggregated JSON data from prepare() and merge().

        data is a dict with already-aggregated JSON structures from prepare() and merge()
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
            }

        # Extract data from the structure (already JSON)
        by_job_type = data.get('by_job_type', [])
        by_launch_type = data.get('by_launch_type', [])
        by_controller_version = data.get('by_controller_version', [])
        job_host_pairs_total = data.get('job_host_pairs_total', 0)

        # Handle empty data
        if not by_job_type and not by_launch_type and not by_controller_version:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_controller_version': [],
                    'job_host_pairs_total': job_host_pairs_total,
                },
            }

        # Drop list columns from stats (we only need the computed totals, not the raw lists)
        for stats_list in [by_job_type, by_launch_type, by_controller_version]:
            for item in stats_list:
                # Drop list columns that were used for deduplication
                for col in ['unique_hosts', 'job_remote_ids', 'job_types', 'launch_types']:
                    if col in item:
                        del item[col]

        # Prepare JSON data (already in JSON format)
        json_data = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_controller_version': by_controller_version,
            'job_host_pairs_total': job_host_pairs_total,
        }

        return {
            'json': json_data,
        }
