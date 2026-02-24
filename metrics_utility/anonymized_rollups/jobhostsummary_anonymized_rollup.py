import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class JobHostSummaryAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - job_host_summary_service collector data
    """

    def __init__(self):
        super().__init__('job_host_summary')
        self.collector_names = ['job_host_summary_service']

    def _convert_id_columns_to_strings(self, dataframe):
        """Convert ID columns to strings, including host_remote_id."""
        # Call parent method first
        dataframe = super()._convert_id_columns_to_strings(dataframe)

        # Also convert host_remote_id if it exists
        if not dataframe.empty and 'host_remote_id' in dataframe.columns:
            dataframe['host_remote_id'] = dataframe['host_remote_id'].apply(
                lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) else x
            )

        return dataframe

    def _merge_stats_json(self, stats_all, stats_new, groupby_col):
        """Merge two stats JSON lists by summing numeric columns and unioning lists."""
        if not stats_all:
            return stats_new if stats_new else []
        if not stats_new:
            return stats_all if stats_all else []

        # Create lookup dictionaries keyed by grouping column
        all_dict = {item.get(groupby_col): item.copy() for item in stats_all}
        new_dict = {item.get(groupby_col): item.copy() for item in stats_new}

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
            'job_type_total',
            'launch_type_total',
        ]

        # List columns to union (no longer includes unique_hosts - that's now top-level)
        list_cols = ['job_types', 'launch_types']

        for key in all_keys:
            item_all = all_dict.get(key, {})
            item_new = new_dict.get(key, {})
            merged_item = self._create_merged_item(item_all, item_new, numeric_cols, list_cols)
            if merged_item:
                merged_list.append(merged_item)

        return merged_list

    def _create_merged_item(self, item_all, item_new, numeric_cols, list_cols):
        """Create a merged item from two items, handling None/empty cases."""
        if not item_all and not item_new:
            return None

        merged_item = item_all.copy() if item_all else item_new.copy()

        if item_all and item_new:
            self._merge_numeric_columns(merged_item, item_all, item_new, numeric_cols)
            self._merge_list_columns(merged_item, item_all, item_new, list_cols)
            self._recompute_totals(merged_item)
        else:
            # Even with a single item, ensure totals are computed from set sizes
            self._recompute_totals(merged_item)

        return merged_item

    def _merge_numeric_columns(self, merged_item, item_all, item_new, numeric_cols):
        """Sum numeric columns from both items."""
        for col in numeric_cols:
            val_all = item_all.get(col) if item_all.get(col) is not None else 0
            val_new = item_new.get(col) if item_new.get(col) is not None else 0
            merged_item[col] = val_all + val_new

    def _merge_list_columns(self, merged_item, item_all, item_new, list_cols):
        """Union list columns from both items."""
        for col in list_cols:
            list_all = item_all.get(col) if item_all.get(col) is not None else []
            list_new = item_new.get(col) if item_new.get(col) is not None else []
            set_all = set(list_all) if isinstance(list_all, list) else set()
            set_new = set(list_new) if isinstance(list_new, list) else set()
            merged_item[col] = sorted(list(set_all.union(set_new)))

    def _recompute_totals(self, merged_item):
        """Recompute totals from list columns."""
        if 'job_types' in merged_item:
            merged_item['job_type_total'] = len(merged_item['job_types'])
        if 'launch_types' in merged_item:
            merged_item['launch_type_total'] = len(merged_item['launch_types'])

    def merge(self, data_all, data_new):
        """
        Merge JSON structures from batches by summing numeric columns and unioning lists.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Merge by_job_type, by_launch_type, by_ansible_version
        by_job_type = self._merge_stats_json(data_all.get('by_job_type', []), data_new.get('by_job_type', []), 'job_type')
        by_launch_type = self._merge_stats_json(data_all.get('by_launch_type', []), data_new.get('by_launch_type', []), 'launch_type')
        by_ansible_version = self._merge_stats_json(data_all.get('by_ansible_version', []), data_new.get('by_ansible_version', []), 'ansible_version')

        # Sum job_host_pairs_total
        job_host_pairs_total = data_all.get('job_host_pairs_total', 0) + data_new.get('job_host_pairs_total', 0)

        # Merge top-level host_ids lists (union and maintain uniqueness)
        host_ids_all = set(data_all.get('host_ids', []))
        host_ids_new = set(data_new.get('host_ids', []))
        host_ids = sorted(list(host_ids_all.union(host_ids_new)))

        return {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'job_host_pairs_total': job_host_pairs_total,
            'host_ids': host_ids,
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
    # ansible_version
    # launch_type

    def _normalize_dataframe(self, dataframe):
        """Normalize dataframe columns, handling missing columns and data types."""
        # Check if job_remote_id column exists
        if 'job_remote_id' not in dataframe.columns:
            dataframe['job_remote_id'] = None

        # Check if host_remote_id column exists (host ID, not host name)
        if 'host_remote_id' not in dataframe.columns:
            dataframe['host_remote_id'] = None

        # Check if model column exists (for backward compatibility)
        if 'model' not in dataframe.columns:
            dataframe['model'] = 'unknown'

        # Normalize ansible_version: treat empty strings as NaN for consistent grouping
        if 'ansible_version' in dataframe.columns:
            dataframe['ansible_version'] = dataframe['ansible_version'].replace('', pd.NA)
        else:
            dataframe['ansible_version'] = pd.NA

        # Handle launch_type: if missing, set to 'unknown'
        if 'launch_type' not in dataframe.columns:
            dataframe['launch_type'] = 'unknown'

        # Keep ansible_version column name (no rename needed)

        # Compute host_outcome
        dataframe['host_outcome'] = 'successful'
        dataframe.loc[dataframe['failures'] > 0, 'host_outcome'] = 'failed'
        dataframe.loc[dataframe['dark'] > 0, 'host_outcome'] = 'unreachable'

    def _aggregate_by_job(self, dataframe):
        """Aggregate dataframe by job_remote_id or by job_type/launch_type/ansible_version."""
        if 'job_remote_id' in dataframe.columns:
            return self._aggregate_by_job_remote_id(dataframe)
        return self._aggregate_by_job_type_directly(dataframe)

    def _aggregate_by_job_remote_id(self, dataframe):
        """Aggregate by job_remote_id to merge hosts for the same job."""
        return (
            dataframe.groupby('job_remote_id')
            .agg(
                dark_total=('dark', 'sum'),
                failures_total=('failures', 'sum'),
                ok_total=('ok', 'sum'),
                skipped_total=('skipped', 'sum'),
                ignored_total=('ignored', 'sum'),
                rescued_total=('rescued', 'sum'),
                successful_hosts_total=('host_outcome', lambda x: (x == 'successful').sum()),
                failed_hosts_total=('host_outcome', lambda x: (x == 'failed').sum()),
                unreachable_hosts_total=('host_outcome', lambda x: (x == 'unreachable').sum()),
                job_type=('model', 'first'),
                launch_type=('launch_type', 'first'),
                ansible_version=('ansible_version', 'first'),
            )
            .reset_index()
        )

    def _aggregate_by_job_type_directly(self, dataframe):
        """Aggregate directly by job_type, launch_type, ansible_version when no job_remote_id."""
        return (
            dataframe.groupby(['model', 'launch_type', 'ansible_version'])
            .agg(
                dark_total=('dark', 'sum'),
                failures_total=('failures', 'sum'),
                ok_total=('ok', 'sum'),
                skipped_total=('skipped', 'sum'),
                ignored_total=('ignored', 'sum'),
                rescued_total=('rescued', 'sum'),
                successful_hosts_total=('host_outcome', lambda x: (x == 'successful').sum()),
                failed_hosts_total=('host_outcome', lambda x: (x == 'failed').sum()),
                unreachable_hosts_total=('host_outcome', lambda x: (x == 'unreachable').sum()),
            )
            .reset_index()
            .rename(columns={'model': 'job_type'})
        )

    def _get_common_aggregations(self):
        """Get common aggregation dictionary for grouping operations."""
        return {
            'dark_total': ('dark_total', 'sum'),
            'failures_total': ('failures_total', 'sum'),
            'ok_total': ('ok_total', 'sum'),
            'skipped_total': ('skipped_total', 'sum'),
            'ignored_total': ('ignored_total', 'sum'),
            'rescued_total': ('rescued_total', 'sum'),
            'successful_hosts_total': ('successful_hosts_total', 'sum'),
            'failed_hosts_total': ('failed_hosts_total', 'sum'),
            'unreachable_hosts_total': ('unreachable_hosts_total', 'sum'),
        }

    def _compute_list_length(self, x):
        """Compute length of list, returning 0 if not a list."""
        return len(x) if isinstance(x, list) else 0

    def _aggregate_by_job_type(self, aggregated_by_job, common_aggregations):
        """Aggregate by job_type."""
        aggregations_by_job_type = aggregated_by_job.groupby('job_type').agg(**common_aggregations).reset_index()
        return aggregations_by_job_type

    def _aggregate_by_launch_type(self, aggregated_by_job, common_aggregations):
        """Aggregate by launch_type."""
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict['job_types'] = ('job_type', lambda x: sorted(list(set(x.dropna()))))

        aggregations_by_launch_type = aggregated_by_job.groupby('launch_type').agg(**aggregations_by_launch_type_dict).reset_index()
        aggregations_by_launch_type['job_type_total'] = aggregations_by_launch_type['job_types'].apply(self._compute_list_length)
        return aggregations_by_launch_type

    def _aggregate_by_ansible_version(self, aggregated_by_job, common_aggregations):
        """Aggregate by ansible_version."""
        aggregations_by_ansible_version_dict = common_aggregations.copy()
        aggregations_by_ansible_version_dict['job_types'] = ('job_type', lambda x: sorted(list(set(x.dropna()))))
        aggregations_by_ansible_version_dict['launch_types'] = ('launch_type', lambda x: sorted(list(set(x.dropna()))))

        aggregations_by_ansible_version = aggregated_by_job.groupby('ansible_version').agg(**aggregations_by_ansible_version_dict).reset_index()
        aggregations_by_ansible_version['job_type_total'] = aggregations_by_ansible_version['job_types'].apply(self._compute_list_length)
        aggregations_by_ansible_version['launch_type_total'] = aggregations_by_ansible_version['launch_types'].apply(self._compute_list_length)
        return aggregations_by_ansible_version

    def prepare(self, dataframe):
        # Convert ID columns to strings at the beginning
        dataframe = self._convert_id_columns_to_strings(dataframe)
        
        # Count all records before processing
        job_host_pairs_total = len(dataframe)

        # Handle empty dataframe
        if dataframe.empty:
            return sanitize_json(
                {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_ansible_version': [],
                    'job_host_pairs_total': job_host_pairs_total,
                    'host_ids': [],
                }
            )

        # Normalize dataframe columns
        self._normalize_dataframe(dataframe)

        # Collect all unique host_ids from the dataframe (top-level, not per-grouping)
        if 'host_remote_id' in dataframe.columns:
            host_ids = sorted(list(set(dataframe['host_remote_id'].dropna())))
        else:
            host_ids = []

        # Aggregate by job
        aggregated_by_job = self._aggregate_by_job(dataframe)

        # Get common aggregations
        common_aggregations = self._get_common_aggregations()

        # Perform aggregations by different dimensions
        aggregations_by_job_type = self._aggregate_by_job_type(aggregated_by_job, common_aggregations)
        aggregations_by_launch_type = self._aggregate_by_launch_type(aggregated_by_job, common_aggregations)
        aggregations_by_ansible_version = self._aggregate_by_ansible_version(aggregated_by_job, common_aggregations)

        # Convert DataFrames to JSON (list of dicts)
        by_job_type = aggregations_by_job_type.to_dict(orient='records')
        by_launch_type = aggregations_by_launch_type.to_dict(orient='records')
        by_ansible_version = aggregations_by_ansible_version.to_dict(orient='records')

        result = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'job_host_pairs_total': job_host_pairs_total,
            'host_ids': host_ids,
        }

        # Sanitize to convert NumPy types to native Python types for JSON serialization
        return sanitize_json(result)

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
                    'by_ansible_version': [],
                    'job_host_pairs_total': 0,
                    'unique_hosts_total': 0,
                    'host_ids': [],
                },
            }

        # Extract data from the structure (already JSON)
        by_job_type = data.get('by_job_type', [])
        by_launch_type = data.get('by_launch_type', [])
        by_ansible_version = data.get('by_ansible_version', [])
        job_host_pairs_total = data.get('job_host_pairs_total', 0)
        host_ids = data.get('host_ids', [])

        # Handle empty data
        if not by_job_type and not by_launch_type and not by_ansible_version:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_ansible_version': [],
                    'job_host_pairs_total': job_host_pairs_total,
                    'unique_hosts_total': 0,
                    'host_ids': host_ids if isinstance(host_ids, list) else [],
                },
            }

        # Compute unique_hosts_total from top-level host_ids list
        if isinstance(host_ids, list):
            unique_hosts_total = len(set(host_ids))
        else:
            unique_hosts_total = 0

        # Drop list columns from stats (we only need the computed totals, not the raw lists)
        for stats_list in [by_job_type, by_launch_type, by_ansible_version]:
            for item in stats_list:
                # Drop list columns that were used for deduplication
                for col in ['job_types', 'launch_types']:
                    if col in item:
                        del item[col]

        # Prepare JSON data (already in JSON format)
        json_data = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'job_host_pairs_total': job_host_pairs_total,
            'unique_hosts_total': unique_hosts_total,
            'host_ids': host_ids,
        }

        return {
            'json': json_data,
        }
