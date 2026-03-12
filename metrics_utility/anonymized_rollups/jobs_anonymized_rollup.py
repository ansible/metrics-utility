import hashlib
import json

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class JobsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - unified_jobs collector data
    """

    def _convert_id_columns_to_strings(self, dataframe):
        """Convert ID columns to strings at the beginning of prepare().

        Converts numeric ID columns (id, job_id, host_id, job_remote_id, unified_job_template_id, inventory_id) to strings
        to ensure consistent JSON serialization.
        """
        if dataframe.empty:
            return dataframe

        id_columns = ['id', 'job_id', 'host_id', 'job_remote_id', 'unified_job_template_id', 'inventory_id']
        for col in id_columns:
            if col in dataframe.columns:
                # Convert numeric IDs to strings, preserving NaN values
                dataframe[col] = dataframe[col].apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) else x)

        return dataframe

    def _preprocess_dataframe(self, dataframe):
        """Preprocess dataframe: filter, normalize columns, and compute derived fields."""
        # Filter out jobs that are not finished
        dataframe = dataframe[dataframe['finished'].notna()]

        # Coerce datetime-like columns to pandas datetimes (timezone-aware if possible)
        for col in ['started', 'finished', 'created']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        # Convert failed column to boolean (handle PostgreSQL 't'/'f' representation)
        if 'failed' in dataframe.columns:
            dataframe['failed'] = dataframe['failed'].replace({'t': True, 'f': False}).fillna(False).astype(bool)

        # Normalize ansible_version: treat empty strings as NaN for consistent grouping
        if 'ansible_version' in dataframe.columns:
            dataframe['ansible_version'] = dataframe['ansible_version'].replace('', pd.NA)

        # Compute job duration in seconds
        dataframe['job_duration_seconds'] = (dataframe['finished'] - dataframe['started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['started'] - dataframe['created']).dt.total_seconds()

        # Pre-compute boolean columns for efficient aggregations
        dataframe['jobs_successful'] = ~dataframe['failed']
        dataframe['jobs_never_started'] = dataframe['started'].isna()
        dataframe['job_duration_successful_seconds'] = dataframe['job_duration_seconds'].where(dataframe['jobs_successful'], 0)
        dataframe['job_duration_failed_seconds'] = dataframe['job_duration_seconds'].where(dataframe['failed'], 0)

        return dataframe

    def _get_common_aggregations(self):
        """Get common aggregation dictionary shared by all groupings."""
        return {
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
            'templates': ('unified_job_template_id', lambda x: sorted(set(x.dropna()))),
            'inventories': ('inventory_id', lambda x: sorted(set(x.dropna()))),
        }

    def _get_ansible_versions_aggregation(self):
        """Get ansible versions aggregation helper."""

        def get_ansible_versions(grouped_series):
            """Helper function to extract sorted unique ansible versions from a group"""
            unique_versions = grouped_series.dropna().unique()
            return sorted([str(v) for v in unique_versions if pd.notna(v)])

        return {'ansible_versions': ('ansible_version', get_ansible_versions)}

    def _compute_list_length(self, x):
        """Compute length of list, returning 0 if not a list."""
        return len(x) if isinstance(x, list) else 0

    def _add_totals_to_aggregation(self, aggregation_df, list_columns):
        """Add total columns computed from list columns."""
        for col, total_col in list_columns:
            if col in aggregation_df.columns:
                aggregation_df[total_col] = aggregation_df[col].apply(self._compute_list_length)

    def _aggregate_by_job_type(self, dataframe, common_aggregations, ansible_versions_aggregation):
        """Aggregate by job_type (model)."""
        aggregations_by_job_type_dict = common_aggregations.copy()
        aggregations_by_job_type_dict.update(ansible_versions_aggregation)

        aggregations_by_job_type = dataframe.groupby('model').agg(**aggregations_by_job_type_dict).reset_index().rename(columns={'model': 'job_type'})

        aggregations_by_job_type['is_automation'] = aggregations_by_job_type['job_type'] == 'job'
        self._add_totals_to_aggregation(aggregations_by_job_type, [('templates', 'templates_total'), ('inventories', 'inventories_total')])

        return aggregations_by_job_type

    def _aggregate_by_launch_type(self, dataframe, common_aggregations, ansible_versions_aggregation):
        """Aggregate by launch_type."""
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict.update({'job_types': ('model', lambda x: sorted(set(x.dropna())))})
        aggregations_by_launch_type_dict.update(ansible_versions_aggregation)

        aggregations_by_launch_type = dataframe.groupby('launch_type').agg(**aggregations_by_launch_type_dict).reset_index()
        self._add_totals_to_aggregation(
            aggregations_by_launch_type,
            [
                ('templates', 'templates_total'),
                ('inventories', 'inventories_total'),
            ],
        )

        return aggregations_by_launch_type

    def _aggregate_by_ansible_version(self, dataframe, common_aggregations):
        """Aggregate by ansible_version."""
        aggregations_by_ansible_version_dict = common_aggregations.copy()
        aggregations_by_ansible_version_dict.update({'job_types': ('model', lambda x: sorted(set(x.dropna())))})

        aggregations_by_ansible_version = dataframe.groupby('ansible_version').agg(**aggregations_by_ansible_version_dict).reset_index()

        self._add_totals_to_aggregation(
            aggregations_by_ansible_version,
            [
                ('templates', 'templates_total'),
                ('inventories', 'inventories_total'),
            ],
        )

        return aggregations_by_ansible_version

    def _aggregate_all_jobs(self, dataframe, common_aggregations, ansible_versions_aggregation):
        """Aggregate all jobs into a single summary row (for jobs_by_controller_version)."""
        aggregations_dict = common_aggregations.copy()
        aggregations_dict.update(ansible_versions_aggregation)
        aggregations_dict.update({'job_types': ('model', lambda x: sorted(set(x.dropna())))})

        # Group by an external constant Series so the dataframe itself is not modified
        aggregations_all_jobs = dataframe.groupby(pd.Series(0, index=dataframe.index)).agg(**aggregations_dict).reset_index(drop=True)

        self._add_totals_to_aggregation(
            aggregations_all_jobs,
            [('templates', 'templates_total'), ('inventories', 'inventories_total')],
        )

        return aggregations_all_jobs

    def _extract_metadata(self, dataframe):
        """Extract metadata fields from dataframe."""
        organizations = sorted(set(dataframe['organization_name'].dropna().unique()))
        forks_total = int(dataframe['forks'].sum()) if 'forks' in dataframe.columns else 0

        scm_types = []
        if 'scm_type' in dataframe.columns:
            scm_types = sorted([str(v) for v in dataframe['scm_type'].dropna().unique() if pd.notna(v) and str(v).strip()])

        return organizations, forks_total, scm_types

    def prepare(self, dataframe):
        # Convert ID columns to strings at the beginning
        dataframe = self._convert_id_columns_to_strings(dataframe)

        # Filter out jobs that are not finished
        dataframe = dataframe[dataframe['finished'].notna()]

        # Handle empty dataframe
        if dataframe.empty:
            return sanitize_json(
                {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_ansible_version': [],
                    'by_controller_version': [],
                    'organizations': [],
                    'forks_total': 0,
                    'scm_types': [],
                    'installed_collections': [],
                }
            )

        # Preprocess dataframe
        dataframe = self._preprocess_dataframe(dataframe)

        # Get aggregation dictionaries
        common_aggregations = self._get_common_aggregations()
        ansible_versions_aggregation = self._get_ansible_versions_aggregation()

        # Perform aggregations by different dimensions
        aggregations_by_job_type = self._aggregate_by_job_type(dataframe, common_aggregations, ansible_versions_aggregation)
        aggregations_by_launch_type = self._aggregate_by_launch_type(dataframe, common_aggregations, ansible_versions_aggregation)
        aggregations_by_ansible_version = self._aggregate_by_ansible_version(dataframe, common_aggregations)
        aggregations_all_jobs = self._aggregate_all_jobs(dataframe, common_aggregations, ansible_versions_aggregation)

        # Convert DataFrames to JSON (list of dicts)
        by_job_type = aggregations_by_job_type.to_dict(orient='records')
        by_launch_type = aggregations_by_launch_type.to_dict(orient='records')
        by_ansible_version = aggregations_by_ansible_version.to_dict(orient='records')
        by_controller_version = aggregations_all_jobs.to_dict(orient='records')

        # Extract metadata
        organizations, forks_total, scm_types = self._extract_metadata(dataframe)

        # Process collections statistics
        collections_stats = self._process_collections_from_jobs(dataframe)

        result = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'by_controller_version': by_controller_version,
            'organizations': organizations,
            'forks_total': forks_total,
            'scm_types': scm_types,
            'installed_collections': collections_stats,
        }

        # Sanitize to convert NumPy types to native Python types for JSON serialization
        return sanitize_json(result)

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
            'jobs_total',
            'jobs_failed_total',
            'jobs_successful_total',
            'jobs_never_started_total',
            'jobs_duration_total_seconds',
            'jobs_successful_duration_total_seconds',
            'jobs_failed_duration_total_seconds',
            'job_waiting_time_total_seconds',
            'templates_total',
            'inventories_total',
        ]

        # List columns to union
        list_cols = ['templates', 'inventories', 'ansible_versions', 'job_types']

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
            self._merge_max_columns(merged_item, item_all, item_new)
            self._merge_min_columns(merged_item, item_all, item_new)
            self._merge_list_columns(merged_item, item_all, item_new, list_cols)
            self._recompute_totals(merged_item)

        return merged_item

    def _merge_numeric_columns(self, merged_item, item_all, item_new, numeric_cols):
        """Sum numeric columns from both items."""
        for col in numeric_cols:
            val_all = item_all.get(col) if item_all.get(col) is not None else 0
            val_new = item_new.get(col) if item_new.get(col) is not None else 0
            merged_item[col] = val_all + val_new

    def _merge_max_value(self, val_all, val_new):
        """Return the maximum of two nullable values."""
        if val_all is not None and val_new is not None:
            return max(val_all, val_new)
        return val_all if val_all is not None else val_new

    def _merge_min_value(self, val_all, val_new):
        """Return the minimum of two nullable values."""
        if val_all is not None and val_new is not None:
            return min(val_all, val_new)
        return val_all if val_all is not None else val_new

    def _merge_max_columns(self, merged_item, item_all, item_new):
        """Take maximum value for max columns."""
        for col in ['job_duration_maximum_seconds', 'job_waiting_time_maximum_seconds']:
            if col in merged_item:
                merged_item[col] = self._merge_max_value(item_all.get(col), item_new.get(col))

    def _merge_min_columns(self, merged_item, item_all, item_new):
        """Take minimum value for min columns."""
        for col in ['job_duration_minimum_seconds', 'job_waiting_time_minimum_seconds']:
            if col in merged_item:
                merged_item[col] = self._merge_min_value(item_all.get(col), item_new.get(col))

    def _merge_list_columns(self, merged_item, item_all, item_new, list_cols):
        """Union list columns from both items."""
        for col in list_cols:
            list_all = item_all.get(col) if item_all.get(col) is not None else []
            list_new = item_new.get(col) if item_new.get(col) is not None else []
            set_all = set(list_all) if isinstance(list_all, list) else set()
            set_new = set(list_new) if isinstance(list_new, list) else set()
            merged_item[col] = sorted(set_all.union(set_new))

    def _recompute_totals(self, merged_item):
        """Recompute totals from list columns."""
        if 'templates' in merged_item:
            merged_item['templates_total'] = len(merged_item['templates'])
        if 'inventories' in merged_item:
            merged_item['inventories_total'] = len(merged_item['inventories'])

    def _merge_single_item_stats(self, stats_all, stats_new):
        """Merge two single-item stats lists (used for jobs_by_controller_version)."""
        if not stats_all:
            return stats_new if stats_new else []
        if not stats_new:
            return stats_all

        numeric_cols = [
            'jobs_total',
            'jobs_failed_total',
            'jobs_successful_total',
            'jobs_never_started_total',
            'jobs_duration_total_seconds',
            'jobs_successful_duration_total_seconds',
            'jobs_failed_duration_total_seconds',
            'job_waiting_time_total_seconds',
            'templates_total',
            'inventories_total',
        ]
        list_cols = ['templates', 'inventories', 'ansible_versions', 'job_types']

        item_all = stats_all[0]
        item_new = stats_new[0]
        merged_item = self._create_merged_item(item_all, item_new, numeric_cols, list_cols)
        return [merged_item] if merged_item else []

    def _merge_list_fields(self, data_all, data_new, field_name):
        """Merge list fields by union and sort."""
        all_set = set(data_all.get(field_name, []))
        new_set = set(data_new.get(field_name, []))
        return sorted(all_set.union(new_set))

    def _merge_single_collection(self, item_all, item_new):
        """Merge two collection stat items by summing numerics, taking max/min, and unioning lists."""
        numeric_cols = [
            'job_count',
            'jobs_failed_total',
            'jobs_successful_total',
            'jobs_never_started_total',
            'jobs_duration_total_seconds',
            'jobs_successful_duration_total_seconds',
            'jobs_failed_duration_total_seconds',
            'job_waiting_time_total_seconds',
        ]
        merged = {col: item_all.get(col, 0) + item_new.get(col, 0) for col in numeric_cols}

        for col in ['job_duration_maximum_seconds', 'job_waiting_time_maximum_seconds']:
            merged[col] = self._merge_max_value(item_all.get(col), item_new.get(col))

        for col in ['job_duration_minimum_seconds', 'job_waiting_time_minimum_seconds']:
            merged[col] = self._merge_min_value(item_all.get(col), item_new.get(col))

        for col, total_col in [('templates', 'templates_total'), ('inventories', 'inventories_total')]:
            merged_list = sorted(set(item_all.get(col) or []) | set(item_new.get(col) or []))
            merged[col] = merged_list
            merged[total_col] = len(merged_list)

        merged['ansible_versions'] = sorted(set(item_all.get('ansible_versions') or []) | set(item_new.get('ansible_versions') or []))

        return merged

    def _merge_collections(self, data_all, data_new):
        """Merge installed_collections by summing numeric fields, taking max/min for extremes,
        and unioning list fields for the same collection+version."""
        collections_all = {(item['collection_name'], item['collection_version']): item for item in data_all.get('installed_collections', [])}
        collections_new = {(item['collection_name'], item['collection_version']): item for item in data_new.get('installed_collections', [])}

        all_keys = set(collections_all.keys()) | set(collections_new.keys())
        merged_collections = {key: self._merge_single_collection(collections_all.get(key, {}), collections_new.get(key, {})) for key in all_keys}

        installed_collections = [
            {'collection_name': collection_name, 'collection_version': collection_version, **stats}
            for (collection_name, collection_version), stats in merged_collections.items()
        ]
        installed_collections.sort(key=lambda x: (x['collection_name'], x['collection_version']))
        return installed_collections

    def merge(self, data_all, data_new):
        """
        Merge JSON structures from batches by summing numeric columns and unioning lists.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Merge by_job_type, by_launch_type, by_ansible_version, by_controller_version
        by_job_type = self._merge_stats_json(data_all.get('by_job_type', []), data_new.get('by_job_type', []), 'job_type')
        by_launch_type = self._merge_stats_json(data_all.get('by_launch_type', []), data_new.get('by_launch_type', []), 'launch_type')
        by_ansible_version = self._merge_stats_json(data_all.get('by_ansible_version', []), data_new.get('by_ansible_version', []), 'ansible_version')
        by_controller_version = self._merge_single_item_stats(data_all.get('by_controller_version', []), data_new.get('by_controller_version', []))

        # Merge list fields
        organizations = self._merge_list_fields(data_all, data_new, 'organizations')
        scm_types = self._merge_list_fields(data_all, data_new, 'scm_types')

        # Sum forks_total
        forks_total = data_all.get('forks_total', 0) + data_new.get('forks_total', 0)

        # Merge collections
        installed_collections = self._merge_collections(data_all, data_new)

        return {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'by_controller_version': by_controller_version,
            'organizations': organizations,
            'forks_total': forks_total,
            'scm_types': scm_types,
            'installed_collections': installed_collections,
        }

    def __init__(self):
        super().__init__('jobs')
        self.collector_names = ['unified_jobs']

    def base(self, data):
        """
        Returns the already-aggregated JSON data from prepare() and merge().
        Computes final totals from lists/sets for proper deduplication.

        data is a dict with already-aggregated JSON structures from prepare() and merge()
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_ansible_version': [],
                    'jobs_by_controller_version': [],
                    'organizations_total': None,
                    'forks_total': None,
                    'jobs_total': None,
                    'installed_collections': [],
                    'scm_types': [],
                },
            }

        # Extract data from the structure (already JSON)
        by_job_type = data.get('by_job_type', [])
        by_launch_type = data.get('by_launch_type', [])
        by_ansible_version = data.get('by_ansible_version', [])
        by_controller_version = data.get('by_controller_version', [])
        organizations = data.get('organizations', [])
        forks_total = data.get('forks_total', 0)
        scm_types = data.get('scm_types', [])
        installed_collections = data.get('installed_collections', [])

        # Handle empty data
        if not by_job_type and not by_launch_type and not by_ansible_version:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_ansible_version': [],
                    'jobs_by_controller_version': [],
                    'organizations_total': 0,
                    'forks_total': 0,
                    'jobs_total': 0,
                    'installed_collections': [],
                    'scm_types': [],
                },
            }

        # Drop list columns from stats (we only need the computed totals, not the raw lists)
        for stats_list in [by_job_type, by_launch_type, by_ansible_version, by_controller_version]:
            for item in stats_list:
                # Drop list columns that were used for deduplication
                for col in ['templates', 'inventories', 'job_types']:
                    if col in item:
                        del item[col]

        # Compute final totals
        organizations_total = len(organizations)
        # Compute jobs_total by summing jobs_total from all job_type groups
        jobs_total = sum(item.get('jobs_total', 0) for item in by_job_type)

        # Prepare JSON data (already in JSON format)
        json_data = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'jobs_by_controller_version': by_controller_version,
            'organizations_total': organizations_total,
            'forks_total': forks_total,
            'jobs_total': jobs_total,
            'installed_collections': installed_collections,
            'scm_types': scm_types,
        }

        return {
            'json': json_data,
        }

    def _parse_collections_data(self, installed_collections_data):
        """
        Parse collections data from row, handling JSON strings and dicts.
        Returns dict or None if parsing fails.
        """
        if pd.isna(installed_collections_data) or not installed_collections_data:
            return None

        try:
            if isinstance(installed_collections_data, str):
                return json.loads(installed_collections_data)
            if isinstance(installed_collections_data, dict):
                return installed_collections_data
        except (json.JSONDecodeError, TypeError):
            pass

        return None

    def _init_collection_stats_entry(self):
        """Return a blank stats entry for a new collection+version key."""
        return {
            'job_count': 0,
            'jobs_failed_total': 0,
            'jobs_successful_total': 0,
            'jobs_never_started_total': 0,
            'jobs_duration_total_seconds': 0,
            'jobs_successful_duration_total_seconds': 0,
            'jobs_failed_duration_total_seconds': 0,
            'job_duration_maximum_seconds': None,
            'job_duration_minimum_seconds': None,
            'job_waiting_time_total_seconds': 0,
            'job_waiting_time_maximum_seconds': None,
            'job_waiting_time_minimum_seconds': None,
            'templates': set(),
            'inventories': set(),
            'ansible_versions': set(),
        }

    def _is_valid_id(self, value):
        """Return True when value is a non-None, non-NaN identifier."""
        return value is not None and not (isinstance(value, float) and pd.isna(value))

    def _update_duration_stats(self, stats, duration, failed):
        """Update duration-related fields in a collection stats entry."""
        if duration is None or pd.isna(duration):
            return
        stats['jobs_duration_total_seconds'] += duration
        if failed:
            stats['jobs_failed_duration_total_seconds'] += duration
        else:
            stats['jobs_successful_duration_total_seconds'] += duration
        stats['job_duration_maximum_seconds'] = self._merge_max_value(stats['job_duration_maximum_seconds'], duration)
        stats['job_duration_minimum_seconds'] = self._merge_min_value(stats['job_duration_minimum_seconds'], duration)

    def _update_waiting_time_stats(self, stats, waiting_time):
        """Update waiting-time-related fields in a collection stats entry."""
        if waiting_time is None or pd.isna(waiting_time):
            return
        stats['job_waiting_time_total_seconds'] += waiting_time
        stats['job_waiting_time_maximum_seconds'] = self._merge_max_value(stats['job_waiting_time_maximum_seconds'], waiting_time)
        stats['job_waiting_time_minimum_seconds'] = self._merge_min_value(stats['job_waiting_time_minimum_seconds'], waiting_time)

    def _update_collection_set_fields(self, stats, row_stats):
        """Add template, inventory, and ansible_version values to their respective sets."""
        template_id = row_stats.get('unified_job_template_id')
        if self._is_valid_id(template_id):
            stats['templates'].add(template_id)

        inventory_id = row_stats.get('inventory_id')
        if self._is_valid_id(inventory_id):
            stats['inventories'].add(inventory_id)

        ansible_version = row_stats.get('ansible_version')
        if self._is_valid_id(ansible_version):
            stats['ansible_versions'].add(str(ansible_version))

    def _process_single_collection(self, collection_name, collection_info, collections_stats, failed, row_stats):
        """Process one collection entry from a job row, updating collections_stats in place."""
        if not isinstance(collection_info, dict):
            return

        version = collection_info.get('version', '')
        if not version:
            return

        key = (collection_name, str(version))
        if key not in collections_stats:
            collections_stats[key] = self._init_collection_stats_entry()

        stats = collections_stats[key]
        stats['job_count'] += 1
        if failed:
            stats['jobs_failed_total'] += 1
        else:
            stats['jobs_successful_total'] += 1

        if row_stats.get('jobs_never_started'):
            stats['jobs_never_started_total'] += 1

        self._update_duration_stats(stats, row_stats.get('job_duration_seconds'), failed)
        self._update_waiting_time_stats(stats, row_stats.get('job_waiting_time_seconds'))
        self._update_collection_set_fields(stats, row_stats)

    def _process_collections_dict(self, collections_data, collections_stats, failed, row_stats):
        """
        Process a collections dict and update the stats dict with collection name/version pairs,
        tracking job_count, jobs_failed_total, jobs_successful_total and additional job statistics.

        row_stats is a dict with per-row statistics:
            job_duration_seconds, job_waiting_time_seconds, jobs_never_started,
            unified_job_template_id, inventory_id, ansible_version.
        """
        if not isinstance(collections_data, dict):
            return

        for collection_name, collection_info in collections_data.items():
            self._process_single_collection(collection_name, collection_info, collections_stats, failed, row_stats)

    def _hash_installed_collections(self, raw):
        """Compute a SHA-256 hash of the raw installed_collections string.

        Used as a cache key to avoid re-parsing identical JSON payloads
        (e.g. all jobs that share the same execution environment).
        SHA-256 is used instead of the raw string to keep the cache memory-efficient
        when the JSON payload is large.
        """
        return hashlib.sha256(raw.encode('utf-8', errors='replace')).hexdigest()

    def _process_collections_from_jobs(self, dataframe):
        """
        Extract unique collection name and version pairs from jobs dataframe.
        Count how many jobs use each unique collection+version combination,
        including failed and successful job counts.

        Optimized version using itertuples() for better performance.
        Additionally uses a hash-based cache to avoid re-parsing identical
        installed_collections JSON payloads (common when many jobs share the
        same execution environment).

        Returns a list of dicts with:
        - collection_name: str
        - collection_version: str
        - job_count: int
        - jobs_failed_total: int
        - jobs_successful_total: int
        """
        if 'installed_collections' not in dataframe.columns:
            return []

        # Use dict for tracking job_count, jobs_failed_total, jobs_successful_total per collection+version
        collections_stats = {}

        # Cache: SHA-256 hash of raw JSON string -> parsed collections dict
        # Many jobs share the same installed_collections because they run on the
        # same execution environment, so we avoid redundant json.loads() calls.
        parse_cache = {}

        # Use itertuples() for fastest row iteration (10-100x faster than iterrows)
        # itertuples() creates namedtuples with column names as attributes
        # Column names with special characters are sanitized, but 'installed_collections' should work fine
        for row in dataframe.itertuples(index=False):
            installed_collections_data = getattr(row, 'installed_collections', None)

            if not installed_collections_data or pd.isna(installed_collections_data):
                continue

            # Use hash of the raw string as cache key to avoid storing large strings
            raw = installed_collections_data if isinstance(installed_collections_data, str) else str(installed_collections_data)
            cache_key = self._hash_installed_collections(raw)

            if cache_key not in parse_cache:
                parse_cache[cache_key] = self._parse_collections_data(installed_collections_data)

            collections_data = parse_cache[cache_key]
            if collections_data:
                failed = bool(getattr(row, 'failed', False))
                row_stats = {
                    'job_duration_seconds': getattr(row, 'job_duration_seconds', None),
                    'job_waiting_time_seconds': getattr(row, 'job_waiting_time_seconds', None),
                    'jobs_never_started': bool(getattr(row, 'jobs_never_started', False)),
                    'unified_job_template_id': getattr(row, 'unified_job_template_id', None),
                    'inventory_id': getattr(row, 'inventory_id', None),
                    'ansible_version': getattr(row, 'ansible_version', None),
                }
                self._process_collections_dict(collections_data, collections_stats, failed, row_stats)

        # Convert dict to list of dicts, converting sets to sorted lists
        result = [
            {
                'collection_name': collection_name,
                'collection_version': collection_version,
                'job_count': stats['job_count'],
                'jobs_failed_total': stats['jobs_failed_total'],
                'jobs_successful_total': stats['jobs_successful_total'],
                'jobs_never_started_total': stats['jobs_never_started_total'],
                'jobs_duration_total_seconds': stats['jobs_duration_total_seconds'],
                'jobs_successful_duration_total_seconds': stats['jobs_successful_duration_total_seconds'],
                'jobs_failed_duration_total_seconds': stats['jobs_failed_duration_total_seconds'],
                'job_duration_maximum_seconds': stats['job_duration_maximum_seconds'],
                'job_duration_minimum_seconds': stats['job_duration_minimum_seconds'],
                'job_waiting_time_total_seconds': stats['job_waiting_time_total_seconds'],
                'job_waiting_time_maximum_seconds': stats['job_waiting_time_maximum_seconds'],
                'job_waiting_time_minimum_seconds': stats['job_waiting_time_minimum_seconds'],
                'templates': sorted(stats['templates']),
                'inventories': sorted(stats['inventories']),
                'templates_total': len(stats['templates']),
                'inventories_total': len(stats['inventories']),
                'ansible_versions': sorted(stats['ansible_versions']),
            }
            for (collection_name, collection_version), stats in collections_stats.items()
        ]

        # Sort by collection_name, then by collection_version for consistent output
        result.sort(key=lambda x: (x['collection_name'], x['collection_version']))

        return result
