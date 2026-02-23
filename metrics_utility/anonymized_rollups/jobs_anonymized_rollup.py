import json

from collections import Counter

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class JobsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - unified_jobs collector data
    """

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
            'job_ids': ('id', lambda x: sorted(list(set(x.dropna())))),
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
            'templates': ('job_template_name', lambda x: sorted(list(set(x.dropna())))),
            'inventories': ('inventory_name', lambda x: sorted(list(set(x.dropna())))),
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
        self._add_totals_to_aggregation(
            aggregations_by_job_type, [('job_ids', 'jobs_total'), ('templates', 'templates_total'), ('inventories', 'inventories_total')]
        )

        return aggregations_by_job_type

    def _aggregate_by_launch_type(self, dataframe, common_aggregations, ansible_versions_aggregation):
        """Aggregate by launch_type."""
        aggregations_by_launch_type_dict = common_aggregations.copy()
        aggregations_by_launch_type_dict.update({'job_types': ('model', lambda x: sorted(list(set(x.dropna()))))})
        aggregations_by_launch_type_dict.update(ansible_versions_aggregation)

        aggregations_by_launch_type = dataframe.groupby('launch_type').agg(**aggregations_by_launch_type_dict).reset_index()
        self._add_totals_to_aggregation(
            aggregations_by_launch_type,
            [
                ('job_ids', 'jobs_total'),
                ('job_types', 'job_type_total'),
                ('templates', 'templates_total'),
                ('inventories', 'inventories_total'),
            ],
        )

        return aggregations_by_launch_type

    def _aggregate_by_ansible_version(self, dataframe, common_aggregations):
        """Aggregate by ansible_version."""
        aggregations_by_ansible_version_dict = common_aggregations.copy()
        aggregations_by_ansible_version_dict.update({'job_types': ('model', lambda x: sorted(list(set(x.dropna()))))})

        aggregations_by_ansible_version = dataframe.groupby('ansible_version').agg(**aggregations_by_ansible_version_dict).reset_index()

        self._add_totals_to_aggregation(
            aggregations_by_ansible_version,
            [
                ('job_ids', 'jobs_total'),
                ('job_types', 'job_type_total'),
                ('templates', 'templates_total'),
                ('inventories', 'inventories_total'),
            ],
        )

        return aggregations_by_ansible_version

    def _extract_metadata(self, dataframe):
        """Extract metadata fields from dataframe."""
        organizations = sorted(list(set(dataframe['organization_name'].dropna().unique())))
        job_ids = sorted(list(set(dataframe['id'].dropna().unique())))
        forks_total = int(dataframe['forks'].sum()) if 'forks' in dataframe.columns else 0

        scm_types = []
        if 'scm_type' in dataframe.columns:
            scm_types = sorted([str(v) for v in dataframe['scm_type'].dropna().unique() if pd.notna(v) and str(v).strip()])

        return organizations, job_ids, forks_total, scm_types

    def prepare(self, dataframe):
        # Convert ID columns to strings at the beginning
        dataframe = self._convert_id_columns_to_strings(dataframe)
        
        # Filter out jobs that are not finished
        dataframe = dataframe[dataframe['finished'].notna()]

        # Handle empty dataframe
        if dataframe.empty:
            return sanitize_json({
                'by_job_type': [],
                'by_launch_type': [],
                'by_ansible_version': [],
                'organizations': [],
                'forks_total': 0,
                'job_ids': [],
                'scm_types': [],
                'installed_collections': [],
            })

        # Preprocess dataframe
        dataframe = self._preprocess_dataframe(dataframe)

        # Get aggregation dictionaries
        common_aggregations = self._get_common_aggregations()
        ansible_versions_aggregation = self._get_ansible_versions_aggregation()

        # Perform aggregations by different dimensions
        aggregations_by_job_type = self._aggregate_by_job_type(dataframe, common_aggregations, ansible_versions_aggregation)
        aggregations_by_launch_type = self._aggregate_by_launch_type(dataframe, common_aggregations, ansible_versions_aggregation)
        aggregations_by_ansible_version = self._aggregate_by_ansible_version(dataframe, common_aggregations)

        # Convert DataFrames to JSON (list of dicts)
        by_job_type = aggregations_by_job_type.to_dict(orient='records')
        by_launch_type = aggregations_by_launch_type.to_dict(orient='records')
        by_ansible_version = aggregations_by_ansible_version.to_dict(orient='records')

        # Extract metadata
        organizations, job_ids, forks_total, scm_types = self._extract_metadata(dataframe)

        # Process collections statistics
        collections_stats = self._process_collections_from_jobs(dataframe)

        result = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'organizations': organizations,
            'forks_total': forks_total,
            'job_ids': job_ids,
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
            'job_type_total',
        ]

        # List columns to union
        list_cols = ['job_ids', 'templates', 'inventories', 'ansible_versions', 'job_types']

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

    def _merge_max_columns(self, merged_item, item_all, item_new):
        """Take maximum value for max columns."""
        max_cols = ['job_duration_maximum_seconds', 'job_waiting_time_maximum_seconds']
        for col in max_cols:
            if col in merged_item:
                val_all = item_all.get(col)
                val_new = item_new.get(col)
                if val_all is not None and val_new is not None:
                    merged_item[col] = max(val_all, val_new)
                elif val_all is not None:
                    merged_item[col] = val_all
                elif val_new is not None:
                    merged_item[col] = val_new

    def _merge_min_columns(self, merged_item, item_all, item_new):
        """Take minimum value for min columns."""
        min_cols = ['job_duration_minimum_seconds', 'job_waiting_time_minimum_seconds']
        for col in min_cols:
            if col in merged_item:
                val_all = item_all.get(col)
                val_new = item_new.get(col)
                if val_all is not None and val_new is not None:
                    merged_item[col] = min(val_all, val_new)
                elif val_all is not None:
                    merged_item[col] = val_all
                elif val_new is not None:
                    merged_item[col] = val_new

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
        if 'job_ids' in merged_item:
            merged_item['jobs_total'] = len(merged_item['job_ids'])
        if 'templates' in merged_item:
            merged_item['templates_total'] = len(merged_item['templates'])
        if 'inventories' in merged_item:
            merged_item['inventories_total'] = len(merged_item['inventories'])
        if 'job_types' in merged_item:
            merged_item['job_type_total'] = len(merged_item['job_types'])

    def _merge_list_fields(self, data_all, data_new, field_name):
        """Merge list fields by union and sort."""
        all_set = set(data_all.get(field_name, []))
        new_set = set(data_new.get(field_name, []))
        return sorted(list(all_set.union(new_set)))

    def _merge_collections(self, data_all, data_new):
        """Merge installed_collections by summing job_count for same collection+version."""
        collections_all = {
            (item['collection_name'], item['collection_version']): item['job_count'] for item in data_all.get('installed_collections', [])
        }
        collections_new = {
            (item['collection_name'], item['collection_version']): item['job_count'] for item in data_new.get('installed_collections', [])
        }

        merged_collections = {}
        all_collection_keys = set(collections_all.keys()) | set(collections_new.keys())
        for key in all_collection_keys:
            merged_collections[key] = collections_all.get(key, 0) + collections_new.get(key, 0)

        installed_collections = [
            {
                'collection_name': collection_name,
                'collection_version': collection_version,
                'job_count': job_count,
            }
            for (collection_name, collection_version), job_count in merged_collections.items()
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

        # Merge by_job_type, by_launch_type, by_ansible_version
        by_job_type = self._merge_stats_json(data_all.get('by_job_type', []), data_new.get('by_job_type', []), 'job_type')
        by_launch_type = self._merge_stats_json(data_all.get('by_launch_type', []), data_new.get('by_launch_type', []), 'launch_type')
        by_ansible_version = self._merge_stats_json(data_all.get('by_ansible_version', []), data_new.get('by_ansible_version', []), 'ansible_version')

        # Merge list fields
        organizations = self._merge_list_fields(data_all, data_new, 'organizations')
        job_ids = self._merge_list_fields(data_all, data_new, 'job_ids')
        scm_types = self._merge_list_fields(data_all, data_new, 'scm_types')

        # Sum forks_total
        forks_total = data_all.get('forks_total', 0) + data_new.get('forks_total', 0)

        # Merge collections
        installed_collections = self._merge_collections(data_all, data_new)

        return {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
            'organizations': organizations,
            'forks_total': forks_total,
            'job_ids': job_ids,
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
        organizations = data.get('organizations', [])
        forks_total = data.get('forks_total', 0)
        job_ids = data.get('job_ids', [])
        scm_types = data.get('scm_types', [])
        installed_collections = data.get('installed_collections', [])

        # Handle empty data
        if not by_job_type and not by_launch_type and not by_ansible_version:
            return {
                'json': {
                    'by_job_type': [],
                    'by_launch_type': [],
                    'by_ansible_version': [],
                    'organizations_total': 0,
                    'forks_total': 0,
                    'jobs_total': 0,
                    'installed_collections': [],
                    'scm_types': [],
                },
            }

        # Drop list columns from stats (we only need the computed totals, not the raw lists)
        for stats_list in [by_job_type, by_launch_type, by_ansible_version]:
            for item in stats_list:
                # Drop list columns that were used for deduplication
                for col in ['job_ids', 'templates', 'inventories', 'job_types']:
                    if col in item:
                        del item[col]

        # Compute final totals from lists
        organizations_total = len(organizations)
        jobs_total = len(job_ids)

        # Prepare JSON data (already in JSON format)
        json_data = {
            'by_job_type': by_job_type,
            'by_launch_type': by_launch_type,
            'by_ansible_version': by_ansible_version,
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

    def _process_collections_dict(self, collections_data, collections_counter):
        """
        Process a collections dict and update the counter with collection name/version pairs.
        """
        if not isinstance(collections_data, dict):
            return

        for collection_name, collection_info in collections_data.items():
            if not isinstance(collection_info, dict):
                continue

            version = collection_info.get('version', '')
            if version:
                collections_counter[(collection_name, str(version))] += 1

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
            collections_data = self._parse_collections_data(installed_collections_data)
            if collections_data:
                self._process_collections_dict(collections_data, collections_counter)

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
