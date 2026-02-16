import json
import os
import re

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage


_COLLECTION_RE = re.compile(r'^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$')


def extract_collection_name(x: str | None) -> str | None:
    if not x:
        return None
    m = _COLLECTION_RE.match(x)
    return f'{m.group(1)}.{m.group(2)}' if m else None


def merge_by_name(obj1, obj2, name_key):
    merged = {}

    for entry in obj1 + obj2:
        key = entry[name_key]
        merged.setdefault(key, {}).update(entry)

    # Convert dict back to list
    merged_list = list(merged.values())

    return merged_list


class EventModulesAnonymizedRollup(BaseAnonymizedRollup):
    """
    Event collections rollups operate over main_jobevent_service collector data

    Important columns in data:
    module_name (task_action) - name of the module that was executed
    job_id - id of the job that was executed
    host_id - id of the host that was automated
    playbook - name of the playbook that was executed
    job_created - timestamp of the job creation
    job_started - timestamp of the job start
    job_finished - timestamps of the job finish
    event - name of the event that was executed
    task_uuid - uuid of the task that was executed

    Computed columns:
    job_duration - duration of the job in seconds (computed from job_started and job_finished)
    job_waiting_time - waiting time of the job in seconds (computed from job_created and job_started)
    job_failed - boolean flag indicating if the job failed
    collection_name - name of the collection that was used - from module_name
    collection_source - source of the collection (e.g. Red Hat, Partner A, Community) - from collections_types

    task_success_event - boolean flag indicating if the task was successful
    task_failed_event - boolean flag indicating if the task failed
    task_unreachable_event - boolean flag indicating if the task was unreachable
    task_skipped_event - boolean flag indicating if the task was skipped

    How the events works?
    Job is created and executed on each host - defined by playbook.
    Task is a single action that is executed on a host. Task calls module. Module is also part of the collection.
    Collection can come from different sources:
    - Red Hat
    - Partner A
    - Community
    - Validated
    - etc.

    When task fails, it can be retried multiple times.
    When task is successful, it is not retried.
    When task is skipped, it is not retried.
    When task is ignored, it is not retried.
    """

    def __init__(self):
        super().__init__('events_modules')

        self.collector_names = ['main_jobevent_service']

        # Open the JSON file using path relative to this module
        collections_path = os.path.join(os.path.dirname(__file__), 'collections.json')
        with open(collections_path, 'r') as f:
            self.collections = json.load(f)

    def merge(self, data_all, data_new):
        """
        Override merge to aggregate module_stats, collection_stats, role_stats from batches.
        Sums numeric columns and unions sets for proper deduplication.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        def merge_stats_df(df_all, df_new, groupby_cols):
            """Merge two stats dataframes by summing numeric columns and unioning sets."""
            if df_all.empty:
                return df_new.copy() if not df_new.empty else pd.DataFrame()
            if df_new.empty:
                return df_all.copy() if not df_all.empty else pd.DataFrame()

            # Merge on grouping columns
            merged = pd.merge(
                df_all,
                df_new,
                on=groupby_cols,
                how='outer',
                suffixes=('_all', '_new')
            )

            # Numeric columns to sum
            numeric_cols = [
                'jobs_total', 'jobs_successful_total', 'jobs_failed_total',
                'jobs_duration_total_seconds', 'jobs_waiting_time_total_seconds',
                'jobs_never_started_total', 'task_ok_total', 'task_ok_with_retries_total',
                'task_failed_total', 'task_unreachable_total', 'task_skipped_total',
                'task_failed_and_ignored_total', 'jobs_failed_because_of_module_failure_total',
                'jobs_successful_duration_total_seconds', 'jobs_failed_duration_total_seconds',
                'warnings_total', 'deprecations_total', 'processed_events_total', 'tasks_total',
            ]

            # Sum numeric columns
            for col in numeric_cols:
                if f'{col}_all' in merged.columns and f'{col}_new' in merged.columns:
                    merged[col] = merged[f'{col}_all'].fillna(0) + merged[f'{col}_new'].fillna(0)
                elif f'{col}_all' in merged.columns:
                    merged[col] = merged[f'{col}_all'].fillna(0)
                elif f'{col}_new' in merged.columns:
                    merged[col] = merged[f'{col}_new'].fillna(0)

            # Set columns to union
            set_cols = ['host_ids', 'controller_versions']
            for col in set_cols:
                if f'{col}_all' in merged.columns and f'{col}_new' in merged.columns:
                    def union_sets(row):
                        set_all = row[f'{col}_all'] if pd.notna(row[f'{col}_all']) else set()
                        set_new = row[f'{col}_new'] if pd.notna(row[f'{col}_new']) else set()
                        if isinstance(set_all, set) and isinstance(set_new, set):
                            return set_all.union(set_new)
                        elif isinstance(set_all, set):
                            return set_all
                        elif isinstance(set_new, set):
                            return set_new
                        else:
                            return set()
                    merged[col] = merged.apply(union_sets, axis=1)
                elif f'{col}_all' in merged.columns:
                    merged[col] = merged[f'{col}_all']
                elif f'{col}_new' in merged.columns:
                    merged[col] = merged[f'{col}_new']

            # Recompute unique_hosts_total from host_ids set
            if 'host_ids' in merged.columns:
                merged['unique_hosts_total'] = merged['host_ids'].apply(lambda x: len(x) if isinstance(x, set) else 0)

            # Drop intermediate columns
            cols_to_drop = [c for c in merged.columns if c.endswith('_all') or c.endswith('_new')]
            merged = merged.drop(columns=cols_to_drop)

            return merged

        # Merge module_stats
        module_stats = merge_stats_df(
            data_all.get('module_stats', pd.DataFrame()),
            data_new.get('module_stats', pd.DataFrame()),
            ['module_name', 'collection_source', 'collection_name']
        )

        # Merge collection_stats
        collection_stats = merge_stats_df(
            data_all.get('collection_stats', pd.DataFrame()),
            data_new.get('collection_stats', pd.DataFrame()),
            ['collection_name', 'collection_source']
        )

        # Merge role_stats
        role_stats = merge_stats_df(
            data_all.get('role_stats', pd.DataFrame()),
            data_new.get('role_stats', pd.DataFrame()),
            ['role', 'collection_name', 'collection_source']
        )

        # Merge unique_modules sets
        unique_modules_all = data_all.get('unique_modules', set())
        unique_modules_new = data_new.get('unique_modules', set())
        unique_modules = unique_modules_all.union(unique_modules_new) if isinstance(unique_modules_all, set) and isinstance(unique_modules_new, set) else (unique_modules_all if isinstance(unique_modules_all, set) else unique_modules_new)

        # Merge modules_per_playbook dicts (union sets per playbook)
        modules_per_playbook_all = data_all.get('modules_per_playbook', {})
        modules_per_playbook_new = data_new.get('modules_per_playbook', {})
        modules_per_playbook = {}
        all_playbooks = set(modules_per_playbook_all.keys()) | set(modules_per_playbook_new.keys())
        for playbook in all_playbooks:
            set_all = modules_per_playbook_all.get(playbook, set())
            set_new = modules_per_playbook_new.get(playbook, set())
            if isinstance(set_all, set) and isinstance(set_new, set):
                modules_per_playbook[playbook] = set_all.union(set_new)
            elif isinstance(set_all, set):
                modules_per_playbook[playbook] = set_all
            elif isinstance(set_new, set):
                modules_per_playbook[playbook] = set_new
            else:
                modules_per_playbook[playbook] = set()

        # Merge unique_hosts sets
        unique_hosts_all = data_all.get('unique_hosts', set())
        unique_hosts_new = data_new.get('unique_hosts', set())
        unique_hosts = unique_hosts_all.union(unique_hosts_new) if isinstance(unique_hosts_all, set) and isinstance(unique_hosts_new, set) else (unique_hosts_all if isinstance(unique_hosts_all, set) else unique_hosts_new)

        return {
            'collected_events_total': data_all['collected_events_total'] + data_new['collected_events_total'],
            'warnings_total': data_all.get('warnings_total', 0) + data_new.get('warnings_total', 0),
            'deprecations_total': data_all.get('deprecations_total', 0) + data_new.get('deprecations_total', 0),
            'module_stats': module_stats,
            'collection_stats': collection_stats,
            'role_stats': role_stats,
            'unique_modules': unique_modules,
            'modules_per_playbook': modules_per_playbook,
            'unique_hosts': unique_hosts,
        }

    # Prepare is run for each batch of data
    # then it is merged with other batches into one dataframes
    # as default, merging is done by concatenating dataframes (defined in base class)
    def prepare(self, dataframe):
        # Count all events before pruning
        collected_events_total = len(dataframe) if dataframe is not None and not dataframe.empty else 0

        # Count warnings and deprecations before filtering
        # These events don't have task_uuid, host_id, module_name, etc., so they're filtered out later
        # but we count them here for statistics
        if dataframe is None or dataframe.empty or 'event' not in dataframe.columns:
            warnings_total = 0
            deprecations_total = 0
        else:
            warnings_total = len(dataframe[dataframe['event'] == 'warning'])
            deprecations_total = len(dataframe[dataframe['event'] == 'deprecated'])

        # Failure/Success rate of modules
        success_events_list = ['runner_on_ok', 'runner_on_async_ok', 'runner_item_on_ok']
        failed_events_list = ['runner_on_failed', 'runner_on_async_failed', 'runner_item_on_failed']
        unreachable_events_list = ['runner_on_unreachable', 'runner_item_on_unreachable']
        skipped_events_list = ['runner_on_skipped', 'runner_item_on_skipped']
        warnings_and_deprecations_events_list = ['warning', 'deprecated']

        # Filter for only the event types that are used in analysis
        all_relevant_events = (
            success_events_list + failed_events_list + unreachable_events_list + skipped_events_list + warnings_and_deprecations_events_list
        )
        dataframe = dataframe[dataframe['event'].isin(all_relevant_events)]

        # Prepare data
        collections = self.collections

        # if missing ignore_errors column, insert it, default is False. If values is null, set it to False
        if 'ignore_errors' not in dataframe.columns:
            dataframe['ignore_errors'] = False

        dataframe['ignore_errors'] = dataframe['ignore_errors'].fillna(False).astype(bool)

        # Coerce datetime-like columns to pandas datetimes (UTC) to accept strings like '...+00'
        for col in ['job_created', 'job_started', 'job_finished']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        # add module column into the dataframe based on dataframe_content_usage.py approach
        dataframe['module_name'] = (
            dataframe['resolved_action'].fillna(dataframe['task_action']).where(lambda s: s.notna() & (s.astype(str).str.strip() != ''))
        )

        # add role column into the dataframe based on dataframe_content_usage.py approach
        # If resolved_role is not there, fill it with role column
        dataframe['role'] = dataframe['resolved_role'].fillna(dataframe['role']).astype(str)
        # Only get valid role names into role column
        dataframe['role'] = dataframe['role'].apply(lambda x: DataframeContentUsage.extract_role_name(x))

        dataframe = dataframe.assign(job_failed=dataframe['job_failed'].fillna(False).astype(bool))

        # Vectorized extraction of collection name (much faster than .apply())
        # Extract first two parts (namespace.collection) from module name
        # Requires at least 3 parts: namespace.collection.module
        dataframe['collection_name'] = dataframe['module_name'].str.extract(
            r'^([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$', expand=False
        )

        dataframe['job_duration_seconds'] = (dataframe['job_finished'] - dataframe['job_started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['job_started'] - dataframe['job_created']).dt.total_seconds()

        # fill collection source from collections_types
        dataframe['collection_source'] = dataframe['collection_name'].map(collections).fillna('Unknown')

        # Mark events
        dataframe['task_success_event'] = dataframe['event'].isin(success_events_list)

        dataframe['task_failed_event'] = dataframe['event'].isin(failed_events_list) & ~dataframe['ignore_errors']
        dataframe['task_failed_and_ignored_event'] = dataframe['event'].isin(failed_events_list) & dataframe['ignore_errors']
        dataframe['task_unreachable_event'] = dataframe['event'].isin(unreachable_events_list)
        dataframe['task_skipped_event'] = dataframe['event'].isin(skipped_events_list)

        # determine module level warnings and deprecations
        # Ensure warnings and deprecations columns exist (they may be missing in test data)
        if 'warnings' not in dataframe.columns:
            dataframe['warnings'] = None
        if 'deprecations' not in dataframe.columns:
            dataframe['deprecations'] = None

        # Parse JSON arrays and check if they contain items
        def parse_and_check_json_array(x):
            """Parse JSON array (string, list, or dict) and return True if it contains items."""
            if pd.isnull(x):
                return False
            try:
                # If already a list/dict, use it directly
                if isinstance(x, (list, dict)):
                    parsed = x
                else:
                    # If string, parse it
                    parsed = json.loads(x) if isinstance(x, str) else x
                # Check if it's a non-empty array
                if isinstance(parsed, list):
                    return len(parsed) > 0
                # If it's a dict or other structure, check if it's truthy
                return bool(parsed)
            except (json.JSONDecodeError, TypeError, ValueError):
                return False

        dataframe['is_warning'] = dataframe['warnings'].apply(parse_and_check_json_array).astype(bool)
        dataframe['is_deprecation'] = dataframe['deprecations'].apply(parse_and_check_json_array).astype(bool)

        dataframe = dataframe[
            dataframe['module_name'].notna()
            & dataframe['host_id'].notna()
            & dataframe['playbook'].notna()
            & dataframe['job_id'].notna()
            & (dataframe['module_name'].str.strip() != '')
            & (dataframe['playbook'].str.strip() != '')
        ]

        # rename ansible_version to controller_version, the fast way
        # change the metadata, no dataframe copy
        # If ansible_version doesn't exist, create controller_version with None values
        if 'ansible_version' in dataframe.columns:
            dataframe.rename(columns={'ansible_version': 'controller_version'}, inplace=True)
        else:
            dataframe['controller_version'] = None

        # Select only the columns needed for analysis to save memory
        columns_to_keep = [
            'job_id',
            'host_id',
            'task_uuid',
            'module_name',
            'playbook',
            'collection_name',
            'collection_source',
            'role',
            'job_failed',
            'job_started',
            'job_duration_seconds',
            'job_waiting_time_seconds',
            'task_success_event',
            'task_failed_event',
            'task_failed_and_ignored_event',
            'task_unreachable_event',
            'task_skipped_event',
            'event',
            'is_warning',
            'is_deprecation',
            'controller_version',
        ]

        dataframe = dataframe[columns_to_keep]

        # This groups by (job, host, task, module, collection) and summarizes all events
        # This can reduce the number of rows, depends of number of retries
        # Note: role is kept as a column but not used for grouping here to avoid splitting tasks unnecessarily
        task_summary = (
            dataframe.groupby(
                ['job_id', 'host_id', 'task_uuid', 'module_name', 'collection_source', 'collection_name'], as_index=False, observed=True
            )
            .agg(
                seen_success=('task_success_event', 'max'),
                seen_failed=('task_failed_event', 'max'),
                seen_unreachable=('task_unreachable_event', 'max'),
                seen_skipped=('task_skipped_event', 'max'),
                seen_failed_and_ignored=('task_failed_and_ignored_event', 'max'),
                job_started=('job_started', 'first'),
                job_failed=('job_failed', 'first'),
                job_duration_seconds=('job_duration_seconds', 'first'),
                job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
                playbook=('playbook', 'first'),
                warnings_total=('is_warning', 'sum'),
                deprecations_total=('is_deprecation', 'sum'),
                processed_events_total=('event', 'size'),
                controller_version=('controller_version', 'first'),
                role=('role', 'first'),  # Keep role for later aggregation
            )
            .assign(
                # mutually exclusive categories - only one can be true
                task_clean_success=lambda x: x['seen_success'] & ~x['seen_failed'] & ~x['seen_unreachable'] & ~x['seen_skipped'],
                task_success_with_reruns=lambda x: x['seen_success'] & (x['seen_failed'] | x['seen_unreachable']),
                task_failed=lambda x: x['seen_failed'] & ~x['seen_success'],
                task_failed_and_ignored=lambda x: x['seen_failed_and_ignored'] & ~x['seen_success'],
                task_unreachable=lambda x: x['seen_unreachable'] & ~x['seen_success'] & ~x['seen_failed'] & ~x['seen_failed_and_ignored'],
                task_skipped=lambda x: (
                    x['seen_skipped'] & ~x['seen_success'] & ~x['seen_failed'] & ~x['seen_unreachable'] & ~x['seen_failed_and_ignored']
                ),
                job_id_that_contained_failed_task=lambda df: df['job_id'].where(df['task_failed']),
            )
        )

        # aggregate task_summary by job_id, task_uuid, module_name, collection_source, collection_name
        # aggregate data for hosts together
        # note that prepare is called in batches and one job and task can be split between batches
        # This will cause acceptable precision loss, because we will end up with two entries for the same job and task
        # duplicated entries will be summed in merge function
        # Note: role is kept as a column but not used for grouping here
        task_summary = task_summary.groupby(
            ['job_id', 'task_uuid', 'module_name', 'collection_source', 'collection_name'], as_index=False, observed=True
        ).agg(
            task_clean_success=('task_clean_success', 'sum'),
            task_success_with_reruns=('task_success_with_reruns', 'sum'),
            task_failed=('task_failed', 'sum'),
            task_failed_and_ignored=('task_failed_and_ignored', 'sum'),
            task_unreachable=('task_unreachable', 'sum'),
            task_skipped=('task_skipped', 'sum'),
            job_id_that_contained_failed_task=('job_id_that_contained_failed_task', 'first'),
            # Preserve columns needed for aggregation
            job_started=('job_started', 'first'),
            job_failed=('job_failed', 'first'),
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            playbook=('playbook', 'first'),
            host_ids=('host_id', lambda x: set(x)),
            warnings_total=('warnings_total', 'sum'),
            deprecations_total=('deprecations_total', 'sum'),
            processed_events_total=('processed_events_total', 'sum'),
            controller_version=('controller_version', 'first'),
            role=('role', 'first'),  # Keep role for role_stats aggregation
        )

        # Handle empty task_summary
        if task_summary.empty:
            return {
                'collected_events_total': collected_events_total,
                'warnings_total': warnings_total,
                'deprecations_total': deprecations_total,
                'module_stats': pd.DataFrame(),
                'collection_stats': pd.DataFrame(),
                'role_stats': pd.DataFrame(),
                'unique_modules': set(),
                'modules_per_playbook': {},
                'unique_hosts': set(),
            }

        # Compute duration columns before aggregation
        task_summary = task_summary.assign(
            jobs_successful_duration_total_seconds=lambda x: x['job_duration_seconds'].where(~x['job_failed'], 0),
            jobs_failed_duration_total_seconds=lambda x: x['job_duration_seconds'].where(x['job_failed'], 0),
        )

        # Common aggregation for module_stats, collection_stats, and role_stats
        common_aggregation = {
            'jobs_total': ('job_id', 'nunique'),
            'jobs_successful_total': ('job_failed', lambda x: (~x).sum()),
            'jobs_failed_total': ('job_failed', 'sum'),
            'jobs_duration_total_seconds': ('job_duration_seconds', 'sum'),
            'jobs_waiting_time_total_seconds': ('job_waiting_time_seconds', 'sum'),
            'jobs_never_started_total': ('job_started', lambda x: x.isna().sum()),
            'host_ids': ('host_ids', lambda x: set().union(*[s for s in x.dropna() if isinstance(s, set)])),
            'task_ok_total': ('task_clean_success', 'sum'),
            'task_ok_with_retries_total': ('task_success_with_reruns', 'sum'),
            'task_failed_total': ('task_failed', 'sum'),
            'task_unreachable_total': ('task_unreachable', 'sum'),
            'task_skipped_total': ('task_skipped', 'sum'),
            'task_failed_and_ignored_total': ('task_failed_and_ignored', 'sum'),
            'jobs_failed_because_of_module_failure_total': ('job_id_that_contained_failed_task', 'nunique'),
            'jobs_successful_duration_total_seconds': ('jobs_successful_duration_total_seconds', 'sum'),
            'jobs_failed_duration_total_seconds': ('jobs_failed_duration_total_seconds', 'sum'),
            'warnings_total': ('warnings_total', 'sum'),
            'deprecations_total': ('deprecations_total', 'sum'),
            'processed_events_total': ('processed_events_total', 'sum'),
            'controller_versions': ('controller_version', lambda x: set(x.dropna())),
        }

        # Compute module_stats
        module_stats = task_summary.groupby(
            ['module_name', 'collection_source', 'collection_name'], as_index=False, observed=True
        ).agg(**common_aggregation)
        module_stats['tasks_total'] = (
            module_stats['task_ok_total']
            + module_stats['task_ok_with_retries_total']
            + module_stats['task_failed_total']
            + module_stats['task_unreachable_total']
            + module_stats['task_skipped_total']
            + module_stats['task_failed_and_ignored_total']
        )
        module_stats['unique_hosts_total'] = module_stats['host_ids'].apply(lambda x: len(x) if isinstance(x, set) else 0)

        # Compute collection_stats
        collection_stats = task_summary.groupby(
            ['collection_name', 'collection_source'], as_index=False, observed=True
        ).agg(**common_aggregation)
        collection_stats['tasks_total'] = (
            collection_stats['task_ok_total']
            + collection_stats['task_ok_with_retries_total']
            + collection_stats['task_failed_total']
            + collection_stats['task_unreachable_total']
            + collection_stats['task_skipped_total']
            + collection_stats['task_failed_and_ignored_total']
        )
        collection_stats['unique_hosts_total'] = collection_stats['host_ids'].apply(lambda x: len(x) if isinstance(x, set) else 0)

        # Extract role collection information for role_stats
        task_summary['role_collection_name'] = (
            task_summary['role'].astype(str).apply(lambda x: extract_collection_name(x) if x and x != 'nan' else None)
        )
        role_collection_source_str = task_summary['role_collection_name'].astype(str).map(self.collections)
        task_summary['role_collection_source'] = role_collection_source_str.fillna('Unknown')

        # Compute role_stats
        role_stats = task_summary.groupby(
            ['role', 'role_collection_name', 'role_collection_source'], as_index=False, observed=True
        ).agg(**common_aggregation)
        role_stats = role_stats.rename(columns={'role_collection_name': 'collection_name', 'role_collection_source': 'collection_source'})
        role_stats['tasks_total'] = (
            role_stats['task_ok_total']
            + role_stats['task_ok_with_retries_total']
            + role_stats['task_failed_total']
            + role_stats['task_unreachable_total']
            + role_stats['task_skipped_total']
            + role_stats['task_failed_and_ignored_total']
        )
        role_stats['unique_hosts_total'] = role_stats['host_ids'].apply(lambda x: len(x) if isinstance(x, set) else 0)

        # Compute unique_modules set
        unique_modules = set(task_summary['module_name'].dropna().unique())

        # Compute modules_per_playbook dict (playbook -> set of module names)
        modules_per_playbook = {}
        for playbook in task_summary['playbook'].dropna().unique():
            modules_in_playbook = set(task_summary[task_summary['playbook'] == playbook]['module_name'].dropna().unique())
            modules_per_playbook[playbook] = modules_in_playbook

        # Compute unique_hosts set from all host_ids
        host_sets = [s for s in task_summary['host_ids'].dropna() if isinstance(s, set)]
        unique_hosts = set().union(*host_sets) if host_sets else set()

        return {
            'collected_events_total': collected_events_total,
            'warnings_total': warnings_total,
            'deprecations_total': deprecations_total,
            'module_stats': module_stats,
            'collection_stats': collection_stats,
            'role_stats': role_stats,
            'unique_modules': unique_modules,
            'modules_per_playbook': modules_per_playbook,
            'unique_hosts': unique_hosts,
        }

    def base(self, data):
        """
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated
        *Total hosts automated

        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
        * Total job duration for collection sources (averages can be computed from totals and counts).
        * Number of hosts automated per job for each collection source (totals only).
        * Number of jobs per collection source that have failed.
        * Success/failure rate of jobs per collection source (number of jobs that have failed / number of jobs).
        * Number of jobs executed that use a specific partner collection - TODO - not implemented yet, must be communicated

        data is a dict with already-aggregated module_stats, collection_stats, role_stats from prepare() and merge()
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {'collected_events_total': 0, 'warnings_total': 0, 'deprecations_total': 0},
                'rollup': {'aggregated': pd.DataFrame(), 'collected_events_total': 0, 'warnings_total': 0, 'deprecations_total': 0},
            }

        # Extract data from the structure
        collected_events_total = data.get('collected_events_total', 0)
        warnings_total = data.get('warnings_total', 0)
        deprecations_total = data.get('deprecations_total', 0)
        module_stats = data.get('module_stats', pd.DataFrame())
        collection_stats = data.get('collection_stats', pd.DataFrame())
        role_stats = data.get('role_stats', pd.DataFrame())
        unique_modules = data.get('unique_modules', set())
        modules_per_playbook = data.get('modules_per_playbook', {})
        unique_hosts = data.get('unique_hosts', set())

        # Handle empty data
        if module_stats.empty and collection_stats.empty and role_stats.empty:
            return {
                'json': {
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                },
                'rollup': {
                    'aggregated': pd.DataFrame(),
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                },
            }

        # Convert controller_versions sets to sorted lists for JSON serialization
        for df in [module_stats, collection_stats, role_stats]:
            if not df.empty and 'controller_versions' in df.columns:
                df['controller_versions'] = df['controller_versions'].apply(
                    lambda x: sorted(list(x)) if isinstance(x, set) else (x if isinstance(x, list) else [])
                )

        # Compute modules_used_to_automate_total from unique_modules set
        modules_used_to_automate_total = len(unique_modules) if isinstance(unique_modules, set) else 0

        # Convert modules_per_playbook dict (sets) to counts for JSON
        modules_used_per_playbook_total = {
            playbook: len(module_set) if isinstance(module_set, set) else module_set
            for playbook, module_set in modules_per_playbook.items()
        }

        # Compute hosts_automated_total from unique_hosts set
        hosts_automated_total = len(unique_hosts) if isinstance(unique_hosts, set) else 0

        # Convert categorical columns back to strings before JSON serialization
        # This ensures JSON output contains strings, not categorical codes
        categorical_columns = [
            'module_name',
            'collection_name',
            'collection_source',
            'role',
        ]
        for df in [module_stats, collection_stats, role_stats]:
            if not df.empty:
                for col in categorical_columns:
                    if col in df.columns and df[col].dtype.name == 'category':
                        df[col] = df[col].astype(str)

        # Drop set columns before JSON serialization (they're not JSON serializable)
        # We only need the computed totals, not the raw sets
        for df in [module_stats, collection_stats, role_stats]:
            if not df.empty and 'host_ids' in df.columns:
                df.drop(columns=['host_ids'], inplace=True)

        # Prepare JSON data (converted to dicts/lists)
        json_data = {
            'modules_used_to_automate_total': modules_used_to_automate_total,
            'modules_used_per_playbook_total': modules_used_per_playbook_total,
            'module_stats': module_stats.to_dict(orient='records') if not module_stats.empty else [],
            'collection_stats': collection_stats.to_dict(orient='records') if not collection_stats.empty else [],
            'role_stats': role_stats.to_dict(orient='records') if not role_stats.empty else [],
            'hosts_automated_total': hosts_automated_total,
            'collected_events_total': collected_events_total,
            'warnings_total': warnings_total,
            'deprecations_total': deprecations_total,
        }

        return {
            'json': json_data,
        }
