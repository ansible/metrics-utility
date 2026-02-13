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
        Override merge to handle the new structure with collected_events_total, warnings_total, deprecations_total and task_summary.
        Concatenates task_summary dataframes and sums event totals.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Concatenate task_summary dataframes and sum event totals
        return {
            'collected_events_total': data_all['collected_events_total'] + data_new['collected_events_total'],
            'warnings_total': data_all.get('warnings_total', 0) + data_new.get('warnings_total', 0),
            'deprecations_total': data_all.get('deprecations_total', 0) + data_new.get('deprecations_total', 0),
            'task_summary': pd.concat([data_all['task_summary'], data_new['task_summary']], ignore_index=True),
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
        # duplicated entries will be summed in base function
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
            # Preserve columns needed in base() function
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
            role=('role', 'first'),  # Keep role for later aggregation
        )

        return {
            'collected_events_total': collected_events_total,
            'warnings_total': warnings_total,
            'deprecations_total': deprecations_total,
            'task_summary': task_summary,
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


        data is a dict with 'collected_events_total' and 'task_summary' dataframe
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {'collected_events_total': 0, 'warnings_total': 0, 'deprecations_total': 0},
                'rollup': {'aggregated': pd.DataFrame(), 'collected_events_total': 0, 'warnings_total': 0, 'deprecations_total': 0},
            }

        # Extract event totals and task_summary dataframe from the data structure
        collected_events_total = data.get('collected_events_total', 0)
        warnings_total = data.get('warnings_total', 0)
        deprecations_total = data.get('deprecations_total', 0)
        dataframe = data.get('task_summary', pd.DataFrame())

        # TODO - ensure all columns are present in the dataframe, then let analysis run with empty data
        if dataframe.empty:
            return {
                'json': {
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                },
                'rollup': {
                    'aggregated': dataframe,
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                },
            }

        # Final aggregation: handle any cross-batch duplicates, sum them, loss of precision is acceptable
        # Note: role is kept as a column but not used for grouping here
        dataframe = dataframe.groupby(
            ['job_id', 'task_uuid', 'module_name', 'collection_source', 'collection_name'], as_index=False, observed=True
        ).agg(
            task_clean_success=('task_clean_success', 'sum'),
            task_success_with_reruns=('task_success_with_reruns', 'sum'),
            task_failed=('task_failed', 'sum'),
            task_failed_and_ignored=('task_failed_and_ignored', 'sum'),
            task_unreachable=('task_unreachable', 'sum'),
            task_skipped=('task_skipped', 'sum'),
            job_id_that_contained_failed_task=('job_id_that_contained_failed_task', 'first'),
            # Preserve columns needed for later aggregations
            playbook=('playbook', 'first'),
            job_started=('job_started', 'first'),
            job_failed=('job_failed', 'first'),
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            host_ids=('host_ids', lambda x: set().union(*[s for s in x.dropna() if isinstance(s, set)])),
            warnings_total=('warnings_total', 'sum'),
            deprecations_total=('deprecations_total', 'sum'),
            processed_events_total=('processed_events_total', 'sum'),
            controller_version=('controller_version', 'first'),
            role=('role', 'first'),  # Keep role for role_stats aggregation
        )

        # Convert string columns to categorical for memory efficiency
        # Ensure 'Unknown' is included in collection_source categories
        string_columns = ['module_name', 'collection_name', 'collection_source', 'playbook', 'role']
        for col in string_columns:
            if col in dataframe.columns:
                if col == 'collection_source':
                    # Ensure 'Unknown' is in categories by explicitly including it
                    unique_values = list(dataframe[col].dropna().unique())
                    if 'Unknown' not in unique_values:
                        unique_values.append('Unknown')
                    dataframe[col] = pd.Categorical(dataframe[col], categories=unique_values)
                else:
                    dataframe[col] = dataframe[col].astype('category')

        # Modules used to automate
        # distinct name of modules used to automate

        # pick unique module name and associated collection source
        list_of_modules_used_to_automate = dataframe.groupby('module_name', as_index=False, observed=True).agg(
            {'collection_source': lambda x: x.unique()[0], 'collection_name': lambda x: x.unique()[0]}
        )

        # Total number of modules automated
        modules_used_to_automate_total = len(list_of_modules_used_to_automate)

        # Modules used per playbook (totals only, averages can be computed from totals)
        modules_used_per_playbook_total = dataframe.groupby('playbook', observed=True)['module_name'].nunique()

        # Data is already aggregated from prepare() and merge()
        # Task status categories are already computed in prepare(), so we can use dataframe directly
        task_summary = dataframe

        # Compute duration columns before aggregation
        task_summary = task_summary.assign(
            jobs_successful_duration_total_seconds=lambda x: x['job_duration_seconds'].where(~x['job_failed'], 0),
            jobs_failed_duration_total_seconds=lambda x: x['job_duration_seconds'].where(x['job_failed'], 0),
        )

        # common aggregation for module_stats and collection_stats
        common_aggregation = {
            'jobs_total': ('job_id', 'nunique'),
            'jobs_successful_total': ('job_failed', lambda x: (~x).sum()),
            'jobs_failed_total': ('job_failed', 'sum'),
            'jobs_duration_total_seconds': ('job_duration_seconds', 'sum'),
            'jobs_waiting_time_total_seconds': ('job_waiting_time_seconds', 'sum'),
            'jobs_never_started_total': ('job_started', lambda x: x.isna().sum()),
            'unique_hosts_total': ('host_ids', lambda x: len(set().union(*[s for s in x.dropna() if isinstance(s, set)]))),
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
            # this should be list of controller versions (sorted for consistency)
            'controller_versions': ('controller_version', lambda x: sorted(set(x.dropna()))),
        }

        # Per-module counts
        # receiver of this data can easily calculate success rates
        module_stats = task_summary.groupby(['module_name', 'collection_source', 'collection_name'], as_index=False, observed=True).agg(
            **common_aggregation
        )
        # Compute tasks_total as sum of all task status totals
        module_stats['tasks_total'] = (
            module_stats['task_ok_total']
            + module_stats['task_ok_with_retries_total']
            + module_stats['task_failed_total']
            + module_stats['task_unreachable_total']
            + module_stats['task_skipped_total']
            + module_stats['task_failed_and_ignored_total']
        )

        collection_stats = task_summary.groupby(['collection_name', 'collection_source'], as_index=False, observed=True).agg(**common_aggregation)
        # Compute tasks_total as sum of all task status totals
        collection_stats['tasks_total'] = (
            collection_stats['task_ok_total']
            + collection_stats['task_ok_with_retries_total']
            + collection_stats['task_failed_total']
            + collection_stats['task_unreachable_total']
            + collection_stats['task_skipped_total']
            + collection_stats['task_failed_and_ignored_total']
        )

        # Extract collection_name from role for collection-based roles (namespace.collection.role)
        # For standalone roles (namespace.role), collection_name will be None
        # Note: task_summary already has collection_name/collection_source from the MODULE,
        # but for role_stats we need the ROLE's collection (which may differ from the module's collection)
        # Convert role to string first to avoid categorical issues
        task_summary['role_collection_name'] = (
            task_summary['role'].astype(str).apply(lambda x: extract_collection_name(x) if x and x != 'nan' else None)
        )
        # Map role collection_name to collection_source - convert to string first, then fillna, then convert to categorical
        role_collection_source_str = task_summary['role_collection_name'].astype(str).map(self.collections)
        task_summary['role_collection_source'] = role_collection_source_str.fillna('Unknown')

        # Convert role collection columns to categorical for memory efficiency
        # Ensure 'Unknown' is included in role_collection_source categories
        if 'role_collection_name' in task_summary.columns:
            task_summary['role_collection_name'] = task_summary['role_collection_name'].astype('category')
        if 'role_collection_source' in task_summary.columns:
            # Ensure 'Unknown' is in categories by explicitly including it
            unique_values = list(task_summary['role_collection_source'].dropna().unique())
            if 'Unknown' not in unique_values:
                unique_values.append('Unknown')
            task_summary['role_collection_source'] = pd.Categorical(task_summary['role_collection_source'], categories=unique_values)

        role_stats = task_summary.groupby(['role', 'role_collection_name', 'role_collection_source'], as_index=False, observed=True).agg(
            **common_aggregation
        )
        # Rename columns to match expected output format
        role_stats = role_stats.rename(columns={'role_collection_name': 'collection_name', 'role_collection_source': 'collection_source'})
        # Compute tasks_total as sum of all task status totals
        role_stats['tasks_total'] = (
            role_stats['task_ok_total']
            + role_stats['task_ok_with_retries_total']
            + role_stats['task_failed_total']
            + role_stats['task_unreachable_total']
            + role_stats['task_skipped_total']
            + role_stats['task_failed_and_ignored_total']
        )

        # Get hosts_automated_total from the dataframe by unioning all host_ids sets
        if not dataframe.empty and 'host_ids' in dataframe.columns:
            host_sets = [s for s in dataframe['host_ids'].dropna() if isinstance(s, set)]
            all_hosts = set().union(*host_sets) if host_sets else set()
            hosts_automated_total = len(all_hosts)
        else:
            hosts_automated_total = 0

        # Convert categorical columns back to strings before JSON serialization
        # This ensures JSON output contains strings, not categorical codes
        categorical_columns = [
            'module_name',
            'collection_name',
            'collection_source',
            'playbook',
            'role',
            'role_collection_name',
            'role_collection_source',
        ]
        for df in [list_of_modules_used_to_automate, module_stats, collection_stats, role_stats]:
            for col in categorical_columns:
                if col in df.columns and df[col].dtype.name == 'category':
                    df[col] = df[col].astype(str)

        # Convert playbook index to string if it's categorical
        if modules_used_per_playbook_total.index.dtype.name == 'category':
            modules_used_per_playbook_total.index = modules_used_per_playbook_total.index.astype(str)

        # Prepare JSON data (converted to dicts/lists)
        json_data = {
            'modules_used_to_automate_total': modules_used_to_automate_total,
            'modules_used_per_playbook_total': modules_used_per_playbook_total.to_dict(),
            'module_stats': module_stats.to_dict(orient='records'),
            'collection_stats': collection_stats.to_dict(orient='records'),
            'role_stats': role_stats.to_dict(orient='records'),
            'hosts_automated_total': hosts_automated_total,
            'collected_events_total': collected_events_total,
            'warnings_total': warnings_total,
            'deprecations_total': deprecations_total,
        }

        return {
            'json': json_data,
        }
