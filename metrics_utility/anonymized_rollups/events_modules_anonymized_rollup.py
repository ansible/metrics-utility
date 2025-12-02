import json
import os
import re

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


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
        Override merge to handle the new structure with event_total and task_summary.
        Concatenates task_summary dataframes and sums event_total.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Concatenate task_summary dataframes and sum event_totals
        return {
            'event_total': data_all['event_total'] + data_new['event_total'],
            'task_summary': pd.concat([data_all['task_summary'], data_new['task_summary']], ignore_index=True),
        }

    # Prepare is run for each batch of data
    # then it is merged with other batches into one dataframes
    # as default, merging is done by concatenating dataframes (defined in base class)
    def prepare(self, dataframe):
        # Count all events before pruning
        event_total = len(dataframe)

        # Failure/Success rate of modules
        success_events_list = ['runner_on_ok', 'runner_on_async_ok', 'runner_item_on_ok']
        failed_events_list = ['runner_on_failed', 'runner_on_async_failed', 'runner_item_on_failed']
        unreachable_events_list = ['runner_on_unreachable', 'runner_item_on_unreachable']
        skipped_events_list = ['runner_on_skipped', 'runner_item_on_skipped']

        # Filter for only the event types that are used in analysis
        all_relevant_events = success_events_list + failed_events_list + unreachable_events_list + skipped_events_list
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

        dataframe = dataframe[
            dataframe['module_name'].notna()
            & dataframe['host_id'].notna()
            & dataframe['playbook'].notna()
            & dataframe['job_id'].notna()
            & (dataframe['module_name'].str.strip() != '')
            & (dataframe['playbook'].str.strip() != '')
        ]

        # Select only the columns needed for analysis to save memory
        columns_to_keep = [
            'job_id',
            'host_id',
            'task_uuid',
            'module_name',
            'playbook',
            'collection_name',
            'collection_source',
            'job_failed',
            'job_started',
            'job_duration_seconds',
            'job_waiting_time_seconds',
            'task_success_event',
            'task_failed_event',
            'task_failed_and_ignored_event',
            'task_unreachable_event',
            'task_skipped_event',
        ]

        dataframe = dataframe[columns_to_keep]

        # Aggregate events per task to reduce rows before merging batches
        # This groups by (job, host, task, module, collection) and summarizes all events
        # This can greatly reduce the number of rows, depends of number of retries
        task_summary = dataframe.groupby(
            ['job_id', 'host_id', 'task_uuid', 'module_name', 'collection_source', 'collection_name'], as_index=False, observed=True
        ).agg(
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
        )

        return {
            'event_total': event_total,
            'task_summary': task_summary,
        }

    def base(self, data):
        """
        *Avg number of modules used in a playbook
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated
        *Total hosts automated

        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
        * Average job duration for collection sources (total job duration / number of jobs).
        * Average number of hosts automated per job for each collection source.
        * Number of jobs per collection source that have failed.
        * Success/failure rate of jobs per collection source (number of jobs that have failed / number of jobs).
        * Number of jobs executed that use a specific partner collection - TODO - not implemented yet, must be communicated


        data is a dict with 'event_total' and 'task_summary' dataframe
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {'event_total': 0},
                'rollup': {'aggregated': pd.DataFrame(), 'event_total': 0},
            }

        # Extract event_total and task_summary dataframe from the data structure
        event_total = data.get('event_total', 0)
        dataframe = data.get('task_summary', pd.DataFrame())

        # TODO - ensure all columns are present in the dataframe, then let analysis run with empty data
        if dataframe.empty:
            return {
                'json': {'event_total': event_total},
                'rollup': {'aggregated': dataframe, 'event_total': event_total},
            }

        # Categorize columns to reduce memory footprint
        # This is done after batches are concatenated to ensure consistent categories
        # Only categorize string columns with low-to-medium cardinality (high repetition)
        categorical_columns = [
            'collection_source',  # ~20 unique values (Red Hat, Community, Partner, etc.)
            'collection_name',  # ~500-1000 unique collections
            'module_name',  # ~2000-5000 unique modules
            'playbook',  # ~1000-5000 unique playbooks
            'task_uuid',  # ~1000-5000 unique task uuids
        ]

        for col in categorical_columns:
            if col in dataframe.columns:
                dataframe[col] = dataframe[col].astype('category')

        # Final aggregation: handle any cross-batch duplicates
        # because the task running on host can be split between batches
        # we need to aggregate the data second time to get the correct results
        # since this will be rare, reduction of rows will be still significant
        dataframe = dataframe.groupby(
            ['job_id', 'host_id', 'task_uuid', 'module_name', 'collection_source', 'collection_name'], as_index=False, observed=True
        ).agg(
            seen_success=('seen_success', 'max'),
            seen_failed=('seen_failed', 'max'),
            seen_unreachable=('seen_unreachable', 'max'),
            seen_skipped=('seen_skipped', 'max'),
            seen_failed_and_ignored=('seen_failed_and_ignored', 'max'),
            job_started=('job_started', 'first'),
            job_failed=('job_failed', 'first'),
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            playbook=('playbook', 'first'),
        )

        # Modules used to automate
        # distinct name of modules used to automate

        # pick unique module name and associated collection source
        list_of_modules_used_to_automate = dataframe.groupby('module_name', as_index=False, observed=True).agg(
            {'collection_source': lambda x: x.unique()[0], 'collection_name': lambda x: x.unique()[0]}
        )

        # Total number of modules automated
        modules_used_to_automate_total = len(list_of_modules_used_to_automate)

        # Avg number of modules used in a playbook
        avg_number_of_modules_used_in_a_playbooks = dataframe.groupby('playbook', observed=True)['module_name'].nunique().mean()
        modules_used_per_playbook_total = dataframe.groupby('playbook', observed=True)['module_name'].nunique()

        hosts_automated_total = dataframe['host_id'].nunique()

        # Data is already aggregated from prepare() and merge()
        # We just need to compute the mutually exclusive task status categories
        task_summary = dataframe.assign(
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

        # Per-module counts
        # receiver of this data can easily calculate success rates
        module_stats = task_summary.groupby(['module_name', 'collection_source', 'collection_name'], as_index=False, observed=True).agg(
            jobs_total=('job_id', 'nunique'),
            number_of_jobs_never_started=('job_started', lambda x: x.isna().sum()),
            hosts_total=('host_id', 'nunique'),
            task_clean_success_total=('task_clean_success', 'sum'),
            task_success_with_reruns_total=('task_success_with_reruns', 'sum'),
            task_failed_total=('task_failed', 'sum'),
            task_unreachable_total=('task_unreachable', 'sum'),
            task_skipped_total=('task_skipped', 'sum'),
            task_failed_and_ignored_total=('task_failed_and_ignored', 'sum'),
            jobs_failed_because_of_module_failure_total=('job_id_that_contained_failed_task', 'nunique'),
        )

        collection_name_stats = task_summary.groupby(['collection_name', 'collection_source'], as_index=False, observed=True).agg(
            jobs_total=('job_id', 'nunique'),
            number_of_jobs_never_started=('job_started', lambda x: x.isna().sum()),
            hosts_total=('host_id', 'nunique'),
            jobs_failed_because_of_collection_name_failure_total=('job_id_that_contained_failed_task', 'nunique'),
            task_clean_success_total=('task_clean_success', 'sum'),
            task_success_with_reruns_total=('task_success_with_reruns', 'sum'),
            task_failed_total=('task_failed', 'sum'),
            task_unreachable_total=('task_unreachable', 'sum'),
            task_skipped_total=('task_skipped', 'sum'),
            task_failed_and_ignored_total=('task_failed_and_ignored', 'sum'),
        )

        # Per-job statistics for modules (similar to collection_name)
        per_job_module = dataframe.groupby(['job_id', 'module_name', 'collection_name', 'collection_source'], as_index=False, observed=True).agg(
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            host_count=('host_id', 'nunique'),
            job_containing_module_failed=('job_failed', 'max'),
        )

        job_time_stats_module = (
            per_job_module.groupby(['module_name', 'collection_source', 'collection_name'], as_index=False, observed=True)
            .agg(
                jobs_total=('job_id', 'nunique'),
                job_duration_total_seconds=('job_duration_seconds', 'sum'),
                job_waiting_time_total_seconds=('job_waiting_time_seconds', 'sum'),
                avg_hosts_per_job=('host_count', 'mean'),
                jobs_containing_module_failed_total=('job_containing_module_failed', 'sum'),
            )
            .assign(
                avg_job_duration_seconds=lambda x: x['job_duration_total_seconds'] / x['jobs_total'],
                avg_job_waiting_time_seconds=lambda x: x['job_waiting_time_total_seconds'] / x['jobs_total'],
            )
        )

        per_job_collection_name = dataframe.groupby(['job_id', 'collection_name', 'collection_source'], as_index=False, observed=True).agg(
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            host_count=('host_id', 'nunique'),
            job_containing_collection_name_failed=('job_failed', 'max'),
        )

        job_time_stats_collection_name = (
            per_job_collection_name.groupby(['collection_name', 'collection_source'], as_index=False, observed=True)
            .agg(
                jobs_total=('job_id', 'nunique'),
                job_duration_total_seconds=('job_duration_seconds', 'sum'),
                job_waiting_time_total_seconds=('job_waiting_time_seconds', 'sum'),
                avg_hosts_per_job=('host_count', 'mean'),
                jobs_containing_collection_name_failed_total=('job_containing_collection_name_failed', 'sum'),
            )
            .assign(
                avg_job_duration_seconds=lambda x: x['job_duration_total_seconds'] / x['jobs_total'],
                avg_job_waiting_time_seconds=lambda x: x['job_waiting_time_total_seconds'] / x['jobs_total'],
            )
        )

        # merge module_stats and job_time_stats_module into one list based on module_name
        module_stats_dict = module_stats.to_dict(orient='records')
        job_time_stats_module_dict = job_time_stats_module.to_dict(orient='records')
        merged_list_module = merge_by_name(module_stats_dict, job_time_stats_module_dict, 'module_name')

        # merge collection_stats and job_time_stats into one list based on collection_source
        collection_name_stats_dict = collection_name_stats.to_dict(orient='records')
        job_time_stats_collection_name_dict = job_time_stats_collection_name.to_dict(orient='records')
        merged_list_collection_name = merge_by_name(collection_name_stats_dict, job_time_stats_collection_name_dict, 'collection_name')

        # Prepare rollup data (dataframes before conversion)
        rollup_data = {
            'module_stats': module_stats,
            'hosts_automated_total': {'hosts_automated_total': hosts_automated_total},
            'event_total': event_total,
        }

        # Prepare JSON data (converted to dicts/lists)
        json_data = {
            'modules_used_to_automate_total': modules_used_to_automate_total,
            'avg_number_of_modules_used_in_a_playbooks': avg_number_of_modules_used_in_a_playbooks,
            'modules_used_per_playbook_total': modules_used_per_playbook_total.to_dict(),
            'module_stats': merged_list_module,
            'collection_name_stats': merged_list_collection_name,
            'hosts_automated_total': hosts_automated_total,
            'event_total': event_total,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
