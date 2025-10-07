import re

import pandas as pd

from metrics_utility.anonymized_rollups.collections_types import collections_types


_COLLECTION_RE = re.compile(r'^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$')


def extract_collection_name(x: str | None) -> str | None:
    if not x:
        return None
    m = _COLLECTION_RE.match(x)
    return f'{m.group(1)}.{m.group(2)}' if m else None


def merge_collection_source(obj1, obj2):
    merged = {}

    for entry in obj1 + obj2:
        key = entry['collection_source']
        merged.setdefault(key, {}).update(entry)

    # Convert dict back to list
    merged_list = list(merged.values())

    return merged_list


class EventModulesAnonymizedRollups:
    """
    Event collections rollups operate over main_jobevent_service collector data

    Important columns in data:
    module_name (task_action) - name of the module that was executed
    job_id - id of the job that was executed
    host_id - id of the host that was automated
    playbook - name of the playbook that was executed
    job_created - timestamp of the job creation
    job_started - timestamp of the job start
    job_finished - timestamp of the job finish
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

    @staticmethod
    def prepare_data(dataframe):
        # Prepare data

        # Coerce datetime-like columns to pandas datetimes (UTC) to accept strings like '...+00'
        for col in ['job_created', 'job_started', 'job_finished']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        # add module column into the dataframe based on dataframe_content_usage.py approach
        dataframe['module_name'] = (
            dataframe['resolved_action'].fillna(dataframe['task_action']).where(lambda s: s.notna() & (s.astype(str).str.strip() != ''))
        )

        dataframe = dataframe.assign(job_failed=dataframe['job_failed'].fillna(False).astype(bool))
        dataframe['collection_name'] = dataframe['module_name'].apply(extract_collection_name)

        dataframe['job_duration_seconds'] = (dataframe['job_finished'] - dataframe['job_started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['job_started'] - dataframe['job_created']).dt.total_seconds()

        dataframe = dataframe[dataframe['job_duration_seconds'] >= 0]
        dataframe = dataframe[dataframe['job_waiting_time_seconds'] >= 0]

        # fill collection source from collections_types
        dataframe['collection_source'] = dataframe['collection_name'].map(collections_types).fillna('Unknown')

        # Failure/Success rate of modules
        success_events_list = ['runner_on_ok', 'runner_on_async_ok', 'runner_item_on_ok']
        failed_events_list = ['runner_on_failed', 'runner_on_async_failed', 'runner_item_on_failed']
        unreachable_events_list = ['runner_on_unreachable', 'runner_item_on_unreachable']
        skipped_events_list = ['runner_on_skipped', 'runner_item_on_skipped']

        # Mark events
        dataframe['task_success_event'] = dataframe['event'].isin(success_events_list)

        # TODO - load it in collector
        def _ignore_errors_flag(d):
            return isinstance(d, dict) and d.get('ignore_errors', False)

        dataframe['task_failed_event'] = dataframe['event'].isin(failed_events_list) & ~dataframe['event_data'].apply(_ignore_errors_flag)
        dataframe['task_failed_and_ignored_event'] = dataframe['event'].isin(failed_events_list) & dataframe['event_data'].apply(_ignore_errors_flag)
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

        return dataframe

    @staticmethod
    def events_modules_aggregations(dataframe):
        """
        *Avg number of modules used in a playbook
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated

        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
        * Average job duration for collection sources (total job duration / number of jobs).
        * Average number of hosts automated per job for each collection source.
        * Number of jobs per collection source that have failed.
        * Success/failure rate of jobs per collection source (number of jobs that have failed / number of jobs).
        * Number of jobs executed that use a specific partner collection - TODO - not implemented yet, must be communicated


        dataframe corresponds to events joined with jobs
        """

        # Modules used to automate
        # distinct name of modules used to automate
        list_of_modules_used_to_automate = dataframe['module_name'].unique().tolist()

        # Total number of modules automated
        modules_used_to_automate_total = len(list_of_modules_used_to_automate)

        # Avg number of modules used in a playbook
        avg_number_of_modules_used_in_a_playbooks = dataframe.groupby('playbook')['module_name'].nunique().mean()
        modules_used_per_playbook_total = dataframe.groupby('playbook')['module_name'].nunique()

        # Collapse events  one row per (job, module, task)
        # summarize all failed events as number of failed attempts
        # if one success events is seen, task is successful
        # problem is that each task_uuid can have multiple ok and success events
        # when at least one success event is seen, task is successful
        # failed event can be repeated multiple times, we are counting failed attempts
        task_summary = (
            dataframe.groupby(['job_id', 'host_id', 'task_uuid', 'module_name', 'collection_source'])
            .agg(
                seen_success=('task_success_event', 'max'),
                seen_failed=('task_failed_event', 'max'),
                seen_unreachable=('task_unreachable_event', 'max'),
                seen_skipped=('task_skipped_event', 'max'),
                seen_failed_and_ignored=('task_failed_and_ignored_event', 'max'),
            )
            .reset_index()
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
            )
            .assign(job_id_that_contained_failed_task=lambda df: df['job_id'].where(df['task_failed']))
        )

        # Per-module counts
        # receiver of this data can easily calculate success rates
        module_stats = (
            task_summary.groupby(['module_name', 'collection_source'])
            .agg(
                jobs_total=('job_id', 'nunique'),
                hosts_total=('host_id', 'nunique'),
                task_clean_success_total=('task_clean_success', 'sum'),
                task_success_with_reruns_total=('task_success_with_reruns', 'sum'),
                task_failed_total=('task_failed', 'sum'),
                task_unreachable_total=('task_unreachable', 'sum'),
                task_skipped_total=('task_skipped', 'sum'),
                task_failed_and_ignored_total=('task_failed_and_ignored', 'sum'),
                jobs_failed_because_of_module_failure_total=('job_id_that_contained_failed_task', 'nunique'),
            )
            .reset_index()
        )

        collection_stats = (
            task_summary.groupby('collection_source')
            .agg(
                hosts_total=('host_id', 'nunique'),
                jobs_failed_because_of_collection_source_failure_total=('job_id_that_contained_failed_task', 'nunique'),
                task_clean_success_total=('task_clean_success', 'sum'),
                task_success_with_reruns_total=('task_success_with_reruns', 'sum'),
                task_failed_total=('task_failed', 'sum'),
                task_unreachable_total=('task_unreachable', 'sum'),
                task_skipped_total=('task_skipped', 'sum'),
                task_failed_and_ignored_total=('task_failed_and_ignored', 'sum'),
            )
            .reset_index()
        )

        # Collapse to one record per (job_id, collection_source)
        per_job = dataframe.groupby(['job_id', 'collection_source'], as_index=False).agg(
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            host_count=('host_id', 'nunique'),
            job_containing_collection_source_failed=('job_failed', 'max'),
        )

        job_time_stats = (
            per_job.groupby('collection_source')
            .agg(
                jobs_total=('job_id', 'nunique'),
                job_duration_total_seconds=('job_duration_seconds', 'sum'),
                job_waiting_time_total_seconds=('job_waiting_time_seconds', 'sum'),
                avg_hosts_per_job=('host_count', 'mean'),
                jobs_containing_collection_source_failed_total=('job_containing_collection_source_failed', 'sum'),
            )
            .assign(
                avg_job_duration_seconds=lambda x: x['job_duration_total_seconds'] / x['jobs_total'],
                avg_job_waiting_time_seconds=lambda x: x['job_waiting_time_total_seconds'] / x['jobs_total'],
            )
            .reset_index()
        )

        # merge collection_stats and job_time_stats into one list based on collection_source
        collection_stats = collection_stats.to_dict(orient='records')
        job_time_stats = job_time_stats.to_dict(orient='records')

        merged_list = merge_collection_source(collection_stats, job_time_stats)

        return {
            'list_of_modules_used_to_automate': list_of_modules_used_to_automate,
            'modules_used_to_automate_total': modules_used_to_automate_total,
            'avg_number_of_modules_used_in_a_playbooks': avg_number_of_modules_used_in_a_playbooks,
            'modules_used_per_playbook_total': modules_used_per_playbook_total.to_dict(),
            'module_stats': module_stats.to_dict(orient='records'),
            'collection_stats': merged_list,
        }
