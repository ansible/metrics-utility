import re

from metrics_utility.anonymized_rollups.collections_types import collections_types


def collection_regexp():
    return r'^(\w+)\.(\w+)\.((\w+)(\.|$))+'


def extract_collection_name(x):
    if x is None:
        return None

    m = re.match(collection_regexp(), x)

    if m:
        return f'{m.groups()[0]}.{m.groups()[1]}'
    else:
        return None


class Event_Anonymized_Rollups:
    """
    Event rollups operate over main_jobevent_service collector data

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

        # add module column into the dataframe based on dataframe_content_usage.py approach
        dataframe['task_action'] = dataframe.resolved_action.fillna(dataframe.task_action).astype(str)
        dataframe.rename(columns={'task_action': 'module_name'}, inplace=True)

        dataframe = dataframe.assign(job_failed=dataframe['job_failed'].fillna(False).astype(bool))
        dataframe['collection_name'] = dataframe['module_name'].apply(extract_collection_name)

        dataframe['job_duration_seconds'] = (dataframe['job_finished'] - dataframe['job_started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['job_started'] - dataframe['job_created']).dt.total_seconds()

        # fill collection source from collections_types
        dataframe['collection_source'] = dataframe['collection_name'].map(collections_types)

        # Failure/Success rate of modules

        success_events_list = ['runner_on_ok', 'runner_on_async_ok']
        failed_events_list = ['runner_on_failed', 'runner_on_async_failed', 'runner_on_unreachable']

        # Mark events
        dataframe['task_success_event'] = dataframe['event'].isin(success_events_list)
        dataframe['task_failed_event'] = dataframe['event'].isin(failed_events_list)

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
    def event_collections_aggregations(dataframe):
        """
        Aggregates job-level metrics by collection source:

          * Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
          * Average job duration for collection sources.
          * Average number of hosts automated per job for each collection source.
          * Number of jobs per collection source that have failed.
          * Success/failure rate of jobs per collection source.
          * Number of jobs executed that use a specific partner collection.

        dataframe corresponds to events joined with jobs, also collection_source is added - validated, rh-certified, community, etc.
        dataframe contains job durations and waiting times unique per job_id
        """

        # Collapse to one record per (job_id, collection_source)
        per_job = dataframe.groupby(['job_id', 'collection_source'], as_index=False).agg(
            job_duration_seconds=('job_duration_seconds', 'first'),
            job_waiting_time_seconds=('job_waiting_time_seconds', 'first'),
            job_failed=('job_failed', 'first'),
            host_count=('host_id', 'nunique'),
        )

        # Aggregate at collection_source level
        result = (
            per_job.groupby('collection_source')
            .agg(
                jobs_total=('job_id', 'nunique'),
                job_duration_total_seconds=('job_duration_seconds', 'sum'),
                job_waiting_time_total_seconds=('job_waiting_time_seconds', 'sum'),
                jobs_failed_total=('job_failed', 'sum'),
                avg_hosts_per_job=('host_count', 'mean'),
            )
            .assign(
                avg_job_duration_seconds=lambda x: x['job_duration_total_seconds'] / x['jobs_total'],
                avg_job_waiting_time_seconds=lambda x: x['job_waiting_time_total_seconds'] / x['jobs_total'],
                success_rate=lambda x: 1 - (x['jobs_failed_total'] / x['jobs_total']),
            )
            .reset_index()
            .to_dict(orient='records')
        )

        return result

    @staticmethod
    def events_modules_aggregations(dataframe):
        """
        *Avg number of modules used in a playbook
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated

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
            dataframe.groupby(['job_id', 'module_name', 'task_uuid', 'host_id'])
            .agg(
                task_success=('task_success_event', 'max'),  # any success seen?
                failed_attempts_total=('task_failed_event', 'sum'),  # number of failed attempts due to retries
            )
            .reset_index()
            .assign(
                task_failed=lambda x: (~x['task_success']) & (x['failed_attempts_total'] > 0),
                task_success_with_failed_attempts=lambda x: x['task_success'] & (x['failed_attempts_total'] > 0),
                task_success_without_failed_attempts=lambda x: x['task_success'] & (x['failed_attempts_total'] == 0),
                # task other - neither success or failure
                task_other=lambda x: (~x['task_success']) & (x['failed_attempts_total'] == 0),
            )
        )

        # Per-module counts
        # receiver of this data can easily calculate success rates
        module_stats = (
            task_summary.groupby('module_name')
            .agg(
                jobs_total=('job_id', 'nunique'),
                hosts_total=('host_id', 'nunique'),
                tasks_success_total=('task_success', 'sum'),
                tasks_success_with_failed_attempts_total=('task_success_with_failed_attempts', 'sum'),
                tasks_success_without_failed_attempts_total=('task_success_without_failed_attempts', 'sum'),
                tasks_failed_total=('task_failed', 'sum'),
                failed_attempts_total=('failed_attempts_total', 'sum'),
                tasks_other_total=('task_other', 'sum'),
            )
            .reset_index()
            .assign(
                total_success_and_failure=lambda x: x['tasks_success_total'] + x['tasks_failed_total'],
                success_rate=lambda x: x['tasks_success_total'].div(x['tasks_success_total'] + x['tasks_failed_total']),
                success_rate_with_failed_attempts=lambda x: x['tasks_success_with_failed_attempts_total'].div(
                    x['tasks_success_total'] + x['tasks_failed_total']
                ),
                success_rate_without_failed_attempts=lambda x: x['tasks_success_without_failed_attempts_total'].div(
                    x['tasks_success_total'] + x['tasks_failed_total']
                ),
            )
        )

        return {
            'list_of_modules_used_to_automate': list_of_modules_used_to_automate,
            'modules_used_to_automate_total': modules_used_to_automate_total,
            'avg_number_of_modules_used_in_a_playbooks': avg_number_of_modules_used_in_a_playbooks,
            'module_stats': module_stats.to_dict(orient='records'),
            'modules_used_per_playbook_total': modules_used_per_playbook_total.to_dict(),
        }

    @staticmethod
    def base(dataframe):
        dataframe = Event_Anonymized_Rollups.prepare_data(dataframe)

        event_collections_aggregations = Event_Anonymized_Rollups.event_collections_aggregations(dataframe)
        events_modules_aggregations = Event_Anonymized_Rollups.events_modules_aggregations(dataframe)

        return {
            'event_collections_aggregations': event_collections_aggregations,
            'events_modules_aggregations': events_modules_aggregations,
        }
