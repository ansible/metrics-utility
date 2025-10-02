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

        dataframe['job_duration'] = (dataframe['job_finished'] - dataframe['job_started']).dt.total_seconds()
        dataframe['job_waiting_time'] = (dataframe['job_started'] - dataframe['job_created']).dt.total_seconds()


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
            & (dataframe['host_id'].str.strip() != '')
            & (dataframe['playbook'].str.strip() != '')
            & (dataframe['job_id'].str.strip() != '')
        ]

        return dataframe

   

    @staticmethod
    def events_modules_aggregations(dataframe):
        """
        *Avg number of modules used in a playbook
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated

        dataframe corresponds to events
        """

        # ?Number of jobs executed that use a specific partner collection.

        # Modules used to automate
        # distinct name of modules used to automate
        list_of_modules_used_to_automate = dataframe['module_name'].unique().tolist()

        # Total number of modules automated
        total_modules_used_to_automate = len(list_of_modules_used_to_automate)

        # Avg number of modules used in a playbook
        avg_number_of_modules_used_in_a_playbooks = dataframe.groupby('playbook')['module_name'].nunique().mean()

        total_modules_used_per_playbook = dataframe.groupby('playbook')['module_name'].nunique()


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
                total_failed_attempts=('task_failed_event', 'sum'),  # number of failed attempts due to retries
            )
            .reset_index()
            .assign(
                task_failed=lambda x: (~x['task_success']) & (x['retry_attempts'] > 0),     
            )
        )

        # Per-module counts
        # receiver of this data can easily calculate success rates
        module_stats = (
            task_summary.groupby('module_name')
            .agg(
                jobs_total=('job_id', 'nunique'),
                hosts_total=('host_id', 'nunique'),
                tasks_unique_runs_total=('task_uuid', 'nunique'),
                runs_success_total=('task_success', 'sum'),
                runs_failed_total=('task_failed', 'sum'),
                total_failed_attempts=('total_failed_attempts', 'sum'),
            )
            .reset_index()
            .assign(
                # success rate = success_rate / (success_rate + failed_rate)
                success_rate=lambda x: x['runs_success_total'].div(x['runs_success_total'] + x['runs_failed_total']),
            )
        )

        return {
            'list_of_modules_used_to_automate': list_of_modules_used_to_automate,
            'total_modules_used_to_automate': total_modules_used_to_automate,
            'avg_number_of_modules_used_in_a_playbooks': avg_number_of_modules_used_in_a_playbooks,
            'module_stats': module_stats.to_dict(orient='records'),
            'total_modules_used_per_playbook': total_modules_used_per_playbook.to_dict(),
        }

     @staticmethod
    def event_collections_aggregations(dataframe):
        """
        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
          *Average job duration for collection sources
          *Average number of hosts automated per job for each collection source.
          *Number of jobs per collection source that have failed.
          *Success/failure rate of jobs per collection source.
          ?Number of jobs executed that use a specific partner collection.
        """

        result = (
            dataframe.groupby('collection_source')
            .agg(
                jobs_total=('job_id', 'nunique'),
                hosts_total=('host_id', 'nunique'),
                job_duration_total=('job_duration', 'sum'),
                job_waiting_time_total=('job_waiting_time', 'sum'),
                jobs_failed_total=('job_failed', 'sum'),
            )
            .assign(
                avg_job_duration=lambda x: x['total_job_duration'].div(x['total_jobs']),
                avg_job_waiting_time=lambda x: x['total_job_waiting_time'].div(x['total_jobs']),
                success_rate=lambda x: 1 - x['total_jobs_failed'].div(x['total_jobs']),
                avg_hosts_per_job=lambda x: x['total_hosts'].div(x['total_jobs']),
            )
            .reset_index()   # <-- bring collection_source back as a column
            .to_dict(orient='records')
        )

        # make sure everything is converted to python records
        return result

    @staticmethod
    def base(dataframe):
        dataframe = Event_Anonymized_Rollups.prepare_data(dataframe)

        event_collections_aggregations = Event_Anonymized_Rollups.event_collections_aggregations(dataframe)
        events_modules_aggregations = Event_Anonymized_Rollups.events_modules_aggregations(dataframe)

        return {
            'event_collections_aggregations': event_collections_aggregations,
            'events_modules_aggregations': events_modules_aggregations,
        }
