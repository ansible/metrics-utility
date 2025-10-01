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
    """

    @staticmethod
    def prepare_data(dataframe):
        # Prepare data

        # add module column into the dataframe based on dataframe_content_usage.py approach
        dataframe['task_action'] = dataframe.resolved_action.fillna(dataframe.task_action).astype(str)
        dataframe.rename(columns={'task_action': 'module_name'}, inplace=True)

        dataframe = dataframe.assign(job_failed=dataframe['job_failed'].fillna(False).astype(bool))
        dataframe['collection_name'] = dataframe['module_name'].apply(extract_collection_name)

        dataframe['job_duration'] = dataframe['job_finished'] - dataframe['job_started']
        dataframe['job_waiting_time'] = dataframe['job_started'] - dataframe['job_created']

        # fill collection source from collections_types
        dataframe['collection_source'] = dataframe['collection_name'].map(collections_types)

        for col in ['task_success_event', 'task_failed_event']:
            if col not in dataframe.columns:
                dataframe[col] = False  # create with default False
            dataframe[col] = dataframe[col].fillna(False).astype(bool)

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
    def event_collections_aggregations(dataframe):
        """
        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
          *Average job duration for collection sources
          *Average number of hosts automated per job for each collection source.
          *Number of jobs per collection source that have failed.
          *Success/failure rate of jobs per collection source.
        """

        result = dataframe.groupby('collection_source').agg(
            total_jobs=('job_id', 'nunique'),
            total_hosts=('host_id', 'nunique'),
            total_job_duration=('job_duration', 'sum'),
            total_job_waiting_time=('job_waiting_time', 'sum'),
            total_jobs_failed=('job_failed', 'sum'),
        )
        .assign(
            avg_job_duration=lambda x: x['total_job_duration'] / x['total_jobs'],
            avg_job_waiting_time=lambda x: x['total_job_waiting_time'] / x['total_jobs'],
            success_rate=lambda x: x['total_jobs_failed'] / x['total_jobs'],
        )

        # transform result to dict
        result = result.to_dict(orient='records')

        # make sure everything is converted to python records
        return result

    @staticmethod
    def events_modules_aggregations(dataframe):
        """
        ?Number of jobs executed that use a specific partner collection.
        *Avg number of modules used in a playbook
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated
        """

        # ?Number of jobs executed that use a specific partner collection.

        # Modules used to automate
        # distinct name of modules used to automate
        list_of_modules_used_to_automate = dataframe['module_name'].unique().tolist()

        # Total number of modules automated
        total_modules_used_to_automate = len(list_of_modules_used_to_automate)

        # Avg number of modules used in a playbook
        avg_number_of_modules_used_in_a_playbooks = dataframe.groupby('playbook')['module_name'].nunique().mean()
        total_modules_used_per_playbook = dataframe.groupby('playbook')['module_name'].nunique().sum()

        # Failure/Success rate of modules

        success_events_list = ['runner_on_ok', 'runner_on_async_ok']
        failed_events_list = ['runner_on_failed', 'runner_on_async_failed', 'runner_on_unreachable']

        # Mark events
        dataframe['task_success_event'] = dataframe['event'].isin(success_events_list)
        dataframe['task_failed_event'] = dataframe['event'].isin(failed_events_list)

        # Collapse events  one row per (job, module, task)
        # summarize all failed events as number of failed attempts
        # if one success events is seen, task is successful
        task_summary = (
            dataframe.groupby(['job_id', 'module_name', 'task_uuid', 'host_id'])
            .agg(
                task_success=('task_success_event', 'max'),  # any success seen?
                failed_attempts=('task_failed_event', 'sum'),  # number of fails
            )
            .reset_index()
            .assign(
                task_failed=lambda x: (~x['task_success']) & (x['failed_attempts'] > 0),
                task_other=lambda x: (~x['task_success']) & (~x['task_failed']),  # skipped, ignored, etc.
            )
        )

        # Per-module counts
        # receiver of this data can easily calculate success rates
        module_stats = (
            task_summary.groupby('module_name')
            .agg(
                jobs_total=('job_id', 'nunique'),
                hosts_total=('host_id', 'nunique'),
                tasks_unique_runs=('task_uuid', 'nunique'),
                runs_success=('task_success', 'sum'),
                runs_failed=('task_failed', 'sum'),
                runs_other=('task_other', 'sum'),
                total_failed_attempts=('failed_attempts', 'sum'),
            )
            .assign(
                runs_total=lambda x: x['runs_success'] + x['runs_failed'] + x['runs_other']
            )
            .reset_index()
        )

        return {
            'list_of_modules_used_to_automate': list_of_modules_used_to_automate,
            'total_modules_used_to_automate': total_modules_used_to_automate,
            'avg_number_of_modules_used_in_a_playbooks': avg_number_of_modules_used_in_a_playbooks,
            'module_stats': module_stats.to_dict(orient='records'),
            'total_modules_used_per_playbook': total_modules_used_per_playbook.to_dict(orient='records'),
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
