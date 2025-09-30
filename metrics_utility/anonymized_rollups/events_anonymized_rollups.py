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

        # for task success and failure
        dataframe['task_success_event'] = dataframe['task_success_event'].fillna(False).astype(bool)
        dataframe['task_failed_event'] = dataframe['task_failed_event'].fillna(False).astype(bool)

    @staticmethod
    def event_collections_aggregations(dataframe):
        '''
          *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
            *Average job duration for collection sources
            *Average number of hosts automated per job for each collection source.
            *Number of jobs per collection source that have failed.
            *Success/failure rate of jobs per collection source.
        '''

        # drop duplicates so each (job_id, collection_source) pair has one duration, waiting time and etc.
        dropped_duplicates = dataframe.drop_duplicates(subset=['job_id', 'collection_source'])

        # Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community)
        total_jobs_by_collection_source = dataframe.groupby('collection_source')['job_id'].nunique()

        # average job duration and waiting time for collection sources
        # problem is that we need to drop duplicates so each (job_id, collection_source) pair has one duration
        avg_job_duration_by_collection_source = dropped_duplicates.groupby('collection_source')['job_duration'].mean()
        avg_job_waiting_time_by_collection_source = dropped_duplicates.groupby('collection_source')['job_waiting_time'].mean()

        # Average number of hosts automated per job for each collection source.
        avg_hosts_per_job_by_collection_source = (
            dataframe.groupby(['collection_source', 'job_id'])['host_id'].nunique().groupby('collection_source').mean()
        )

        # Number of jobs per collection source that have failed.
        # job_failed columns is True
        # first we must filter failed jobs and count them by collection source
        failed_jobs_by_collection_source = dropped_duplicates[dropped_duplicates['job_failed']].groupby('collection_source')['job_id'].nunique()

        success_jobs_by_collection_source = total_jobs_by_collection_source - failed_jobs_by_collection_source

        # Success/failure rate of jobs per collection source.
        success_rate_by_collection_source = success_jobs_by_collection_source / total_jobs_by_collection_source

        return {
            'total_jobs_by_collection_source': total_jobs_by_collection_source,
            'avg_job_duration_by_collection_source': avg_job_duration_by_collection_source,
            'avg_job_waiting_time_by_collection_source': avg_job_waiting_time_by_collection_source,
            'avg_hosts_per_job_by_collection_source': avg_hosts_per_job_by_collection_source,
            'failed_jobs_by_collection_source': failed_jobs_by_collection_source,
            'success_jobs_by_collection_source': success_jobs_by_collection_source,
            'success_rate_by_collection_source': success_rate_by_collection_source,
        }

    @staticmethod
    def events_aggregations(dataframe):
        '''
        ?Number of jobs executed that use a specific partner collection.
        *Avg number of modules used in a playbook
        *Failure/Success rate of modules
        *Modules Used to Automate
        *Total number of modules automated
        '''

        # ?Number of jobs executed that use a specific partner collection.

        
        # Modules used to automate
        # distinct name of modules used to automate
        list_of_modules_used_to_automate = dataframe['module_name'].unique().tolist()

        # Total number of modules automated
        total_modules_used_to_automate = len(list_of_modules_used_to_automate)

        # Avg number of modules used in a playbook
        avg_number_of_modules_used_in_a_playbook = (
            dataframe.groupby('playbook')['module_name'].nunique().mean()
        )

        # Failure/Success rate of modules

        success_events_list = ['runner_on_ok', 'runner_on_async_ok']
        failed_events_list = ['runner_on_failed', 'runner_on_async_failed', 'runner_on_unreachable']

        # Mark events
        dataframe['task_success_event'] = dataframe['event'].isin(success_events_list)
        dataframe['task_failed_event'] = dataframe['event'].isin(failed_events_list)

        # Collapse events → one row per (job, module, task)
        # summarize all failed events as number of failed attempts
        # if one success events is seen, task is successful
        task_summary = (
            dataframe.groupby(['job_id', 'module_name', 'task_uuid', 'host_id'])
            .agg(
                task_success=('task_success_event', 'max'),   # any success seen?
                failed_attempts=('task_failed_event', 'sum')  # number of fails
            )
            .reset_index()
            .assign(
                task_failed=lambda x: (~x['task_success']) & (x['failed_attempts'] > 0),
                task_other=lambda x: (~x['task_success']) & (~x['task_failed'])   # skipped, ignored, etc.
            )
        )

        # Per-module counts
        module_stats = (
            task_summary.groupby('module_name')
            .agg(
                jobs_total=('job_id', 'nunique'),
                hosts_total=('host_id', 'nunique'),
                runs_total=('task_uuid', 'count'),
                runs_success=('task_success', 'sum'),
                runs_failed=('task_failed', 'sum'),
                runs_other=('task_other', 'sum'),
                total_failed_attempts=('failed_attempts', 'sum')
            )
            .reset_index()
        )
    












    # TODO - will use rollup data from events rollups - need to update events rollups
    @staticmethod
    def base(dataframe):
        """
        This function will create first level aggregation of the event dataframe, the result is json

        Aggregations are:
        
            ?Number of jobs executed that use a specific partner collection.
            - Total number of jobs executed by collection source
            - Avg number of modules used in a playbook
            - Failure/Success rate of modules
            - Modules Used to Automate
            - Total number of modules automated
        """

        Event_Anonymized_Rollups.prepare_data(dataframe)

        event_collections_aggregations = Event_Anonymized_Rollups.event_collections_aggregations(dataframe)
        events_aggregations = Event_Anonymized_Rollups.events_aggregations(dataframe)


        '''
        # group by playbook, module, include job_failed column
        aggregations_by_playbook_module = (
            dataframe.groupby(['playbook', 'module_name'])
            .agg(
                job_failed=('job_failed', 'max'),
                job_total=('job_id', 'nunique'),
            )
            .reset_index()  # <-- make keys real columns
        )

        # modules used to automate
        list_of_modules_used_to_automate = aggregations_by_playbook_module['module_name'].unique().tolist()
        total_modules_used_to_automate = len(list_of_modules_used_to_automate)

        # return as object that can be converted to json
        return {
            'aggregations_by_playbook_module': aggregations_by_playbook_module.to_dict(orient='records'),
            'list_of_modules_used_to_automate': list_of_modules_used_to_automate,
            'total_modules_used_to_automate': total_modules_used_to_automate,
        }
        '''


class Events_Collections_Anonymized_Rollups:
    """
    Collectors -
    - unified_jobs
    - main_jobevent_service
    """

    # TODO - will use rollup data from events rollups - need to update events rollups
    @staticmethod
    def base(dataframe):
        """
        events, jobs - dataframes from collectors
        This function will create first level aggregation of the job dataframe, the result is json

        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
        *Average job duration for collection sources
        *Average number of hosts automated per job for each collection source.
        *Number of jobs per collection source that have failed.
        *Success/failure rate of jobs per collection source.

        ?Number of jobs executed that use a specific partner collection.
        """
        '''
        events = dataframe

        events = events.assign(job_failed=events['job_failed'].fillna(False).astype(bool))
        events['collection_name'] = events['module_name'].apply(extract_collection_name)

        events['job_duration'] = events['job_finished'] - events['job_started']
        events['job_waiting_time'] = events['job_started'] - events['job_created']

        # fill collection source from collections_types
        events['collection_source'] = events['collection_name'].map(collections_types)

        # Total jobs executed by collection source
        total_jobs_by_collection_source = events.groupby('collection_source')['job_id'].nunique()

        # Average job duration by collection source
        # drop duplicates so each (job_id, collection_source) pair has one duration
        dropped_duplicates = events.drop_duplicates(subset=['job_id', 'collection_source'])
        avg_job_duration_by_collection_source = dropped_duplicates.groupby('collection_source')['job_duration'].mean()

        # Success/failure rate of jobs per collection source.
        # column job_failed
        failed_jobs_by_collection_source = (
            events[events['job_failed']].groupby('collection_source')['job_id'].nunique().reindex(total_jobs_by_collection_source.index, fill_value=0)
        )
        success_rate_by_collection_source = (total_jobs_by_collection_source - failed_jobs_by_collection_source) / total_jobs_by_collection_source

        # Average number of hosts automated per job for each collection source.
        # split data by collection source, count the number of unique host_id per job_id
        # then average the number of hosts per job for each collection source
        avg_hosts_per_job_by_collection_source = (
            events.groupby(['collection_source', 'job_id'])['host_id'].nunique().groupby('collection_source').mean()
        )

        result = {
            'total_jobs_by_collection_source': total_jobs_by_collection_source,
            'avg_job_duration_by_collection_source': avg_job_duration_by_collection_source,
            'failed_jobs_by_collection_source': failed_jobs_by_collection_source,
            'success_rate_by_collection_source': success_rate_by_collection_source,
            'avg_hosts_per_job_by_collection_source': avg_hosts_per_job_by_collection_source,
        }

        return result
        '''