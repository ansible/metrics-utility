import re

import collections_types


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


class Jobs_Events_Collections_Anonymized_Rollups:
    """
    Collectors -
    - unified_jobs
    - main_jobevent_service
    """

    @staticmethod
    def base(events, jobs):
        """
        events, jobs - dataframes from collectors
        This function will create first level aggregation of the job dataframe, the result is json

        Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
        Average job duration for validated content vs. community content.
        Average number of hosts automated per job for validated vs. community content.
        Number of jobs executed that use a specific partner collection.
        Number of jobs using certified content that have failed due to a known bug.
        Success/failure rate of jobs using validated vs. community content.
        """

        events['collection_name'] = events['module_name'].apply(extract_collection_name)

        events['job_duration'] = events['finished'] - events['started']
        events['job_waiting_time'] = events['started'] - events['job_created']

        # fill collection source from collections_types
        events['collection_source'] = events['collection_name'].map(collections_types)

        # Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
        # group by collection source and count distinct job_id
        collection_source_jobs = events.groupby('collection_source')['job_id'].nunique()

        # Average job duration for validated content vs. community content.
        # group by collection_source, average all duration of events per each job and average jobs
        # Step 1: drop duplicates so each (job_id, content_source) pair has one duration
        df_unique = dataframe.drop_duplicates(subset=['job_id', 'content_source'])

        # Step 2: compute average job duration by content_source
        avg_durations = df_unique.groupby('content_source')['duration'].mean()

        # Average number of hosts automated per job for validated vs. community content.
        # group by collection_source and job_id count total hosts per job and average them
        hosts_per_job = dataframe.groupby(['collection_source', 'job_id'])['host_id'].nunique().reset_index(name='hosts_automated')

        # Step 2: average across jobs, per collection_source
        avg_hosts_per_job_per_collection_source = (
            hosts_per_job.groupby('collection_source')['hosts_automated'].mean().reset_index(name='avg_hosts_per_job')
        )

        # Number of jobs executed that use a specific partner collection. TODO
        # group by collection_source, count distinct job_id
        # TODO - what is partner collection?

        # TODO - probably for all content, not only just certified
        # Number of jobs using content that have failed due to a known bug.
        # use column job_failed (boolean)
        # first filter certified content

        failed_jobs = dataframe[dataframe['job_failed'] == True]

        # count distinct job_id
        number_of_failed_jobs_certified_content = failed_jobs['job_id'].nunique()
