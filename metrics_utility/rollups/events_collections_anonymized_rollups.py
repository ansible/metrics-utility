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
