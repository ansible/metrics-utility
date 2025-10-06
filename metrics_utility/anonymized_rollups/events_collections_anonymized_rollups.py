


class Event_Collections_Anonymized_Rollups:
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
    def event_collections_aggregations(dataframe):
        """
        Aggregates job-level metrics by collection source:

          * Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
          * Average job duration for collection sources.
          * Average number of hosts automated per job for each collection source.
          * Number of jobs per collection source that have failed.
          * Success/failure rate of jobs per collection source.
          * Number of jobs executed that use a specific partner collection - TODO - not implemented yet, must be communicated

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

