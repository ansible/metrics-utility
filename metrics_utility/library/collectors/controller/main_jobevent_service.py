from datetime import timedelta

from ..util import collector, copy_table


@collector
def main_jobevent_service(*, db=None, since=None, until=None, output_dir=None):
    """
    Collects job events for jobs that finished in the given time window.

    Uses two optimizations for partition pruning:
    1. Hourly timestamp ranges in WHERE clause (literal values for partition pruning)
    2. Temporary table for job_ids to avoid huge IN clauses
    """

    jobs_query = """
        SELECT
            uj.id AS job_id,
            uj.created AS job_created
        FROM main_unifiedjob uj
        WHERE uj.finished >= %(since)s
          AND uj.finished < %(until)s
    """

    # Fetch all jobs in the time window
    with db.cursor() as cursor:
        cursor.execute(jobs_query, {'since': since, 'until': until})
        jobs = cursor.fetchall()

    # No jobs in the window
    if not jobs:
        return None

    # Create temporary table for job_ids
    # - DROP IF EXISTS ensures no bloat from previous runs in same session
    # - TEMPORARY TABLE is session-scoped, auto-drops when connection closes
    # - This avoids huge IN clauses with 100K+ job_ids

    # We are loading the finished jobs then we are filtering
    # for the job_created, this cannot be done by simple joins because
    # job_created is partitioned and partitions prunning dont work with joins
    with db.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS temp_jobevent_service_jobs')

        cursor.execute("""
            CREATE TEMPORARY TABLE temp_jobevent_service_jobs (
                job_id INTEGER PRIMARY KEY
            )
        """)

        # Insert unique job_ids
        job_ids_set = set(job_id for job_id, _ in jobs)

        # Use execute_values for efficient bulk insert
        try:
            from psycopg2.extras import execute_values

            execute_values(cursor, 'INSERT INTO temp_jobevent_service_jobs (job_id) VALUES %s', [(jid,) for jid in job_ids_set], page_size=1000)
        except ImportError:
            # Fallback for psycopg3
            for job_id in job_ids_set:
                cursor.execute('INSERT INTO temp_jobevent_service_jobs (job_id) VALUES (%s)', (job_id,))

        # Analyze temp table for query optimizer
        cursor.execute('ANALYZE temp_jobevent_service_jobs')

    # Extract unique hour boundaries from job_created timestamps
    # This reduces potentially 100K timestamps down to ~100-1000 hourly ranges
    hour_boundaries = set()
    for job_id, job_created in jobs:
        # Skip jobs with NULL created timestamp (defensive programming)
        if job_created is None:
            continue
        # Truncate to hour boundary (matching partition boundaries)
        hour_start = job_created.replace(minute=0, second=0, microsecond=0)
        hour_boundaries.add(hour_start)

    # Sort hours for range grouping
    sorted_hours = sorted(hour_boundaries)

    # Group consecutive hours into ranges to reduce OR clauses
    # e.g., hours [0,1,2,5,6,10] → ranges [(0,3), (5,7), (10,11)]
    ranges = []
    if sorted_hours:
        range_start = sorted_hours[0]
        range_end = sorted_hours[0] + timedelta(hours=1)

        for hour in sorted_hours[1:]:
            if hour == range_end:  # Consecutive hour - extend current range
                range_end = hour + timedelta(hours=1)
            else:  # Gap found - save current range and start new one
                ranges.append((range_start, range_end))
                range_start = hour
                range_end = hour + timedelta(hours=1)

        # Don't forget the last range
        ranges.append((range_start, range_end))

    # Build WHERE clause with consolidated ranges for partition pruning
    # PostgreSQL can see these literal timestamps and prune partitions accordingly
    or_clauses = []
    for range_start, range_end in ranges:
        or_clauses.append(f"(e.job_created >= '{range_start.isoformat()}'::timestamptz AND e.job_created < '{range_end.isoformat()}'::timestamptz)")

    # Handle edge case: if no ranges, use FALSE to return empty result set
    # This maintains valid SQL structure while returning 0 rows
    where_clause = ' OR '.join(or_clauses) if or_clauses else 'FALSE'

    # Final event query
    # - INNER JOIN with temp table filters events immediately (efficient!)
    # - WHERE clause enables partition pruning via literal hour boundaries
    query = f"""
        SELECT
            e.id,
            e.created,
            e.modified,
            e.job_created,
            uj.finished as job_finished,
            e.uuid,
            e.parent_uuid,
            e.event,

            -- JSON extracted fields
            (ed.event_data->>'task_action')       AS task_action,
            (ed.event_data->>'resolved_action')   AS resolved_action,
            (ed.event_data->>'resolved_role')     AS resolved_role,
            (ed.event_data->>'duration')          AS duration,
            (ed.event_data->>'start')::timestamptz AS start,
            (ed.event_data->>'end')::timestamptz   AS end,
            (ed.event_data->>'task_uuid')        AS task_uuid,
            COALESCE( (ed.event_data->>'ignore_errors')::boolean, false ) AS ignore_errors,
            e.failed,
            e.changed,
            e.playbook,
            e.play,
            e.task,
            e.role,
            e.job_id  AS job_remote_id,
            e.job_id,
            e.host_id AS host_remote_id,
            e.host_id,
            e.host_name,

            -- Warnings and deprecations (json arrays)
            ed.event_data->'res'->'warnings'     AS warnings,
            ed.event_data->'res'->'deprecations' AS deprecations,

            CASE
                WHEN e.event = 'playbook_on_stats'
                THEN ed.event_data - 'artifact_data'
            END AS playbook_on_stats,

            uj.failed as job_failed,
            uj.started as job_started

        FROM main_jobevent e
        INNER JOIN temp_jobevent_service_jobs t ON t.job_id = e.job_id
        CROSS JOIN LATERAL (
            SELECT replace(e.event_data, '\\u', '\\u005cu')::jsonb AS event_data
        ) AS ed
        LEFT JOIN main_unifiedjob uj ON uj.id = e.job_id
        WHERE ({where_clause})
    """

    return copy_table(db=db, table='main_jobevent', query=query, output_dir=output_dir)
