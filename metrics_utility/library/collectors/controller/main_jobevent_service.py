from datetime import timedelta

from ..util import DataframeOutput, collector


@collector
def main_jobevent_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    """
    Collects job events for jobs that finished in the given time window.

    Uses two optimizations for partition pruning:
    1. Hourly timestamp ranges in WHERE clause (literal values for partition pruning)
    2. Subquery against main_unifiedjob to filter by job_id without materialising
       the full ID list in Python (avoids memory exhaustion and oversized IN clauses).
    """

    jobs_query = """
        SELECT
            uj.id AS job_id,
            uj.created AS job_created
        FROM main_unifiedjob uj
        WHERE uj.finished >= %(since)s
          AND uj.finished < %(until)s
    """

    # Fetch all jobs in the time window.
    # We still need the job_created timestamps to compute hourly partition ranges
    # (partition pruning does not work through joins, so literal timestamps are required).
    with db.cursor() as cursor:
        cursor.execute(jobs_query, {'since': since, 'until': until})
        jobs = cursor.fetchall()

    # Extract unique hour boundaries from job_created timestamps.
    # This reduces potentially 100K timestamps down to ~100-1000 hourly ranges.
    hour_boundaries = set()
    for _job_id, job_created in jobs:
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

    # Build WHERE clause with consolidated ranges for partition pruning.
    # PostgreSQL can see these literal timestamps and prune partitions accordingly.
    or_clauses = []
    for range_start, range_end in ranges:
        or_clauses.append(f"(e.job_created >= '{range_start.isoformat()}'::timestamptz AND e.job_created < '{range_end.isoformat()}'::timestamptz)")

    # Handle edge case: if no ranges, use FALSE to return empty result set.
    # This maintains valid SQL structure while returning 0 rows.
    timestamp_where_clause = ' OR '.join(or_clauses) if or_clauses else 'FALSE'

    # Build job_id filter as a subquery instead of a Python-materialised IN list.
    # This prevents memory exhaustion and avoids oversized query plans when hundreds
    # of thousands of job IDs would otherwise be embedded in the SQL string.
    # When no jobs exist in the window, fall back to FALSE so the schema is preserved.
    if jobs:
        since_iso = since.isoformat()
        until_iso = until.isoformat()
        job_id_where_clause = (
            f"e.job_id IN ("
            f"SELECT id FROM main_unifiedjob"
            f" WHERE finished >= '{since_iso}'::timestamptz"
            f"   AND finished < '{until_iso}'::timestamptz"
            f")"
        )
    else:
        job_id_where_clause = 'FALSE'

    # Filter for only the event types that are used in analysis
    relevant_events = [
        'runner_on_ok',
        'runner_on_async_ok',
        'runner_item_on_ok',
        'runner_on_failed',
        'runner_on_async_failed',
        'runner_item_on_failed',
        'runner_on_unreachable',
        'runner_item_on_unreachable',
        'runner_on_skipped',
        'runner_item_on_skipped',
        # job annotations
        'warning',
        'deprecated',
    ]
    event_types_str = ','.join(f"'{event}'" for event in relevant_events)
    event_type_where_clause = f'e.event IN ({event_types_str})'

    # Combine all WHERE conditions
    where_clause = f'({timestamp_where_clause}) AND ({job_id_where_clause}) AND ({event_type_where_clause})'

    # Final event query
    # - WHERE clause filters by job_id and enables partition pruning via literal hour boundaries
    query = f"""
        SELECT
            e.id,
            e.created,
            e.modified,
            e.job_created,
            uj.finished as job_finished,
            uj.ansible_version,
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
        CROSS JOIN LATERAL (
            SELECT replace(e.event_data, '\\u', '\\u005cu')::jsonb AS event_data
        ) AS ed
        LEFT JOIN main_unifiedjob uj ON uj.id = e.job_id
        WHERE {where_clause}
    """

    return output.sql(db, query)
