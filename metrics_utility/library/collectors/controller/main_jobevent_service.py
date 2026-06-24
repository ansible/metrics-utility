from datetime import timedelta

from metrics_utility.logger import logger

from ..util import DataframeOutput, collector


_DEFAULT_JOB_LIMIT = 2_000
_DEFAULT_ROW_LIMIT = 400_000

_RELEVANT_EVENTS = [
    'runner_on_ok',
    'runner_on_async_ok',
    'runner_item_on_ok',
    'runner_on_failed',
    'runner_on_async_failed',
    'runner_item_on_failed',
    'runner_on_unreachable',
    'runner_item_on_unreachable',
    # job annotations
    'warning',
    'deprecated',
]


def _normalize_limit(value, default, name):
    """
    Coerce *value* to a usable integer limit.

    - Non-integer / uncastable  → fall back to *default* (warning logged)
    - Negative                  → fall back to *default* (warning logged)
    - Zero                      → None  (means "no limit")
    - Positive                  → used as-is
    """
    if value is None:
        return value
    try:
        value = int(value)
    except (TypeError, ValueError):
        logger.warning(
            'main_jobevent_service: invalid %s %r, falling back to default %d.',
            name,
            value,
            default,
        )
        return default
    if value < 0:
        logger.warning(
            'main_jobevent_service: negative %s %d, falling back to default %d.',
            name,
            value,
            default,
        )
        return default
    return None if value == 0 else value


def _build_job_created_ranges(jobs):
    """
    Return a list of (range_start, range_end) tuples covering all non-NULL
    job_created timestamps, merged into consecutive-hour runs.

    e.g. hours [01, 02, 03, 06, 10] → [(01, 04), (06, 07), (10, 11)]
    """
    hour_boundaries = set()
    for _job_id, job_created in jobs:
        if job_created is None:
            continue
        hour_boundaries.add(job_created.replace(minute=0, second=0, microsecond=0))

    ranges = []
    for hour in sorted(hour_boundaries):
        if ranges and hour == ranges[-1][1]:
            ranges[-1] = (ranges[-1][0], hour + timedelta(hours=1))
        else:
            ranges.append((hour, hour + timedelta(hours=1)))
    return ranges


def _build_timestamp_where(ranges):
    """Build a SQL OR-clause for partition pruning from a list of hour ranges."""
    if not ranges:
        return 'FALSE'
    clauses = [f"(e.job_created >= '{rs.isoformat()}'::timestamptz AND e.job_created < '{re.isoformat()}'::timestamptz)" for rs, re in ranges]
    return ' OR '.join(clauses)


@collector
def main_jobevent_service(*, db=None, since=None, until=None, row_limit=_DEFAULT_ROW_LIMIT, job_limit=_DEFAULT_JOB_LIMIT, output=DataframeOutput()):
    """
    Collects job events for jobs that finished in the given time window.

    Uses two optimizations for partition pruning:
    1. Hourly timestamp ranges in WHERE clause (literal values for partition pruning)
    2. Direct job_id filtering in WHERE clause

    job_limit caps the number of jobs processed per window (sorted by job_created,
    oldest first) to keep the IN clause manageable. row_limit caps total event rows.
    """

    job_limit = _normalize_limit(job_limit, _DEFAULT_JOB_LIMIT, 'job_limit')
    row_limit = _normalize_limit(row_limit, _DEFAULT_ROW_LIMIT, 'row_limit')

    job_limit_clause = 'LIMIT %(job_limit)s' if job_limit is not None else ''
    jobs_query = f"""
        SELECT
            uj.id AS job_id,
            uj.created AS job_created
        FROM main_unifiedjob uj
        WHERE uj.finished >= %(since)s
          AND uj.finished < %(until)s
        ORDER BY uj.created
        {job_limit_clause}
    """

    with db.cursor() as cursor:
        cursor.execute(jobs_query, {'since': since, 'until': until, 'job_limit': job_limit})
        jobs = cursor.fetchall()

    if job_limit is not None and len(jobs) >= job_limit:
        logger.info(
            'main_jobevent_service: job limit reached (>= %d jobs in window). '
            'Jobs beyond the limit were not collected for this window. '
            'Increase METRICS_SERVICE_JOBEVENT_JOB_LIMIT if fuller coverage is needed.',
            job_limit,
        )

    # We are loading the finished jobs then we are filtering for job_created;
    # this cannot be done by simple joins because job_created is partitioned
    # and partition pruning does not work with joins.
    job_ids_set = {job_id for job_id, _ in jobs}
    job_id_where_clause = f'e.job_id IN ({",".join(str(j) for j in job_ids_set)})' if job_ids_set else 'FALSE'

    ranges = _build_job_created_ranges(jobs)
    timestamp_where_clause = _build_timestamp_where(ranges)

    event_types_str = ','.join(f"'{e}'" for e in _RELEVANT_EVENTS)
    where_clause = f'({timestamp_where_clause}) AND ({job_id_where_clause}) AND (e.event IN ({event_types_str}))'

    limit_clause = f'LIMIT {row_limit}' if row_limit is not None else ''

    # WHERE clause filters by job_id and enables partition pruning via literal
    # hour boundaries. LIMIT caps total rows (statistical sample; partial data
    # is acceptable for analytics).
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
        {limit_clause}
    """

    df = output.sql(db, query)

    if row_limit is not None and len(df) >= row_limit:
        logger.info(
            'main_jobevent_service: row limit reached (%d rows). '
            'Events beyond the limit were not collected for this window. '
            'Increase METRICS_SERVICE_JOBEVENT_ROW_LIMIT if fuller coverage is needed.',
            row_limit,
        )

    return df
