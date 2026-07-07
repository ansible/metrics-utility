from datetime import timedelta

from metrics_utility.logger import logger

from ..util import DataframeOutput, collector


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

_ONE_HOUR = timedelta(hours=1)


def _normalize_row_limit(value):
    """
    Coerce *value* to a usable integer row limit.

    - Non-integer / uncastable  → fall back to default (warning logged)
    - Negative                  → fall back to default (warning logged)
    - Zero                      → None  (means "no limit")
    - Positive                  → used as-is
    """
    if value is None:
        return value
    try:
        value = int(value)
    except (TypeError, ValueError):
        logger.warning(
            'main_jobevent_created_service: invalid row_limit %r, falling back to default %d.',
            value,
            _DEFAULT_ROW_LIMIT,
        )
        return _DEFAULT_ROW_LIMIT
    if value < 0:
        logger.warning(
            'main_jobevent_created_service: negative row_limit %d, falling back to default %d.',
            value,
            _DEFAULT_ROW_LIMIT,
        )
        return _DEFAULT_ROW_LIMIT
    return None if value == 0 else value


@collector
def main_jobevent_created_service(*, db=None, since=None, until=None, row_limit=_DEFAULT_ROW_LIMIT, output=DataframeOutput()):
    """
    Collects job events for a single hourly partition by filtering directly on job_created.

    Unlike main_jobevent_service, which first queries jobs by their finished time and
    then filters events by job_id, this collector sets since-until directly on the
    job_created column — the partition key of main_jobevent — so PostgreSQL scans
    exactly one hourly partition with no intermediate job lookup.

    The since-until window must span exactly one hour; a ValueError is raised otherwise.
    """
    if since is None or until is None:
        raise ValueError('main_jobevent_created_service: both since and until must be provided')

    if until - since != _ONE_HOUR:
        raise ValueError(
            f'main_jobevent_created_service: since-until window must be exactly one hour, got {until - since} (since={since}, until={until})'
        )

    row_limit = _normalize_row_limit(row_limit)

    event_types_str = ','.join(f"'{e}'" for e in _RELEVANT_EVENTS)
    limit_clause = f'LIMIT {row_limit}' if row_limit is not None else ''

    query = f"""
        SELECT
            e.id,
            e.created,
            e.modified,
            e.job_created,
            uj.finished                                                     AS job_finished,
            uj.ansible_version,
            e.uuid,
            e.parent_uuid,
            e.event,

            -- JSON extracted fields
            (ed.event_data->>'task_action')                                 AS task_action,
            (ed.event_data->>'resolved_action')                             AS resolved_action,
            (ed.event_data->>'resolved_role')                               AS resolved_role,
            (ed.event_data->>'duration')                                    AS duration,
            (ed.event_data->>'start')::timestamptz                          AS start,
            (ed.event_data->>'end')::timestamptz                            AS end,
            (ed.event_data->>'task_uuid')                                   AS task_uuid,
            COALESCE( (ed.event_data->>'ignore_errors')::boolean, false )   AS ignore_errors,
            e.failed,
            e.changed,
            e.playbook,
            e.play,
            e.task,
            e.role,
            e.job_id                                                        AS job_remote_id,
            e.job_id,
            e.host_id                                                       AS host_remote_id,
            e.host_id,
            e.host_name,

            -- Warnings and deprecations (json arrays)
            ed.event_data->'res'->'warnings'                                AS warnings,
            ed.event_data->'res'->'deprecations'                            AS deprecations,

            CASE
                WHEN e.event = 'playbook_on_stats'
                THEN ed.event_data - 'artifact_data'
            END                                                             AS playbook_on_stats,

            uj.failed                                                       AS job_failed,
            uj.started                                                      AS job_started

        FROM main_jobevent e
        CROSS JOIN LATERAL (
            SELECT replace(e.event_data, '\\u', '\\u005cu')::jsonb AS event_data
        ) AS ed
        LEFT JOIN main_unifiedjob uj ON uj.id = e.job_id
        WHERE e.job_created >= '{since.isoformat()}'::timestamptz
          AND e.job_created <  '{until.isoformat()}'::timestamptz
          AND e.event IN ({event_types_str})
        {limit_clause}
    """

    df = output.sql(db, query)

    if row_limit is not None and len(df) >= row_limit:
        logger.info(
            'main_jobevent_created_service: row limit reached (%d rows). '
            'Events beyond the limit were not collected for this partition. '
            'Increase METRICS_SERVICE_JOBEVENT_ROW_LIMIT if fuller coverage is needed.',
            len(df),
        )

    return df
