"""Raw AWX automation event analytics collector."""

from datetime import datetime

from ..util import DataframeOutput, collector


@collector
def events_table(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Return direct ``main_jobevent`` analytics rows for a modified window."""
    since_sql, until_sql = _window(since, until)
    query = f"""
        SELECT
            main_jobevent.id,
            main_jobevent.created,
            main_jobevent.modified,
            main_jobevent.job_created,
            main_jobevent.uuid,
            main_jobevent.parent_uuid,
            main_jobevent.event,
            x.task_action,
            x.resolved_action,
            x.resolved_role,
            CASE
                WHEN main_jobevent.event = 'playbook_on_stats'
                THEN replace(main_jobevent.event_data, '\\u', '\\u005cu')::jsonb - 'artifact_data'
            END AS playbook_on_stats,
            main_jobevent.failed,
            main_jobevent.changed,
            main_jobevent.playbook,
            main_jobevent.play,
            main_jobevent.task,
            main_jobevent.role,
            main_jobevent.job_id,
            main_jobevent.host_id,
            main_jobevent.host_name,
            CAST(x.start AS TIMESTAMP WITH TIME ZONE) AS start,
            CAST(x."end" AS TIMESTAMP WITH TIME ZONE) AS end,
            x.duration,
            x.res->'warnings' AS warnings,
            x.res->'deprecations' AS deprecations
        FROM main_jobevent,
             jsonb_to_record(
                 replace(main_jobevent.event_data, '\\u', '\\u005cu')::jsonb
             ) AS x(
                 res json,
                 duration text,
                 task_action text,
                 resolved_action text,
                 resolved_role text,
                 start text,
                 "end" text
             )
        WHERE main_jobevent.modified > {since_sql}
          AND main_jobevent.modified <= {until_sql}
        ORDER BY main_jobevent.id ASC
    """
    return output.sql(db, query)


def _window(since, until):
    for name, value in (('since', since), ('until', until)):
        if value is not None and not isinstance(value, datetime):
            raise TypeError(f'events_table: {name} must be a datetime, got {type(value).__name__}')
        if value is not None and value.tzinfo is None:
            raise ValueError(f'events_table: {name} must be timezone-aware')
    if since is None or until is None:
        raise ValueError('events_table requires both since and until')
    return f"'{since.isoformat()}'", f"'{until.isoformat()}'"
