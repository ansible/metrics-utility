from ..util import collector, copy_table


@collector
def main_jobevent_service(*, db=None, since=None, until=None, output_dir=None):
    # Use the table alias 'e' here (you alias main_jobevent as e in the FROM)
    # FIXME: suspicious replace, why not e.event_data::jsonb->>'foo'
    event_data = r"replace(e.event_data, '\u', '\u005cu')::jsonb"

    jobs_query = """
        SELECT
            uj.id AS job_id,
            uj.created AS job_created
        FROM main_unifiedjob uj
        WHERE uj.finished >= %(since)s
          AND uj.finished < %(until)s
    """
    jobs = []

    # do raw sql for django.db connection
    with db.cursor() as cursor:
        cursor.execute(jobs_query, {'since': since, 'until': until})
        jobs = cursor.fetchall()

    # No jobs in the window
    if not jobs:
        return None

    # Build a literal WHERE clause that preserves (job_id, job_created) pairing
    # (e.job_id, e.job_created) IN (VALUES (id1, 'ts1'::timestamptz), ...)
    pairs_sql = ',\n'.join(f"({jid}, '{jcreated.isoformat()}'::timestamptz)" for jid, jcreated in jobs)
    where_clause = f'(e.job_id, e.job_created) IN (VALUES {pairs_sql})'

    # Final event query
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
            ({event_data}->>'task_action')       AS task_action,
            ({event_data}->>'resolved_action')   AS resolved_action,
            ({event_data}->>'resolved_role')     AS resolved_role,
            ({event_data}->>'duration')          AS duration,
            ({event_data}->>'start')::timestamptz AS start,
            ({event_data}->>'end')::timestamptz   AS end,
            ({event_data}->>'task_uuid')        AS task_uuid,
            COALESCE( ({event_data}->>'ignore_errors')::boolean, false ) AS ignore_errors,
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
            {event_data}->'res'->'warnings'     AS warnings,
            {event_data}->'res'->'deprecations' AS deprecations,

            CASE
                WHEN e.event = 'playbook_on_stats'
                THEN {event_data} - 'artifact_data'
            END AS playbook_on_stats,

            uj.failed as job_failed,
            uj.started as job_started

        FROM main_jobevent e
        LEFT JOIN main_unifiedjob uj ON uj.id = e.job_id
        WHERE {where_clause}
    """

    return copy_table(db=db, table='main_jobevent', query=query, output_dir=output_dir)
