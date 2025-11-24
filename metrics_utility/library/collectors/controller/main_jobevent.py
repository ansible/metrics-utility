from ..util import collector, copy_table


@collector
def main_jobevent(*, db=None, since=None, until=None, output_dir=None):
    where = ' AND '.join(
        [
            f"main_jobhostsummary.modified >= '{since.isoformat()}'",
            f"main_jobhostsummary.modified < '{until.isoformat()}'",
        ]
    )

    query = f"""
        WITH job_scope AS (
            SELECT
                main_jobhostsummary.id AS main_jobhostsummary_id,
                main_jobhostsummary.created AS main_jobhostsummary_created,
                main_jobhostsummary.modified AS main_jobhostsummary_modified,
                main_unifiedjob.created AS job_created,
                main_jobhostsummary.job_id AS job_id,
                main_jobhostsummary.host_name
            FROM main_jobhostsummary
            JOIN main_unifiedjob ON main_unifiedjob.id = main_jobhostsummary.job_id
            WHERE {where}
        )
        SELECT
            job_scope.main_jobhostsummary_id,
            job_scope.main_jobhostsummary_created,
            main_jobevent.id,
            main_jobevent.created,
            main_jobevent.modified,
            main_jobevent.job_created as job_created,
            main_jobevent.event,
            (ed.event_data->>'task_action')::TEXT AS task_action,
            (ed.event_data->>'resolved_action')::TEXT AS resolved_action,
            (ed.event_data->>'resolved_role')::TEXT AS resolved_role,
            (ed.event_data->>'duration')::TEXT AS duration,
            main_jobevent.failed,
            main_jobevent.changed,
            main_jobevent.playbook,
            main_jobevent.play,
            main_jobevent.task,
            main_jobevent.role,
            main_jobevent.job_id as job_remote_id,
            main_jobevent.host_id as host_remote_id,
            main_jobevent.host_name
        FROM main_jobevent
        CROSS JOIN LATERAL (
            SELECT replace(main_jobevent.event_data, '\\u', '\\u005cu')::jsonb AS event_data
        ) AS ed
        JOIN job_scope ON
            job_scope.job_created = main_jobevent.job_created
            AND job_scope.job_id = main_jobevent.job_id
            AND job_scope.host_name = main_jobevent.host_name
        WHERE main_jobevent.event IN (
            'runner_on_ok',
            'runner_on_failed',
            'runner_on_unreachable',
            'runner_on_skipped',
            'runner_retry',
            'runner_on_async_ok',
            'runner_item_on_ok',
            'runner_item_on_failed',
            'runner_item_on_skipped'
        )
        """

    return copy_table(db=db, table='main_jobevent', query=query, output_dir=output_dir)
