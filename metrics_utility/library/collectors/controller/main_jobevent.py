from ..util import DataframeOutput, collector, date_where, get_batch_size


_JOBEVENT_TYPES = (
    'runner_on_ok',
    'runner_on_failed',
    'runner_on_unreachable',
    'runner_on_skipped',
    'runner_retry',
    'runner_on_async_ok',
    'runner_item_on_ok',
    'runner_item_on_failed',
    'runner_item_on_skipped',
)

_JOBEVENT_TYPES_SQL = ', '.join(f"'{t}'" for t in _JOBEVENT_TYPES)


@collector
def main_jobevent(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect job-event rows from the large partitioned main_jobevent table.

    Uses a job_scope CTE (based on the smaller main_jobhostsummary) to scope events to
    the given time window, filtering to the 9 relevant runner event types. When
    METRICS_UTILITY_GATHER_BATCH_SIZE is set, applies an ID-range filter on
    main_jobevent.id so each batch uses a primary-key scan rather than a full partition scan.
    """
    where = date_where('main_jobhostsummary.modified', since, until)

    def build_query(jobevent_batch_filter='TRUE'):
        return f"""
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
            WHERE main_jobevent.event IN ({_JOBEVENT_TYPES_SQL})
            AND ({jobevent_batch_filter})
        """

    batch_size = get_batch_size()
    if batch_size:
        # ID-range batching on main_jobevent.id. job_scope CTE covers the full
        # time window (based on the smaller main_jobhostsummary); the ID filter
        # is applied to the large partitioned main_jobevent table so each batch
        # uses a primary-key scan rather than a full-partition scan.
        # Note: min_max_query aliases main_jobhostsummary as 'jhs', so the time
        # filter must reference the alias (not the bare table name used in `where`).
        min_max_where = date_where('jhs.modified', since, until)
        min_max_query = f"""
            SELECT MIN(je.id), MAX(je.id)
            FROM main_jobevent je
            JOIN main_jobhostsummary jhs
                ON jhs.job_id = je.job_id AND jhs.host_name = je.host_name
            WHERE {min_max_where}
            AND je.event IN ({_JOBEVENT_TYPES_SQL})
        """
        return output.batch_sql(
            db,
            query_fn=lambda s, e: build_query(f'main_jobevent.id >= {s} AND main_jobevent.id < {e}'),
            min_max_query=min_max_query,
            batch_size=batch_size,
        )

    return output.sql(db, build_query())
