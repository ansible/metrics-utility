"""Collector for cluster instance topology and consumed capacity."""

import json

from ..util import DictOutput, collector


@collector
def instance_info(*, db=None, output=DictOutput()):
    """Return instance info with consumed capacity, keyed by instance UUID."""
    control_task_impact = _get_control_task_impact(db)

    query = """
        WITH exec_consumed AS (
            SELECT
                execution_node AS hostname,
                COALESCE(SUM(task_impact), 0) AS consumed
            FROM main_unifiedjob
            WHERE status IN ('running', 'waiting')
                AND execution_node IS NOT NULL
                AND execution_node != ''
            GROUP BY execution_node
        ),
        ctrl_consumed AS (
            SELECT
                controller_node AS hostname,
                COUNT(*) * %(ctrl_impact)s AS consumed
            FROM main_unifiedjob
            WHERE status IN ('running', 'waiting')
                AND controller_node IS NOT NULL
                AND controller_node != ''
            GROUP BY controller_node
        )
        SELECT
            i.uuid,
            i.hostname,
            i.version,
            i.capacity,
            i.cpu,
            i.memory,
            i.managed_by_policy,
            i.enabled,
            i.node_type,
            COALESCE(ec.consumed, 0) + COALESCE(cc.consumed, 0) AS consumed_capacity
        FROM main_instance i
        LEFT JOIN exec_consumed ec ON ec.hostname = i.hostname
        LEFT JOIN ctrl_consumed cc ON cc.hostname = i.hostname
    """

    info = {}
    with db.cursor() as cursor:
        cursor.execute(query, {'ctrl_impact': control_task_impact})
        for row in cursor.fetchall():
            uuid, _hostname, version, capacity, cpu, memory, managed_by_policy, enabled, node_type, consumed_capacity = row
            info[uuid] = {
                'uuid': uuid,
                'version': version,
                'capacity': capacity,
                'cpu': cpu,
                'memory': memory,
                'managed_by_policy': managed_by_policy,
                'enabled': enabled,
                'consumed_capacity': consumed_capacity,
                'remaining_capacity': capacity - consumed_capacity,
                'node_type': node_type,
            }
    return output.dict(info)


def _get_control_task_impact(db):
    """Read AWX_CONTROL_NODE_TASK_IMPACT from conf_setting, default 1."""
    query = """
        SELECT value FROM conf_setting
        WHERE key = 'AWX_CONTROL_NODE_TASK_IMPACT'
    """
    with db.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        if row and row[0]:
            try:
                return int(json.loads(row[0]))
            except (TypeError, ValueError):
                pass
    return 1
