"""Collector for workflow job node run data."""

from ..util import DataframeOutput, collector, date_where


@collector
def workflow_job_node_table(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Return workflow job nodes filtered by modified time, with success/failure/always edges."""
    query = f"""
        SELECT
            main_workflowjobnode.id,
            main_workflowjobnode.created,
            main_workflowjobnode.modified,
            main_workflowjobnode.job_id,
            main_workflowjobnode.unified_job_template_id,
            main_workflowjobnode.workflow_job_id,
            main_workflowjobnode.inventory_id,
            success_nodes.nodes AS success_nodes,
            failure_nodes.nodes AS failure_nodes,
            always_nodes.nodes AS always_nodes,
            main_workflowjobnode.do_not_run,
            main_workflowjobnode.all_parents_must_converge
        FROM main_workflowjobnode
        LEFT JOIN (
            SELECT from_workflowjobnode_id, ARRAY_AGG(to_workflowjobnode_id) AS nodes
            FROM main_workflowjobnode_success_nodes
            GROUP BY from_workflowjobnode_id
        ) success_nodes ON main_workflowjobnode.id = success_nodes.from_workflowjobnode_id
        LEFT JOIN (
            SELECT from_workflowjobnode_id, ARRAY_AGG(to_workflowjobnode_id) AS nodes
            FROM main_workflowjobnode_failure_nodes
            GROUP BY from_workflowjobnode_id
        ) failure_nodes ON main_workflowjobnode.id = failure_nodes.from_workflowjobnode_id
        LEFT JOIN (
            SELECT from_workflowjobnode_id, ARRAY_AGG(to_workflowjobnode_id) AS nodes
            FROM main_workflowjobnode_always_nodes
            GROUP BY from_workflowjobnode_id
        ) always_nodes ON main_workflowjobnode.id = always_nodes.from_workflowjobnode_id
        WHERE {date_where('main_workflowjobnode.modified', since, until)}
        ORDER BY main_workflowjobnode.id ASC
    """
    return output.sql(db, query)
