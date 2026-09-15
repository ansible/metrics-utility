"""Collector for workflow job template node definitions."""

from ..util import DataframeOutput, collector


@collector
def workflow_job_template_node_table(*, db=None, output=DataframeOutput()):
    """Return workflow job template node definitions with success/failure/always edges."""
    query = """
        SELECT
            main_workflowjobtemplatenode.id,
            main_workflowjobtemplatenode.created,
            main_workflowjobtemplatenode.modified,
            main_workflowjobtemplatenode.unified_job_template_id,
            main_workflowjobtemplatenode.workflow_job_template_id,
            main_workflowjobtemplatenode.inventory_id,
            success_nodes.nodes AS success_nodes,
            failure_nodes.nodes AS failure_nodes,
            always_nodes.nodes AS always_nodes,
            main_workflowjobtemplatenode.all_parents_must_converge
        FROM main_workflowjobtemplatenode
        LEFT JOIN (
            SELECT from_workflowjobtemplatenode_id, ARRAY_AGG(to_workflowjobtemplatenode_id) AS nodes
            FROM main_workflowjobtemplatenode_success_nodes
            GROUP BY from_workflowjobtemplatenode_id
        ) success_nodes ON main_workflowjobtemplatenode.id = success_nodes.from_workflowjobtemplatenode_id
        LEFT JOIN (
            SELECT from_workflowjobtemplatenode_id, ARRAY_AGG(to_workflowjobtemplatenode_id) AS nodes
            FROM main_workflowjobtemplatenode_failure_nodes
            GROUP BY from_workflowjobtemplatenode_id
        ) failure_nodes ON main_workflowjobtemplatenode.id = failure_nodes.from_workflowjobtemplatenode_id
        LEFT JOIN (
            SELECT from_workflowjobtemplatenode_id, ARRAY_AGG(to_workflowjobtemplatenode_id) AS nodes
            FROM main_workflowjobtemplatenode_always_nodes
            GROUP BY from_workflowjobtemplatenode_id
        ) always_nodes ON main_workflowjobtemplatenode.id = always_nodes.from_workflowjobtemplatenode_id
        ORDER BY main_workflowjobtemplatenode.id ASC
    """
    return output.sql(db, query)
