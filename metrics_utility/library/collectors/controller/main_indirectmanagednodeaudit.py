from ..util import collector, copy_table


@collector
def main_indirectmanagednodeaudit(*, db=None, since=None, until=None, output_dir=None):
    where = ' AND '.join(
        [
            f"main_indirectmanagednodeaudit.created >= '{since.isoformat()}'",
            f"main_indirectmanagednodeaudit.created < '{until.isoformat()}'",
        ]
    )

    query = f"""
        SELECT
            main_indirectmanagednodeaudit.id,
            main_indirectmanagednodeaudit.created as created,
            main_indirectmanagednodeaudit.name as host_name,
            main_indirectmanagednodeaudit.host_id AS host_remote_id,
            main_indirectmanagednodeaudit.canonical_facts,
            main_indirectmanagednodeaudit.facts,
            main_indirectmanagednodeaudit.events,
            main_indirectmanagednodeaudit.count as task_runs,
            main_unifiedjob.created AS job_created,
            main_indirectmanagednodeaudit.job_id AS job_remote_id,
            main_unifiedjob.unified_job_template_id AS job_template_remote_id,
            main_unifiedjob.name AS job_template_name,
            main_inventory.id AS inventory_remote_id,
            main_inventory.name AS inventory_name,
            main_organization.id AS organization_remote_id,
            main_organization.name AS organization_name,
            main_unifiedjobtemplate_project.id AS project_remote_id,
            main_unifiedjobtemplate_project.name AS project_name
        FROM main_indirectmanagednodeaudit
        LEFT JOIN main_job ON main_job.unifiedjob_ptr_id = main_indirectmanagednodeaudit.job_id
        LEFT JOIN main_unifiedjob ON main_unifiedjob.id = main_indirectmanagednodeaudit.job_id
        LEFT JOIN main_inventory ON main_inventory.id = main_indirectmanagednodeaudit.inventory_id
        LEFT JOIN main_organization ON main_organization.id = main_unifiedjob.organization_id
        LEFT JOIN main_unifiedjobtemplate AS main_unifiedjobtemplate_project ON main_unifiedjobtemplate_project.id = main_job.project_id
        WHERE {where}
        ORDER BY main_indirectmanagednodeaudit.created ASC
    """

    return copy_table(db=db, table='main_indirectmanagednodeaudit', query=query, output_dir=output_dir)
