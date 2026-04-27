from ..util import DataframeOutput, collector, date_where, get_batch_size


@collector
def main_indirectmanagednodeaudit(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect indirect managed node audit rows for the given time window.

    Joins main_indirectmanagednodeaudit with job, inventory, and organization tables.
    Only available on Controller >= 2.6 (the table does not exist on 2.4).
    Supports METRICS_UTILITY_GATHER_BATCH_SIZE for ID-range batching on the primary table.
    """
    where = date_where('main_indirectmanagednodeaudit.created', since, until)

    def build_query(batch_filter='TRUE'):
        return f"""
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
            WHERE {where} AND ({batch_filter})
            ORDER BY main_indirectmanagednodeaudit.id ASC
        """

    batch_size = get_batch_size()
    if batch_size:
        min_max_query = f'SELECT MIN(id), MAX(id) FROM main_indirectmanagednodeaudit WHERE {where}'
        return output.batch_sql(
            db,
            query_fn=lambda s, e: build_query(f'main_indirectmanagednodeaudit.id >= {s} AND main_indirectmanagednodeaudit.id < {e}'),
            min_max_query=min_max_query,
            batch_size=batch_size,
        )

    return output.sql(db, build_query())
