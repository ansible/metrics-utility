"""Collector for indirect managed node audit records from the Controller database."""

from ..util import DataframeOutput, collector, date_where


@collector
def main_indirectmanagednodeaudit(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect indirect managed node audit records.

    When since and until are omitted the query returns all records (full-table
    snapshot). Pass since/until to restrict results to a specific time window
    based on the related job's finished timestamp.

    Args:
        db: Django database connection.
        since: Optional inclusive start datetime for the job ``finished`` filter.
        until: Optional exclusive end datetime for the job ``finished`` filter.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with indirect node audit fields, or list of CSV paths.
    """
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
        WHERE
            {date_where('main_unifiedjob.finished', since, until)}
    """

    return output.sql(db, query)
