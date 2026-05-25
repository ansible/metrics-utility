"""Collector for unified_jobs extended with dashboard-specific fields."""

from ..util import DataframeOutput, collector, date_where


@collector
def unified_jobs_dashboard(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect unified job records with additional fields required by the automation-reports dashboard.

    Extends the base ``unified_jobs`` query with project name, launched-by user,
    label IDs, and a host count — columns the dashboard needs to populate
    ``JobData`` and ``JobLabel`` records without a separate DB round-trip.

    Filters by ``finished`` timestamp (consistent with the anonymised rollup
    pipeline). Only jobs with status ``failed`` or ``successful`` and
    ``launch_type != 'sync'`` are useful to the dashboard, but filtering is
    intentionally left to the caller so this collector can be reused broadly.

    Args:
        db: Django database connection.
        since: Inclusive start datetime for the ``finished`` filter.
        until: Exclusive end datetime for the ``finished`` filter.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame or list of CSV file paths depending on *output*.
    """
    query = f"""
        SELECT
            main_unifiedjob.id,
            main_unifiedjob.polymorphic_ctype_id,
            django_content_type.model,
            main_unifiedjob.organization_id,
            main_organization.name AS organization_name,
            main_executionenvironment.image AS execution_environment_image,
            main_job.inventory_id,
            main_inventory.name AS inventory_name,
            main_unifiedjob.execution_environment_id,
            main_unifiedjob.created,
            main_unifiedjob.modified,
            main_unifiedjob.name,
            main_unifiedjob.unified_job_template_id,
            main_unifiedjob.launch_type,
            main_unifiedjob.schedule_id,
            main_unifiedjob.execution_node,
            main_unifiedjob.controller_node,
            main_unifiedjob.cancel_flag,
            main_unifiedjob.status,
            main_unifiedjob.failed,
            main_unifiedjob.started,
            main_unifiedjob.finished,
            main_unifiedjob.elapsed,
            main_unifiedjob.job_explanation,
            main_unifiedjob.instance_group_id,
            main_unifiedjob.installed_collections,
            main_unifiedjob.ansible_version,
            main_job.forks,
            mut.name AS job_template_name,
            main_project.scm_type,
            main_project.unifiedjobtemplate_ptr_id AS project_id,
            ujp.name AS project_name,
            CASE WHEN main_unifiedjob.launch_type IN ('manual', 'relaunch')
                 THEN auth_user.id ELSE NULL END AS launched_by_id,
            CASE WHEN main_unifiedjob.launch_type IN ('manual', 'relaunch')
                 THEN auth_user.username ELSE NULL END AS launched_by_username,
            (SELECT STRING_AGG(l.label_id::text, ',')
             FROM main_unifiedjob_labels l
             WHERE l.unifiedjob_id = main_unifiedjob.id) AS label_ids,
            (SELECT COUNT(*)
             FROM main_jobhostsummary
             WHERE job_id = main_unifiedjob.id) AS num_hosts
        FROM main_unifiedjob
        LEFT JOIN django_content_type ON main_unifiedjob.polymorphic_ctype_id = django_content_type.id
        LEFT JOIN main_job ON main_unifiedjob.id = main_job.unifiedjob_ptr_id
        LEFT JOIN main_inventory ON main_job.inventory_id = main_inventory.id
        LEFT JOIN main_organization ON main_organization.id = main_unifiedjob.organization_id
        LEFT JOIN main_executionenvironment ON main_executionenvironment.id = main_unifiedjob.execution_environment_id
        LEFT JOIN main_project ON main_job.project_id = main_project.unifiedjobtemplate_ptr_id
        LEFT JOIN main_unifiedjobtemplate AS mut ON mut.id = main_unifiedjob.unified_job_template_id
        LEFT JOIN main_unifiedjobtemplate AS ujp ON ujp.id = main_project.unifiedjobtemplate_ptr_id
        LEFT JOIN auth_user ON auth_user.id = main_unifiedjob.created_by_id
        WHERE
            {date_where('main_unifiedjob.finished', since, until)}
        ORDER BY main_unifiedjob.id ASC
    """

    return output.sql(db, query)
