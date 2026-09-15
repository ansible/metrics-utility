"""Raw AWX unified job analytics collector."""

from datetime import datetime

from ..util import DataframeOutput, collector


@collector
def unified_jobs_table(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Return jobs created or finished in the direct AWX analytics window."""
    since_sql, until_sql = _window(since, until)
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
            main_unifiedjob.created,
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
            main_job.forks
        FROM main_unifiedjob
        JOIN django_content_type
            ON main_unifiedjob.polymorphic_ctype_id = django_content_type.id
        LEFT JOIN main_job
            ON main_unifiedjob.id = main_job.unifiedjob_ptr_id
        LEFT JOIN main_inventory
            ON main_job.inventory_id = main_inventory.id
        LEFT JOIN main_organization
            ON main_organization.id = main_unifiedjob.organization_id
        LEFT JOIN main_executionenvironment
            ON main_executionenvironment.id = main_unifiedjob.execution_environment_id
        WHERE (
            (main_unifiedjob.created > {since_sql} AND main_unifiedjob.created <= {until_sql})
            OR (main_unifiedjob.finished > {since_sql} AND main_unifiedjob.finished <= {until_sql})
        )
          AND main_unifiedjob.launch_type != 'sync'
        ORDER BY main_unifiedjob.id ASC
    """
    return output.sql(db, query)


def _window(since, until):
    for name, value in (('since', since), ('until', until)):
        if value is not None and not isinstance(value, datetime):
            raise TypeError(f'unified_jobs_table: {name} must be a datetime, got {type(value).__name__}')
        if value is not None and value.tzinfo is None:
            raise ValueError(f'unified_jobs_table: {name} must be timezone-aware')
    if since is None or until is None:
        raise ValueError('unified_jobs_table requires both since and until')
    return f"'{since.isoformat()}'", f"'{until.isoformat()}'"
