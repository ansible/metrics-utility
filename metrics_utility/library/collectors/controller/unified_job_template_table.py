"""Collector for unified job template table data."""

from ..util import DataframeOutput, collector


@collector
def unified_job_template_table(*, db=None, output=DataframeOutput()):
    """Return full dump of unified job templates."""
    query = """
        SELECT
            main_unifiedjobtemplate.id,
            main_unifiedjobtemplate.polymorphic_ctype_id,
            django_content_type.model,
            main_executionenvironment.image AS execution_environment_image,
            main_unifiedjobtemplate.created,
            main_unifiedjobtemplate.modified,
            main_unifiedjobtemplate.created_by_id,
            main_unifiedjobtemplate.modified_by_id,
            main_unifiedjobtemplate.name,
            main_unifiedjobtemplate.current_job_id,
            main_unifiedjobtemplate.last_job_id,
            main_unifiedjobtemplate.last_job_failed,
            main_unifiedjobtemplate.last_job_run,
            main_unifiedjobtemplate.next_job_run,
            main_unifiedjobtemplate.next_schedule_id,
            main_unifiedjobtemplate.status
        FROM main_unifiedjobtemplate
        JOIN django_content_type
            ON main_unifiedjobtemplate.polymorphic_ctype_id = django_content_type.id
        LEFT JOIN main_executionenvironment
            ON main_executionenvironment.id = main_unifiedjobtemplate.execution_environment_id
        ORDER BY main_unifiedjobtemplate.id ASC
    """
    return output.sql(db, query)
