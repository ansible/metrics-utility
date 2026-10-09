"""Collector for project counts grouped by organization and SCM type."""

from ..util import DataframeOutput, collector


@collector
def projects_by_scm_type_org(*, db=None, output=DataframeOutput()):
    """Return project counts by owning organization and normalized SCM type."""
    query = """
        SELECT
            main_unifiedjobtemplate.organization_id,
            main_organization.name AS organization_name,
            COALESCE(NULLIF(main_project.scm_type, ''), 'manual') AS scm_type,
            COUNT(*) AS project_count
        FROM main_project
        JOIN main_unifiedjobtemplate
            ON main_unifiedjobtemplate.id = main_project.unifiedjobtemplate_ptr_id
        LEFT JOIN main_organization
            ON main_organization.id = main_unifiedjobtemplate.organization_id
        GROUP BY
            main_unifiedjobtemplate.organization_id,
            main_organization.name,
            COALESCE(NULLIF(main_project.scm_type, ''), 'manual')
        ORDER BY organization_name NULLS LAST, organization_id, scm_type
    """

    return output.sql(db, query)
