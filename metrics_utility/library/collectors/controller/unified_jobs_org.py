"""Collector for unified-job summaries grouped by organization."""

from ..util import DataframeOutput, collector, date_where


@collector
def unified_jobs_org(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect job counts and elapsed-time summaries by organization, job type, and status.

    Jobs without an organization remain in a NULL organization group. The time
    window follows :func:`unified_jobs` and filters on ``finished``.
    """
    query = f"""
        SELECT
            main_unifiedjob.organization_id,
            main_organization.name AS organization_name,
            django_content_type.model AS job_type,
            main_unifiedjob.status,
            COUNT(*) AS job_count,
            COUNT(*) FILTER (WHERE main_unifiedjob.failed) AS failure_count,
            COALESCE(SUM(main_unifiedjob.elapsed), 0) AS elapsed_total,
            AVG(main_unifiedjob.elapsed) AS elapsed_average
        FROM main_unifiedjob
        LEFT JOIN main_organization
            ON main_organization.id = main_unifiedjob.organization_id
        LEFT JOIN django_content_type
            ON django_content_type.id = main_unifiedjob.polymorphic_ctype_id
        WHERE {date_where('main_unifiedjob.finished', since, until)}
        GROUP BY
            main_unifiedjob.organization_id,
            main_organization.name,
            django_content_type.model,
            main_unifiedjob.status
        ORDER BY organization_name NULLS LAST, organization_id, job_type, status
    """

    return output.sql(db, query)
