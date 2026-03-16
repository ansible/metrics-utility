from ..util import DataframeOutput, collector


@collector
def job_host_summary_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    where = ' AND '.join(
        [
            f"mu.finished >= '{since.isoformat()}'",
            f"mu.finished < '{until.isoformat()}'",
        ]
    )

    query = f"""
        WITH
            -- First: restrict to jobs that FINISHED in the window (uses index on main_unifiedjob.finished if present)
            filtered_jobs AS (
                SELECT mu.id
                FROM main_unifiedjob mu
                WHERE {where}
                AND mu.finished IS NOT NULL
            ),
            -- Then: only host summaries that belong to those jobs (uses index on main_jobhostsummary.job_id)
            filtered_hosts AS (
                SELECT DISTINCT mjs.host_id
                FROM main_jobhostsummary mjs
                JOIN filtered_jobs fj ON fj.id = mjs.job_id
            ),
            -- ansible_host_variable & ansible_connection_variable
            hosts_variables AS (
                SELECT fh.host_id, h.variables
                FROM filtered_hosts fh
                LEFT JOIN main_host h ON h.id = fh.host_id
            )
        SELECT
            mjs.id,
            mjs.created,
            mjs.modified,
            mjs.host_name,
            mjs.host_id AS host_remote_id,
            hv.variables,
            mjs.changed,
            mjs.dark,
            mjs.failures,
            mjs.ok,
            mjs.processed,
            mjs.skipped,
            mjs.failed,
            mjs.ignored,
            mjs.rescued,
            mu.created AS job_created,
            mjs.job_id AS job_remote_id,
            mu.unified_job_template_id AS job_template_remote_id,
            mut.name AS job_template_name,
            mu.ansible_version,
            mu.launch_type,
            mi.id AS inventory_remote_id,
            mi.name AS inventory_name,
            mo.id AS organization_remote_id,
            mo.name AS organization_name,
            mup.id AS project_remote_id,
            mup.name AS project_name,
            dct.model AS model
        FROM filtered_jobs fj
        JOIN main_jobhostsummary mjs ON mjs.job_id = fj.id
        LEFT JOIN main_job mj ON mjs.job_id = mj.unifiedjob_ptr_id
        LEFT JOIN main_unifiedjob mu ON mu.id = mjs.job_id
        LEFT JOIN django_content_type dct ON mu.polymorphic_ctype_id = dct.id
        LEFT JOIN main_unifiedjobtemplate AS mut ON mut.id = mu.unified_job_template_id
        LEFT JOIN main_unifiedjobtemplate AS mup ON mup.id = mj.project_id
        LEFT JOIN main_inventory mi ON mi.id = mj.inventory_id
        LEFT JOIN main_organization mo ON mo.id = mu.organization_id
        LEFT JOIN hosts_variables hv ON hv.host_id = mjs.host_id
        ORDER BY mu.finished ASC
    """

    return output.sql(db, query)
