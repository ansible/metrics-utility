from ..util import DataframeOutput, collector, date_where, get_batch_size


@collector
def job_host_summary_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    jobs_where = date_where('mu.finished', since, until)

    def build_query(batch_filter='TRUE'):
        return f"""
            WITH
                -- First: restrict to jobs that FINISHED in the window (uses index on main_unifiedjob.finished if present)
                filtered_jobs AS (
                    SELECT mu.id
                    FROM main_unifiedjob mu
                    WHERE {jobs_where}
                    AND mu.finished IS NOT NULL
                )
            SELECT
                mjs.id,
                mjs.created,
                mjs.modified,
                mjs.host_name,
                mjs.host_id AS host_remote_id,
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
            WHERE ({batch_filter})
            ORDER BY mu.finished ASC
        """

    batch_size = get_batch_size()
    if batch_size:
        # ID range from jobhostsummary rows matching the job time window.
        # The filtered_jobs CTE remains full (it's small — one row per job).
        min_max_query = f"""
            SELECT MIN(mjs.id), MAX(mjs.id)
            FROM main_jobhostsummary mjs
            JOIN main_unifiedjob mu ON mu.id = mjs.job_id
            WHERE {jobs_where} AND mu.finished IS NOT NULL
        """
        return output.batch_sql(
            db,
            query_fn=lambda s, e: build_query(f'mjs.id >= {s} AND mjs.id < {e}'),
            min_max_query=min_max_query,
            batch_size=batch_size,
        )

    return output.sql(db, build_query())
