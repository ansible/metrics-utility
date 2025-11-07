from ..util import collector, copy_table


@collector
def job_host_summary_service(*, db=None, since=None, until=None, output_dir=None):
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
            --
            hosts_variables AS (
                SELECT
                    fh.host_id,
                    CASE
                        WHEN metrics_utility_is_valid_json(h.variables)
                        THEN h.variables::jsonb->>'ansible_host'
                        ELSE metrics_utility_parse_yaml_field(h.variables, 'ansible_host')
                    END AS ansible_host_variable,
                    CASE
                        WHEN metrics_utility_is_valid_json(h.variables)
                        THEN h.variables::jsonb->>'ansible_connection'
                        ELSE metrics_utility_parse_yaml_field(h.variables, 'ansible_connection')
                    END AS ansible_connection_variable
                FROM filtered_hosts fh
                LEFT JOIN main_host h ON h.id = fh.host_id
            )
        SELECT
            mjs.id,
            mjs.created,
            mjs.modified,
            mjs.host_name,
            mjs.host_id AS host_remote_id,
            hv.ansible_host_variable,
            hv.ansible_connection_variable,
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
            mu.name AS job_template_name,
            mi.id AS inventory_remote_id,
            mi.name AS inventory_name,
            mo.id AS organization_remote_id,
            mo.name AS organization_name,
            mup.id AS project_remote_id,
            mup.name AS project_name
        FROM filtered_jobs fj
        JOIN main_jobhostsummary mjs ON mjs.job_id = fj.id
        LEFT JOIN main_job mj ON mjs.job_id = mj.unifiedjob_ptr_id
        LEFT JOIN main_unifiedjob mu ON mu.id = mjs.job_id
        LEFT JOIN main_unifiedjobtemplate AS mup ON mup.id = mj.project_id
        LEFT JOIN main_inventory mi ON mi.id = mj.inventory_id
        LEFT JOIN main_organization mo ON mo.id = mu.organization_id
        LEFT JOIN hosts_variables hv ON hv.host_id = mjs.host_id
        ORDER BY mu.finished ASC
    """

    return copy_table(db=db, table='main_jobhostsummary', query=query, prepend_query=True, output_dir=output_dir)
