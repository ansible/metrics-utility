from ..util import collector, copy_table


@collector
def job_host_summary(*, db=None, since=None, until=None, output_dir=None):
    where = ' AND '.join(
        [
            f"main_jobhostsummary.modified >= '{since.isoformat()}'",
            f"main_jobhostsummary.modified < '{until.isoformat()}'",
        ]
    )

    # TODO: controler needs to have an index on main_jobhostsummary.modified
    query = f"""
        WITH
            filtered_hosts AS (
                SELECT DISTINCT main_jobhostsummary.host_id
                FROM main_jobhostsummary
                WHERE {where}
            ),
            hosts_variables AS (
                SELECT
                    filtered_hosts.host_id,
                    CASE
                        WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_host'
                        ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                    END AS ansible_host_variable,
                    CASE
                        WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_connection'
                        ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                    END AS ansible_connection_variable
                FROM filtered_hosts
                LEFT JOIN main_host ON main_host.id = filtered_hosts.host_id
            )
        SELECT
            main_jobhostsummary.id,
            main_jobhostsummary.created,
            main_jobhostsummary.modified,
            main_jobhostsummary.host_name,
            main_jobhostsummary.host_id as host_remote_id,
            hosts_variables.ansible_host_variable,
            hosts_variables.ansible_connection_variable,
            main_jobhostsummary.changed,
            main_jobhostsummary.dark,
            main_jobhostsummary.failures,
            main_jobhostsummary.ok,
            main_jobhostsummary.processed,
            main_jobhostsummary.skipped,
            main_jobhostsummary.failed,
            main_jobhostsummary.ignored,
            main_jobhostsummary.rescued,
            main_unifiedjob.created AS job_created,
            main_jobhostsummary.job_id AS job_remote_id,
            main_unifiedjob.unified_job_template_id AS job_template_remote_id,
            main_unifiedjob.name AS job_template_name,
            main_inventory.id AS inventory_remote_id,
            main_inventory.name AS inventory_name,
            main_organization.id AS organization_remote_id,
            main_organization.name AS organization_name,
            main_unifiedjobtemplate_project.id AS project_remote_id,
            main_unifiedjobtemplate_project.name AS project_name
        FROM main_jobhostsummary
        -- connect to main_job, that has connections into inventory and project
        LEFT JOIN main_job ON main_jobhostsummary.job_id = main_job.unifiedjob_ptr_id
        -- get project name from project_options
        LEFT JOIN main_unifiedjobtemplate AS main_unifiedjobtemplate_project ON main_unifiedjobtemplate_project.id = main_job.project_id
        -- get inventory name from main_inventory
        LEFT JOIN main_inventory ON main_inventory.id = main_job.inventory_id
        -- get job name from main_unifiedjob
        LEFT JOIN main_unifiedjob ON main_unifiedjob.id = main_jobhostsummary.job_id
        -- get organization name from main_organization
        LEFT JOIN main_organization ON main_organization.id = main_unifiedjob.organization_id
        -- get variables from precomputed hosts_variables
        LEFT JOIN hosts_variables ON hosts_variables.host_id = main_jobhostsummary.host_id
        WHERE {where}
        ORDER BY main_jobhostsummary.modified ASC
    """

    return copy_table(db=db, table='main_jobhostsummary', query=query, prepend_query=True, output_dir=output_dir)
