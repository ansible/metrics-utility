from psycopg import sql

from ..util import collector, copy_table, date_where


def _main_host_query(where):
    """
    Build main_host query with dynamic WHERE clause.

    Args:
        where: Either a string (for simple WHERE clauses) or sql.SQL object

    Returns:
        sql.SQL query or string
    """
    query_template = """
        SELECT
            main_host.name as host_name,
            main_host.id AS host_id,
            main_inventory.id AS inventory_remote_id,
            main_inventory.name AS inventory_name,
            main_organization.id AS organization_remote_id,
            main_organization.name AS organization_name,
            main_unifiedjob.created AS last_automation,

            CASE
                WHEN (metrics_utility_is_valid_json(main_host.variables))
                THEN main_host.variables::jsonb->>'ansible_host'
                ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
            END AS ansible_host_variable,

            jsonb_build_object(
                'ansible_product_serial', main_host.ansible_facts->>'ansible_product_serial'::TEXT,
                'ansible_machine_id', main_host.ansible_facts->>'ansible_machine_id'::TEXT,
                'ansible_host',
                CASE
                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                    THEN main_host.variables::jsonb->>'ansible_host'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                END,
                'host_name', main_host.name,
                'ansible_port',
                CASE
                    WHEN (
                        CASE
                            WHEN (metrics_utility_is_valid_json(main_host.variables))
                            THEN main_host.variables::jsonb->>'ansible_port'
                            ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_port' )
                        END
                    ) ~ '^[0-9]+$'
                    THEN (
                        CASE
                            WHEN (metrics_utility_is_valid_json(main_host.variables))
                            THEN main_host.variables::jsonb->>'ansible_port'
                            ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_port' )
                        END
                    )::INTEGER
                    ELSE NULL
                END
            ) AS canonical_facts,

            jsonb_build_object(
                'ansible_connection_variable',
                CASE
                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                    THEN main_host.variables::jsonb->>'ansible_connection'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                END,
                'ansible_virtualization_type',
                main_host.ansible_facts->>'ansible_virtualization_type'::TEXT,
                'ansible_virtualization_role',
                main_host.ansible_facts->>'ansible_virtualization_role'::TEXT,
                'ansible_system_vendor',
                main_host.ansible_facts->>'ansible_system_vendor'::TEXT,
                'ansible_product_name',
                main_host.ansible_facts->>'ansible_product_name'::TEXT,
                'ansible_architecture',
                main_host.ansible_facts->>'ansible_architecture'::TEXT,
                'ansible_processor',
                main_host.ansible_facts->>'ansible_processor'::TEXT,
                'ansible_form_factor',
                main_host.ansible_facts->>'ansible_form_factor'::TEXT,
                'ansible_bios_vendor',
                main_host.ansible_facts->>'ansible_bios_vendor'::TEXT,
                'ansible_bios_version',
                main_host.ansible_facts->>'ansible_bios_version'::TEXT,
                'ansible_board_serial',
                main_host.ansible_facts->>'ansible_board_serial'::TEXT
            ) AS facts

        FROM main_host
        LEFT JOIN main_inventory ON main_inventory.id = main_host.inventory_id
        LEFT JOIN main_organization ON main_organization.id = main_inventory.organization_id
        LEFT JOIN main_unifiedjob ON main_unifiedjob.id = main_host.last_job_id
        WHERE {where}
        ORDER BY main_host.id ASC
    """

    # If where is a string, return a simple f-string formatted query
    if isinstance(where, str):
        return query_template.format(where=where)

    # Otherwise, it's an sql.SQL object, use sql.SQL formatting
    return sql.SQL(query_template).format(where=where)


@collector
def main_host(*, db=None, output_dir=None):
    query = _main_host_query("enabled='t'")
    return copy_table(db=db, table='main_host', query=query, prepend_query=True, output_dir=output_dir)


@collector
def main_host_daily(*, db=None, since=None, until=None, output_dir=None):
    # prefer running with until=False, to not skip hosts that keep being modified

    # Build WHERE clause using date_where
    created_where, created_params = date_where('main_host.created', since, until)
    modified_where, modified_params = date_where('main_host.modified', since, until)

    # Rename params to avoid conflicts
    params_created = {f'created_{k}': v for k, v in created_params.items()}
    params_modified = {f'modified_{k}': v for k, v in modified_params.items()}
    all_params = {**params_created, **params_modified}

    # Update the SQL queries to use renamed params
    created_where_str = created_where.as_string().replace('%(since)s', '%(created_since)s').replace('%(until)s', '%(created_until)s')
    modified_where_str = modified_where.as_string().replace('%(since)s', '%(modified_since)s').replace('%(until)s', '%(modified_until)s')

    where = sql.SQL("""
        enabled='t'
        AND ({created}
        OR {modified})
    """).format(
        created=sql.SQL(created_where_str),
        modified=sql.SQL(modified_where_str),
    )

    query_obj = _main_host_query(where)
    # Convert to string (no context needed, uses default encoding)
    query = query_obj.as_string()

    return copy_table(
        db=db,
        table='main_host_daily',
        query=query,
        params=all_params,
        prepend_query=True,
        output_dir=output_dir,
    )
