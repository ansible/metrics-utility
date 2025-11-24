from ..util import collector, copy_table, date_where


def _main_host_query(where):
    return f"""
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


@collector
def main_host(*, db=None, output_dir=None):
    query = _main_host_query("enabled='t'")
    return copy_table(db=db, table='main_host', query=query, prepend_query=True, output_dir=output_dir)


@collector
def main_host_daily(*, db=None, since=None, until=None, output_dir=None):
    # prefer running with until=False, to not skip hosts that keep being modified

    where = f"""
        enabled='t'
        AND ({date_where('main_host.created', since, until)}
        OR {date_where('main_host.modified', since, until)})
    """
    query = _main_host_query(where)
    return copy_table(db=db, table='main_host_daily', query=query, prepend_query=True, output_dir=output_dir)
