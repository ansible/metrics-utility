"""Collectors for host inventory data from the Controller database."""

from ..util import DataframeOutput, collector, date_where, ensure_functions


def _main_host_query(where):
    """Build the host inventory SQL query with the given WHERE clause.

    Args:
        where: SQL WHERE clause fragment (already validated as safe).

    Returns:
        Complete SQL query string selecting host inventory fields.
    """
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
        LEFT JOIN LATERAL (
            SELECT main_jobhostsummary.job_id
            FROM main_jobhostsummary
            WHERE main_jobhostsummary.host_id = main_host.id
            ORDER BY main_jobhostsummary.id DESC
            LIMIT 1
        ) AS latest_job_host_summary ON TRUE
        LEFT JOIN main_unifiedjob ON main_unifiedjob.id = latest_job_host_summary.job_id
        WHERE {where}
        ORDER BY main_host.id ASC
    """


@collector
def main_host(*, db=None, output=DataframeOutput()):
    """Collect all currently-enabled hosts from the Controller inventory (snapshot).

    Args:
        db: Django database connection.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with host fields, or list of CSV paths.
    """
    query = _main_host_query("enabled='t'")

    # ensure_functions writes to DB, cannot be used in service (readonly DB)
    ensure_functions(db)
    return output.sql(db, query)


@collector
def main_host_daily(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect enabled hosts created or modified within the given time window.

    Args:
        db: Django database connection.
        since: Inclusive start datetime.
        until: Exclusive end datetime (prefer passing None to avoid skipping recently
            modified hosts).
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with host fields, or list of CSV paths.
    """
    # prefer running with until=None, to not skip hosts that keep being modified

    where = f"""
        enabled='t'
        AND ({date_where('main_host.created', since, until)}
        OR {date_where('main_host.modified', since, until)})
    """
    query = _main_host_query(where)

    # ensure_functions writes to DB, cannot be used in service (readonly DB)
    ensure_functions(db)
    return output.sql(db, query)
