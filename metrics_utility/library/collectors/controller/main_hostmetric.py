"""Collector for host metric data from the Controller database."""

from ..util import DataframeOutput, collector, date_where, ensure_functions


@collector
def main_hostmetric(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect host metric records from the Controller database.

    Used by the Renewal Guidance report.

    Args:
        db: Django database connection.
        since: Inclusive start datetime for the ``last_automation`` filter.
        until: Exclusive end datetime for the ``last_automation`` filter.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with host metric fields, or list of CSV paths.
    """
    query = f"""
        SELECT DISTINCT ON (main_hostmetric.hostname)
            main_hostmetric.hostname,
            COALESCE(main_host.id, 0) AS host_id,
            main_hostmetric.first_automation,
            main_hostmetric.last_automation,
            main_hostmetric.automated_counter,
            main_hostmetric.deleted_counter,
            main_hostmetric.last_deleted,
            main_hostmetric.deleted,
            main_host.ansible_facts->>'ansible_product_serial'::TEXT AS ansible_product_serial,
            main_host.ansible_facts->>'ansible_machine_id'::TEXT AS ansible_machine_id,
            CASE
                WHEN (metrics_utility_is_valid_json(main_host.variables))
                THEN main_host.variables::jsonb->>'ansible_host'
                ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host')
            END AS ansible_host_variable,
            CASE
                WHEN (metrics_utility_is_valid_json(main_host.variables))
                THEN main_host.variables::jsonb->>'ansible_connection'
                ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection')
            END AS ansible_connection_variable

        FROM main_hostmetric
        LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname
        WHERE {date_where('main_hostmetric.last_automation', since, until)}
        ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
    """

    ensure_functions(db)
    return output.sql(db, query)
