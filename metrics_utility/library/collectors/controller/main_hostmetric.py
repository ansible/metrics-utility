"""Collector for host metric data from the Controller database."""

from ..util import DataframeOutput, collector, date_where, ensure_functions


# Rows fetched per keyset page when materialising into a DataFrame. COPY-based
# outputs stream the whole result set and ignore this.
PAGE_SIZE = 10000


def _host_metric_query(*, since=None, until=None, marker=None, limit=None):
    """Build the Controller host-metric query and its bound parameters."""
    automation_where = date_where('main_hostmetric.last_automation', since, until)
    deletion_where = date_where('main_hostmetric.last_deleted', since, until)
    where_sql = 'true' if automation_where == deletion_where == 'true' else f'({automation_where} OR {deletion_where})'
    params = []

    if marker is not None:
        # Multi-column keyset comparison matching the ORDER BY below. The marker
        # values come from a previous result row and must remain bound parameters.
        where_sql += ' AND (main_hostmetric.hostname > %s OR (main_hostmetric.hostname = %s AND COALESCE(main_host.id, 0) > %s))'
        last_hostname, last_host_id = marker
        params.extend([last_hostname, last_hostname, last_host_id])

    limit_sql = ''
    if limit is not None:
        limit_sql = 'LIMIT %s'
        params.append(limit)

    query = f"""
        SELECT
            main_hostmetric.hostname,
            COALESCE(main_host.id, 0) AS host_id,
            main_hostmetric.first_automation,
            main_hostmetric.last_automation,
            main_hostmetric.automated_counter,
            main_hostmetric.deleted_counter,
            main_hostmetric.last_deleted,
            main_hostmetric.deleted,
            main_hostmetric.id,
            main_hostmetric.used_in_inventories,
            main_host.ansible_facts->>'ansible_product_serial'::TEXT AS ansible_product_serial,
            main_host.ansible_facts->>'ansible_machine_id'::TEXT AS ansible_machine_id,
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

        FROM main_hostmetric
        LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname
        WHERE {where_sql}
        ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
        {limit_sql}
    """

    return query, params


def _next_marker(row):
    """Return the keyset marker (``(hostname, host_id)``) for the given result row."""
    return (row['hostname'], row['host_id'])


@collector
def main_hostmetric(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect host metric records from the Controller database.

    Reads ``main_hostmetric`` LEFT JOIN ``main_host``, filtered by
    ``last_automation`` or ``last_deleted``. Used by the Renewal Guidance report.

    Uses keyset pagination on ``(hostname, host_id)`` when materialising into a
    DataFrame; COPY-based outputs stream the whole result set in one query.

    Args:
        db: Django database connection.
        since: Inclusive start datetime for the host-metric change filter.
        until: Exclusive end datetime for the host-metric change filter (pass None
            to avoid an upper bound, as the Renewal Guidance report does).
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with host metric fields, or list of CSV paths.
    """

    def build_page(marker, limit):
        return _host_metric_query(since=since, until=until, marker=marker, limit=limit)

    # ensure_functions writes to DB, cannot be used in service (readonly DB)
    ensure_functions(db)
    return output.sql_keyset(db, build_page, _next_marker, page_size=PAGE_SIZE)
