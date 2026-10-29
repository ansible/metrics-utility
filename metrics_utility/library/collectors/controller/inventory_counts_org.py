"""Collector for inventory, host, and source counts grouped by organization."""

from ..util import DataframeOutput, collector


@collector
def inventory_counts_org(*, db=None, output=DataframeOutput()):
    """Return inventory, distinct host, and source counts by organization and kind.

    Matches :func:`inventory_counts` by covering normal and smart inventories.
    NULL organization IDs are retained as an unassigned group. Host and source
    counts are aggregated per inventory before joining, avoiding join fan-out.
    """
    query = """
        WITH host_counts AS (
            SELECT inventory_id, COUNT(DISTINCT id) AS host_count
            FROM main_host
            GROUP BY inventory_id
        ), source_counts AS (
            SELECT inventory_id, COUNT(DISTINCT unifiedjobtemplate_ptr_id) AS source_count
            FROM main_inventorysource
            GROUP BY inventory_id
        )
        SELECT
            main_inventory.organization_id,
            main_organization.name AS organization_name,
            COALESCE(NULLIF(main_inventory.kind, ''), 'normal') AS inventory_kind,
            COUNT(main_inventory.id) AS inventory_count,
            COALESCE(SUM(host_counts.host_count), 0) AS host_count,
            COALESCE(SUM(source_counts.source_count), 0) AS source_count
        FROM main_inventory
        LEFT JOIN main_organization
            ON main_organization.id = main_inventory.organization_id
        LEFT JOIN host_counts
            ON host_counts.inventory_id = main_inventory.id
        LEFT JOIN source_counts
            ON source_counts.inventory_id = main_inventory.id
        WHERE main_inventory.kind IN ('', 'smart')
        GROUP BY
            main_inventory.organization_id,
            main_organization.name,
            COALESCE(NULLIF(main_inventory.kind, ''), 'normal')
        ORDER BY organization_name NULLS LAST, organization_id, inventory_kind
    """

    return output.sql(db, query)
