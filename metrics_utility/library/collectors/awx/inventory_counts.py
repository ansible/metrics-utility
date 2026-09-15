"""Collector for inventory counts with host counts and source details."""

from ..util import DictOutput, collector


@collector
def inventory_counts(*, db=None, output=DictOutput()):
    """Return inventory details keyed by inventory ID.

    Includes host counts, source counts, and per-source details for normal
    inventories, plus smart inventory entries.
    """
    counts = {}

    with db.cursor() as cursor:
        # Normal inventories with host and source counts
        cursor.execute("""
            SELECT
                main_inventory.id,
                main_inventory.name,
                main_inventory.kind,
                COUNT(DISTINCT main_host.id) AS num_hosts,
                COUNT(DISTINCT main_inventorysource.unifiedjobtemplate_ptr_id) AS num_sources
            FROM main_inventory
            LEFT JOIN main_host
                ON main_host.inventory_id = main_inventory.id
            LEFT JOIN main_inventorysource
                ON main_inventorysource.inventory_id = main_inventory.id
            WHERE main_inventory.kind = ''
            GROUP BY main_inventory.id, main_inventory.name, main_inventory.kind
        """)
        for inv_id, name, kind, num_hosts, num_sources in cursor.fetchall():
            counts[inv_id] = {
                'name': name,
                'kind': kind,
                'hosts': num_hosts,
                'sources': num_sources,
                'source_list': [],
            }

        # Source details per inventory
        if counts:
            cursor.execute("""
                SELECT
                    main_inventorysource.inventory_id,
                    main_unifiedjobtemplate.name,
                    main_inventorysource.source,
                    COUNT(DISTINCT main_host_inventory_sources.host_id) AS num_hosts
                FROM main_inventorysource
                JOIN main_unifiedjobtemplate
                    ON main_unifiedjobtemplate.id = main_inventorysource.unifiedjobtemplate_ptr_id
                LEFT JOIN main_host_inventory_sources
                    ON main_host_inventory_sources.inventorysource_id = main_inventorysource.unifiedjobtemplate_ptr_id
                GROUP BY main_inventorysource.inventory_id, main_unifiedjobtemplate.name, main_inventorysource.source
            """)
            for inv_id, name, source, num_hosts in cursor.fetchall():
                if inv_id in counts:
                    counts[inv_id]['source_list'].append(
                        {
                            'name': name,
                            'source': source,
                            'num_hosts': num_hosts,
                        }
                    )

        # Smart inventories
        cursor.execute("""
            SELECT
                main_inventory.id,
                main_inventory.name,
                main_inventory.kind,
                COUNT(main_host.id) AS num_hosts
            FROM main_inventory
            LEFT JOIN main_host
                ON main_host.inventory_id = main_inventory.id
            WHERE main_inventory.kind = 'smart'
            GROUP BY main_inventory.id, main_inventory.name, main_inventory.kind
        """)
        for inv_id, name, kind, num_hosts in cursor.fetchall():
            counts[inv_id] = {
                'name': name,
                'kind': kind,
                'hosts': num_hosts,
                'sources': 0,
                'source_list': [],
            }

    return output.dict(counts)
