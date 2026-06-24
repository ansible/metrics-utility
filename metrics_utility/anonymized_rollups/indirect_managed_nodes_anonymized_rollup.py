"""Anonymized rollup for indirect managed node audit collector data."""

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class IndirectManagedNodesAnonymizedRollup(BaseAnonymizedRollup):
    """Rollup processor for main_indirectmanagednodeaudit collector data."""

    def __init__(self):
        super().__init__('indirect_managed_nodes')
        self.collector_names = ['main_indirectmanagednodeaudit']

    def prepare(self, dataframe):
        """Transform raw indirect node audit data for deduplication.

        Extracts unique host_remote_ids for daily deduplication and billing count.

        Returns JSON-serializable dictionary with deduplicated host IDs and total count.
        """
        dataframe = self._convert_id_columns_to_strings(dataframe)

        if dataframe.empty:
            return {
                'indirect_node_ids': [],
                'indirect_nodes_total': 0,
            }

        # Extract unique host_remote_ids for deduplication
        if 'host_remote_id' in dataframe.columns:
            indirect_node_ids = sorted(set(dataframe['host_remote_id'].dropna()))
        else:
            indirect_node_ids = []

        return sanitize_json(
            {
                'indirect_node_ids': indirect_node_ids,
                'indirect_nodes_total': len(indirect_node_ids),
            }
        )

    def base(self, data):
        """Return final rollup payload with count only (no host IDs for privacy).

        Args:
            data: Accumulated dict from merge() calls, or None if no data.

        Returns:
            Dict with 'json' key containing the final rollup data.
        """
        if data is None:
            return {'json': {'indirect_node_ids': [], 'indirect_nodes_total': 0}}
        return {'json': data}

    def merge(self, data_all, data_new):
        """Merge two indirect node rollups by deduplicating host IDs.

        Combines indirect_node_ids from multiple hourly collections,
        maintaining uniqueness for accurate daily billing counts.

        Args:
            data_all: Accumulated data from previous merges (or None for first merge)
            data_new: New data to merge in

        Returns:
            Merged dictionary with deduplicated indirect_node_ids and updated total
        """
        if data_all is None:
            return data_new

        # Merge and deduplicate indirect_node_ids
        ids_all = set(data_all.get('indirect_node_ids', []))
        ids_new = set(data_new.get('indirect_node_ids', []))
        indirect_node_ids = sorted(ids_all.union(ids_new))

        return {
            'indirect_node_ids': indirect_node_ids,
            'indirect_nodes_total': len(indirect_node_ids),
        }
