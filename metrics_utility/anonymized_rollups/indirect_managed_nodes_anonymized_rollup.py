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
            return {'json': {'indirect_nodes_total': 0}}
        return {'json': {'indirect_nodes_total': data.get('indirect_nodes_total', 0)}}

    def merge(self, data_all, data_new):
        """Pass through the snapshot batch unchanged.

        With a daily snapshot collector there is only one batch per day,
        so cross-batch merging is not needed.

        Args:
            data_all: Unused (always None for snapshot collectors)
            data_new: Prepared data from the single snapshot batch

        Returns:
            data_new unchanged
        """
        return data_new
