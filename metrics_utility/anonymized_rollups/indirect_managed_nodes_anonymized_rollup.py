"""Anonymized rollup for indirect managed node audit collector data."""

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json
from metrics_utility.metric_utils import INDIRECT


class IndirectManagedNodesAnonymizedRollup(BaseAnonymizedRollup):
    """Rollup processor for main_indirectmanagednodeaudit collector data."""

    def __init__(self):
        super().__init__('indirect_managed_nodes')
        self.collector_names = ['main_indirectmanagednodeaudit']

    def prepare(self, dataframe):
        """Transform raw indirect node audit data, adding managed_node_type tag.

        Injects managed_node_type = INDIRECT constant to tag all records as
        indirectly-managed nodes for downstream billing/rollup processing.

        Returns JSON-serializable dictionary for storage in HourlyMetricsCollection.
        """
        dataframe = self._convert_id_columns_to_strings(dataframe)

        if dataframe.empty:
            return {}

        dataframe['managed_node_type'] = INDIRECT

        # Convert DataFrame to dict and handle datetime serialization
        records = dataframe.to_dict(orient='records')

        # Convert pandas Timestamp objects to ISO format strings
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif hasattr(value, 'isoformat'):
                    record[key] = value.isoformat()

        return sanitize_json(records)
