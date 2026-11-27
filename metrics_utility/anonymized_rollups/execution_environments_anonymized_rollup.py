"""Anonymized rollup for execution_environment_service collector data."""

from metrics_utility.anonymized_rollups.base_anonymized_rollup import SnapshotAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class ExecutionEnvironmentsAnonymizedRollup(SnapshotAnonymizedRollup):
    """
    Collector - execution_environment_service collector data
    """

    def __init__(self):
        super().__init__('execution_environments')
        self.collector_names = ['execution_environments']

    def prepare(self, dataframe):
        """
        Transform dataframe to JSON structure with totals for managed, unmanaged and all EE.
        """
        # Convert ID columns to strings at the beginning
        if dataframe is not None and not dataframe.empty:
            dataframe = self._convert_id_columns_to_strings(dataframe)

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            return sanitize_json(self._empty_json())

        execution_environments_total = len(dataframe)
        dataframe['managed'] = dataframe['managed'].map({'t': True, 'f': False, True: True, False: False})
        execution_environments_default_total = int(dataframe['managed'].sum())
        execution_environments_custom_total = execution_environments_total - execution_environments_default_total

        result = {
            'execution_environments_total': execution_environments_total,
            'execution_environments_default_total': execution_environments_default_total,
            'execution_environments_custom_total': execution_environments_custom_total,
        }

        # Sanitize to convert NumPy types to native Python types for JSON serialization
        return sanitize_json(result)

    def _empty_json(self):
        return {
            'execution_environments_total': 0,
            'execution_environments_default_total': 0,
            'execution_environments_custom_total': 0,
        }
