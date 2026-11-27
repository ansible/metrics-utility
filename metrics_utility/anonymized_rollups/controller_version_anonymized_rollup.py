"""Anonymized rollup for controller_version_service collector data."""

from metrics_utility.anonymized_rollups.base_anonymized_rollup import SnapshotAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class ControllerVersionAnonymizedRollup(SnapshotAnonymizedRollup):
    """
    Collector - controller_version_service collector data
    """

    def __init__(self):
        super().__init__('controller_version')
        self.collector_names = ['controller_version']

    def prepare(self, dataframe):
        """
        Transform dataframe to JSON structure with controller versions list.
        """
        # Convert ID columns to strings at the beginning
        if dataframe is not None and not dataframe.empty:
            dataframe = self._convert_id_columns_to_strings(dataframe)

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            return sanitize_json([])

        controller_versions = dataframe['controller_version'].tolist() if 'controller_version' in dataframe.columns else []

        # Sanitize to convert NumPy types to native Python types for JSON serialization
        return sanitize_json(controller_versions)

    def _empty_json(self):
        return []
