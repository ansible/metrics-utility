"""Anonymized rollup for controller_version_service collector data."""

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class ControllerVersionAnonymizedRollup(BaseAnonymizedRollup):
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

    def merge(self, data_all, data_new):
        """
        For snapshot collectors, always pick new data (no merging needed).
        """
        return data_new

    def base(self, data):
        """
        Returns the data as-is (data is already computed by prepare).
        Safeguard: if data is a dataframe, call prepare on it first.
        """
        if data is None:
            return {'json': []}
        if isinstance(data, pd.DataFrame):
            data = self.prepare(data)
        return {'json': data}
