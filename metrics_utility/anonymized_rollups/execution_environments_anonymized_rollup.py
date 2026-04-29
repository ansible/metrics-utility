"""Anonymized rollup for execution_environment_service collector data."""

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class ExecutionEnvironmentsAnonymizedRollup(BaseAnonymizedRollup):
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
            return sanitize_json(
                {
                    'execution_environments_total': 0,
                    'execution_environments_default_total': 0,
                    'execution_environments_custom_total': 0,
                }
            )

        execution_environments_total = int(len(dataframe))
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
            return {
                'json': {
                    'execution_environments_total': 0,
                    'execution_environments_default_total': 0,
                    'execution_environments_custom_total': 0,
                }
            }
        if isinstance(data, pd.DataFrame):
            data = self.prepare(data)
        return {'json': data}
