import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class FeatureFlagsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Rollup for feature_flags_service collector data.

    Returns the list of enabled feature flag names (snapshot collector).
    """

    def __init__(self):
        super().__init__('feature_flags')
        self.collector_names = ['feature_flags']

    def prepare(self, dataframe):
        """
        Transform dataframe to a list of enabled feature flag names.
        """
        if dataframe is None or dataframe.empty:
            return sanitize_json([])

        flag_names = dataframe['name'].tolist() if 'name' in dataframe.columns else []

        return sanitize_json(flag_names)

    def merge(self, data_all, data_new):
        """
        For snapshot collectors, always use the latest data (no merging needed).
        """
        return data_new

    def base(self, data):
        """
        Returns the data as-is (data is already a list produced by prepare).
        Safeguard: if data is a dataframe, call prepare on it first.
        """
        if data is None:
            return {'json': []}
        if isinstance(data, pd.DataFrame):
            data = self.prepare(data)
        return {'json': data}
