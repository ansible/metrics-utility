from metrics_utility.anonymized_rollups.base_anonymized_rollup import SnapshotAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class FeatureFlagsAnonymizedRollup(SnapshotAnonymizedRollup):
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

    def _empty_json(self):
        return []
