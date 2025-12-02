from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class ExecutionEnvironmentsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - execution_environment_service collector data
    """

    def __init__(self):
        super().__init__('execution_environments')
        self.collector_names = ['execution_environments']

    def base(self, dataframe):
        """
        Number of execution enviornment configured in the controller
        Ratio of Default EE vs Custom EE
        """

        # default vs custom EE - field Managed in table (true for default).
        # simple count of rows that has managed = true

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            return {
                'json': {},
                'rollup': {'aggregated': dataframe},
            }

        EE_total = int(len(dataframe))
        dataframe['managed'] = dataframe['managed'].map({'t': True, 'f': False})
        EE_default_total = int(dataframe['managed'].sum())
        EE_custom_total = EE_total - EE_default_total

        # Prepare JSON data (same as rollup for scalar values)
        json_data = {
            'EE_total': EE_total,
            'EE_default_total': EE_default_total,
            'EE_custom_total': EE_custom_total,
        }

        # Prepare rollup data (raw values before conversion)
        rollup_data = {
            'execution_environments': json_data,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
