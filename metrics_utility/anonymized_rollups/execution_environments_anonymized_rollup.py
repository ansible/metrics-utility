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

        # TODO - ensure all columns are present in the dataframe, then let analysis run with empty data
        if dataframe.empty:
            return {
                'json': {},
                'rollup': {'aggregated': dataframe},
            }

        total_ee = int(len(dataframe))
        dataframe['managed'] = dataframe['managed'].map({'t': True, 'f': False})
        default_ee = int(dataframe['managed'].sum())
        custom_ee = total_ee - default_ee

        # Prepare JSON data (same as rollup for scalar values)
        json_data = {
            'total_EE': total_ee,
            'default_EE': default_ee,
            'custom_EE': custom_ee,
        }

        # Prepare rollup data (raw values before conversion)
        rollup_data = {
            'execution_environments': json_data,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
