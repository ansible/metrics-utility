class ExecutionEnvironmentsAnonymizedRollups:
    """
    Collector - execution_environment_service collector data
    """

    @staticmethod
    def base(dataframe):
        """
        Number of execution enviornment configured in the controller
        Ratio of Default EE vs Custom EE
        """

        # default vs custom EE - field Managed in table (true for default).
        # simple count of rows that has managed = true

        total_ee = int(len(dataframe))
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
