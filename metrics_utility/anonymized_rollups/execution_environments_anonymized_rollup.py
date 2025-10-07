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

        total_ee = len(dataframe)
        default_ee = dataframe['managed'].sum()  # since True=1, False=0
        custom_ee = total_ee - default_ee

        return {
            'total_EE': total_ee,
            'default_EE': default_ee,
            'custom_EE': custom_ee,
        }
