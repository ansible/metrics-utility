import pandas as pd

from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollups


def test_base_counts():
    # Sample dataframe with managed column
    df = pd.DataFrame({'managed': [True, False, True, False, False]})

    result = ExecutionEnvironmentsAnonymizedRollups.base(df)

    # Expected values
    assert result['total_EE'] == 5
    assert result['default_EE'] == 2  # two True
    assert result['custom_EE'] == 3  # total - default
