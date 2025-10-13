import pandas as pd

from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollups


execution_environments = [
    {'managed': True},
    {'managed': False},
    {'managed': True},
    {'managed': False},
    {'managed': False},
]


def test_base_counts():
    # Sample dataframe with managed column
    df = pd.DataFrame(execution_environments)

    result = ExecutionEnvironmentsAnonymizedRollups.base(df)
    result = result['json']

    # Expected values
    assert result['total_EE'] == 5
    assert result['default_EE'] == 2  # two True
    assert result['custom_EE'] == 3  # total - default
