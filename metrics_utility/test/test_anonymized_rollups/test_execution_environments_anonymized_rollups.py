import pandas as pd

from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup


execution_environments = [
    {'managed': 't'},
    {'managed': 'f'},
    {'managed': 't'},
    {'managed': 'f'},
    {'managed': 'f'},
]


def test_base_counts():
    # Sample dataframe with managed column
    df = pd.DataFrame(execution_environments)

    execution_environments_anonymized_rollup = ExecutionEnvironmentsAnonymizedRollup()
    df = execution_environments_anonymized_rollup.prepare(df)
    result = execution_environments_anonymized_rollup.base(df)
    result = result['json']

    # Expected values
    assert result['total_EE'] == 5
    assert result['default_EE'] == 2  # two True
    assert result['custom_EE'] == 3  # total - default
