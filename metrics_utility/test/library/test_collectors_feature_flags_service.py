from django.db import connection

from metrics_utility.library.collectors.controller.feature_flags_service import feature_flags_service


def test_feature_flags_service_command():
    """Build and validate feature_flags_service output from library collector."""
    collector_instance = feature_flags_service(db=connection)
    df = collector_instance.gather()

    assert df is not None, 'feature_flags_service returned None'
    assert not df.empty, 'feature_flags_service returned an empty DataFrame'

    # Validate required columns are present
    expected_columns = {'name', 'condition', 'value', 'description', 'support_level', 'toggle_type', 'visibility'}
    assert expected_columns.issubset(set(df.columns)), f'Missing columns: {expected_columns - set(df.columns)}'

    # All returned flags must be enabled (condition='boolean', value='True')
    assert all(df['condition'] == 'boolean'), 'All returned flags should have condition=boolean'
    assert all(df['value'] == 'True'), 'All returned flags should have value=True'

    # The disabled flag should NOT appear in the results
    assert 'FEATURE_SOME_DISABLED_FLAG' not in df['name'].values, 'Disabled flag FEATURE_SOME_DISABLED_FLAG should be filtered out'

    # The two enabled flags from test data should be present
    assert 'FEATURE_INDIRECT_NODE_COUNTING_ENABLED' in df['name'].values, 'FEATURE_INDIRECT_NODE_COUNTING_ENABLED should be in results'
    assert 'FEATURE_ANALYTICS_ENABLED' in df['name'].values, 'FEATURE_ANALYTICS_ENABLED should be in results'

    # Results should be sorted by name ascending
    names = df['name'].tolist()
    assert names == sorted(names), 'Feature flags should be sorted by name ascending'

    # Exactly 2 enabled flags from test data
    assert len(df) == 2, f'Expected 2 enabled feature flags, got {len(df)}'
