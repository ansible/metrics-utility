import pandas as pd

from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup


# Test data matching credentials_service collector output format
# Columns: credential_type, job_id, model
credentials = [
    {'credential_type': 'Machine', 'job_id': 1, 'model': 'job'},
    {'credential_type': 'Machine', 'job_id': 2, 'model': 'job'},
    {'credential_type': 'Vault', 'job_id': 1, 'model': 'job'},
    {'credential_type': 'Source Control', 'job_id': 3, 'model': 'workflowjob'},
    {'credential_type': 'Source Control', 'job_id': 3, 'model': 'workflowjob'},
    {'credential_type': 'Network', 'job_id': 4, 'model': 'job'},
    {'credential_type': 'Amazon Web Services', 'job_id': 5, 'model': 'job'},
    {'credential_type': 'Amazon Web Services', 'job_id': 6, 'model': 'job'},
    {'credential_type': 'Amazon Web Services', 'job_id': 7, 'model': 'job'},
    {'credential_type': 'Container Registry', 'job_id': 8, 'model': 'job'},
]


def test_credentials_anonymized_rollup_prepare():
    """Test prepare() method extracts unique credential types in a batch."""
    df = pd.DataFrame(credentials)
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.prepare(df)

    # Result should be a DataFrame with credential_type column only
    assert isinstance(result, pd.DataFrame)
    assert 'credential_type' in result.columns

    # Check unique credential types
    credential_types = set(result['credential_type'].dropna().unique())
    assert 'Machine' in credential_types
    assert 'Vault' in credential_types
    assert 'Source Control' in credential_types
    assert 'Network' in credential_types
    assert 'Amazon Web Services' in credential_types
    assert 'Container Registry' in credential_types

    # Total rows should be 6 (one per unique credential type)
    assert len(result) == 6


def test_credentials_anonymized_rollup_base():
    """Test base() method gets unique credential types across batches and converts to JSON format."""
    # Simulate data from prepare() - unique credential types
    prepared_data = pd.DataFrame(
        [
            {'credential_type': 'Machine'},
            {'credential_type': 'Vault'},
            {'credential_type': 'Source Control'},
            {'credential_type': 'Network'},
            {'credential_type': 'Amazon Web Services'},
            {'credential_type': 'Container Registry'},
        ]
    )

    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(prepared_data)

    # Result should have 'json' key
    assert 'json' in result

    # Check JSON output format - should be a sorted list
    json_data = result['json']
    assert isinstance(json_data, list)

    # Check that all credential types are present (sorted)
    assert 'Amazon Web Services' in json_data
    assert 'Container Registry' in json_data
    assert 'Machine' in json_data
    assert 'Network' in json_data
    assert 'Source Control' in json_data
    assert 'Vault' in json_data

    # Should be sorted
    assert json_data == sorted(json_data)
    assert len(json_data) == 6


def test_credentials_anonymized_rollup_prepare_and_base():
    """Test full flow: prepare() followed by base()."""
    df = pd.DataFrame(credentials)
    credentials_rollup = CredentialsAnonymizedRollup()

    # Prepare the data
    prepared = credentials_rollup.prepare(df)

    # Base aggregation
    result = credentials_rollup.base(prepared)
    json_data = result['json']

    # Verify it's a sorted list with all unique credential types
    assert isinstance(json_data, list)
    assert len(json_data) == 6
    assert 'Amazon Web Services' in json_data
    assert 'Container Registry' in json_data
    assert 'Machine' in json_data
    assert 'Network' in json_data
    assert 'Source Control' in json_data
    assert 'Vault' in json_data
    assert json_data == sorted(json_data)


def test_credentials_anonymized_rollup_multiple_batches():
    """Test that base() correctly gets unique credential types from multiple batches."""
    # Simulate multiple batches from prepare()
    batch1 = pd.DataFrame(
        [
            {'credential_type': 'Machine'},
            {'credential_type': 'Vault'},
        ]
    )

    batch2 = pd.DataFrame(
        [
            {'credential_type': 'Machine'},  # Same type, different batch (should be deduplicated)
            {'credential_type': 'Network'},
        ]
    )

    # Concatenate batches (as would happen in real processing)
    combined = pd.concat([batch1, batch2], ignore_index=True)

    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(combined)
    json_data = result['json']

    # Should be a sorted list with unique credential types
    assert isinstance(json_data, list)
    assert len(json_data) == 3  # Machine, Vault, Network (Machine appears only once)
    assert 'Machine' in json_data
    assert 'Vault' in json_data
    assert 'Network' in json_data
    assert json_data == sorted(json_data)


def test_credentials_anonymized_rollup_prepare_empty_dataframe():
    """Test prepare() with empty dataframe."""
    df = pd.DataFrame()
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.prepare(df)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ['credential_type']


def test_credentials_anonymized_rollup_prepare_missing_column():
    """Test prepare() with missing credential_type column."""
    df = pd.DataFrame([{'job_id': 1, 'model': 'job'}])  # Missing credential_type
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.prepare(df)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ['credential_type']


def test_credentials_anonymized_rollup_base_none():
    """Test base() with None input (no data files)."""
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(None)

    assert 'json' in result
    assert result['json'] == []


def test_credentials_anonymized_rollup_base_empty_dataframe():
    """Test base() with empty dataframe."""
    df = pd.DataFrame()
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(df)

    assert 'json' in result
    assert result['json'] == []


def test_credentials_anonymized_rollup_base_missing_columns():
    """Test base() with missing required columns."""
    df = pd.DataFrame([{'some_column': 'value'}])  # Missing credential_type
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(df)

    assert 'json' in result
    assert result['json'] == []


def test_credentials_anonymized_rollup_field_name_conversion():
    """Test that credential type names are preserved correctly."""
    # Test various name formats
    test_data = pd.DataFrame(
        [
            {'credential_type': 'Machine'},
            {'credential_type': 'Source Control'},  # Space
            {'credential_type': 'Amazon Web Services'},  # Multiple spaces
            {'credential_type': 'Container-Registry'},  # Hyphen
            {'credential_type': 'My-Custom Type'},  # Hyphen and space
            {'credential_type': 'UPPERCASE'},  # Uppercase
        ]
    )

    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(test_data)
    json_data = result['json']

    # Should be a sorted list with all credential types preserved
    assert isinstance(json_data, list)
    assert len(json_data) == 6
    assert 'Amazon Web Services' in json_data
    assert 'Container-Registry' in json_data
    assert 'Machine' in json_data
    assert 'My-Custom Type' in json_data
    assert 'Source Control' in json_data
    assert 'UPPERCASE' in json_data
    assert json_data == sorted(json_data)
