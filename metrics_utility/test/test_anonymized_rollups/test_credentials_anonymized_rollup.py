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
    """Test prepare() method counts occurrences of each credential type in a batch."""
    df = pd.DataFrame(credentials)
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.prepare(df)

    # Result should be a DataFrame with credential_type and count columns
    assert isinstance(result, pd.DataFrame)
    assert 'credential_type' in result.columns
    assert 'count' in result.columns

    # Check counts for each credential type
    result_dict = dict(zip(result['credential_type'], result['count']))
    assert result_dict['Machine'] == 2
    assert result_dict['Vault'] == 1
    assert result_dict['Source Control'] == 2
    assert result_dict['Network'] == 1
    assert result_dict['Amazon Web Services'] == 3
    assert result_dict['Container Registry'] == 1

    # Total rows should be 6 (one per unique credential type)
    assert len(result) == 6


def test_credentials_anonymized_rollup_base():
    """Test base() method sums counts across batches and converts to JSON format."""
    # Simulate data from prepare() - already aggregated by credential_type
    prepared_data = pd.DataFrame(
        [
            {'credential_type': 'Machine', 'count': 2},
            {'credential_type': 'Vault', 'count': 1},
            {'credential_type': 'Source Control', 'count': 2},
            {'credential_type': 'Network', 'count': 1},
            {'credential_type': 'Amazon Web Services', 'count': 3},
            {'credential_type': 'Container Registry', 'count': 1},
        ]
    )

    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(prepared_data)

    # Result should have 'json' and 'rollup' keys
    assert 'json' in result
    assert 'rollup' in result

    # Check JSON output format
    json_data = result['json']
    assert isinstance(json_data, dict)

    # Check field name conversion (spaces to underscores, lowercase)
    assert 'credential_type_machine_total' in json_data
    assert json_data['credential_type_machine_total'] == 2

    assert 'credential_type_vault_total' in json_data
    assert json_data['credential_type_vault_total'] == 1

    assert 'credential_type_source_control_total' in json_data
    assert json_data['credential_type_source_control_total'] == 2

    assert 'credential_type_network_total' in json_data
    assert json_data['credential_type_network_total'] == 1

    assert 'credential_type_amazon_web_services_total' in json_data
    assert json_data['credential_type_amazon_web_services_total'] == 3

    assert 'credential_type_container_registry_total' in json_data
    assert json_data['credential_type_container_registry_total'] == 1

    # Check rollup output
    rollup_data = result['rollup']
    assert 'aggregated' in rollup_data
    assert isinstance(rollup_data['aggregated'], pd.DataFrame)
    assert len(rollup_data['aggregated']) == 6


def test_credentials_anonymized_rollup_prepare_and_base():
    """Test full flow: prepare() followed by base()."""
    df = pd.DataFrame(credentials)
    credentials_rollup = CredentialsAnonymizedRollup()

    # Prepare the data
    prepared = credentials_rollup.prepare(df)

    # Base aggregation
    result = credentials_rollup.base(prepared)
    json_data = result['json']

    # Verify counts match
    assert json_data['credential_type_machine_total'] == 2
    assert json_data['credential_type_vault_total'] == 1
    assert json_data['credential_type_source_control_total'] == 2
    assert json_data['credential_type_network_total'] == 1
    assert json_data['credential_type_amazon_web_services_total'] == 3
    assert json_data['credential_type_container_registry_total'] == 1


def test_credentials_anonymized_rollup_multiple_batches():
    """Test that base() correctly sums counts from multiple batches."""
    # Simulate multiple batches from prepare()
    batch1 = pd.DataFrame(
        [
            {'credential_type': 'Machine', 'count': 2},
            {'credential_type': 'Vault', 'count': 1},
        ]
    )

    batch2 = pd.DataFrame(
        [
            {'credential_type': 'Machine', 'count': 3},  # Same type, different batch
            {'credential_type': 'Network', 'count': 1},
        ]
    )

    # Concatenate batches (as would happen in real processing)
    combined = pd.concat([batch1, batch2], ignore_index=True)

    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(combined)
    json_data = result['json']

    # Machine should be summed: 2 + 3 = 5
    assert json_data['credential_type_machine_total'] == 5
    assert json_data['credential_type_vault_total'] == 1
    assert json_data['credential_type_network_total'] == 1


def test_credentials_anonymized_rollup_prepare_empty_dataframe():
    """Test prepare() with empty dataframe."""
    df = pd.DataFrame()
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.prepare(df)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ['credential_type', 'count']


def test_credentials_anonymized_rollup_prepare_missing_column():
    """Test prepare() with missing credential_type column."""
    df = pd.DataFrame([{'job_id': 1, 'model': 'job'}])  # Missing credential_type
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.prepare(df)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ['credential_type', 'count']


def test_credentials_anonymized_rollup_base_none():
    """Test base() with None input (no data files)."""
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(None)

    assert 'json' in result
    assert 'rollup' in result
    assert result['json'] == {}
    assert result['rollup']['aggregated'].empty


def test_credentials_anonymized_rollup_base_empty_dataframe():
    """Test base() with empty dataframe."""
    df = pd.DataFrame()
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(df)

    assert 'json' in result
    assert 'rollup' in result
    assert result['json'] == {}
    assert isinstance(result['rollup']['aggregated'], pd.DataFrame)
    assert list(result['rollup']['aggregated'].columns) == ['credential_type', 'count']


def test_credentials_anonymized_rollup_base_missing_columns():
    """Test base() with missing required columns."""
    df = pd.DataFrame([{'some_column': 'value'}])  # Missing credential_type and count
    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(df)

    assert 'json' in result
    assert 'rollup' in result
    assert result['json'] == {}
    assert isinstance(result['rollup']['aggregated'], pd.DataFrame)
    assert list(result['rollup']['aggregated'].columns) == ['credential_type', 'count']


def test_credentials_anonymized_rollup_field_name_conversion():
    """Test that credential type names are correctly converted to field names."""
    # Test various name formats
    test_data = pd.DataFrame(
        [
            {'credential_type': 'Machine', 'count': 1},
            {'credential_type': 'Source Control', 'count': 1},  # Space
            {'credential_type': 'Amazon Web Services', 'count': 1},  # Multiple spaces
            {'credential_type': 'Container-Registry', 'count': 1},  # Hyphen
            {'credential_type': 'My-Custom Type', 'count': 1},  # Hyphen and space
            {'credential_type': 'UPPERCASE', 'count': 1},  # Uppercase
        ]
    )

    credentials_rollup = CredentialsAnonymizedRollup()
    result = credentials_rollup.base(test_data)
    json_data = result['json']

    # Check field name conversions
    assert 'credential_type_machine_total' in json_data
    assert 'credential_type_source_control_total' in json_data
    assert 'credential_type_amazon_web_services_total' in json_data
    assert 'credential_type_container_registry_total' in json_data
    assert 'credential_type_my_custom_type_total' in json_data
    assert 'credential_type_uppercase_total' in json_data

    # All should have count of 1
    for key in json_data:
        assert json_data[key] == 1
