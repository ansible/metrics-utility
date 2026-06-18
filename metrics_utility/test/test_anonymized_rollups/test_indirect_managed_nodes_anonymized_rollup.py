from datetime import UTC, datetime

import pandas as pd

from metrics_utility.anonymized_rollups.indirect_managed_nodes_anonymized_rollup import (
    IndirectManagedNodesAnonymizedRollup,
)
from metrics_utility.metric_utils import INDIRECT


def test_prepare_adds_managed_node_type_tag():
    """Test that prepare() adds managed_node_type = INDIRECT to all records."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1, 2, 3],
            'host_name': ['host1', 'host2', 'host3'],
            'host_remote_id': ['remote1', 'remote2', 'remote3'],
            'created': [datetime(2024, 1, 1, tzinfo=UTC)] * 3,
        }
    )

    result = rollup.prepare(data)

    assert isinstance(result, list)
    assert len(result) == 3
    assert all('managed_node_type' in record for record in result)
    assert all(record['managed_node_type'] == INDIRECT for record in result)


def test_prepare_handles_empty_dataframe():
    """Test that prepare() handles empty DataFrames gracefully."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    empty_data = pd.DataFrame()

    result = rollup.prepare(empty_data)

    assert result == {}


def test_prepare_converts_timestamps_to_iso():
    """Test that prepare() converts pandas Timestamp objects to ISO strings."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1],
            'created': [pd.Timestamp('2024-01-01 12:00:00', tz='UTC')],
        }
    )

    result = rollup.prepare(data)

    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0]['created'], str)
    assert result[0]['created'] == '2024-01-01T12:00:00+00:00'


def test_prepare_handles_nan_values():
    """Test that prepare() converts NaN values to None."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1],
            'host_name': ['host1'],
            'optional_field': [pd.NA],
        }
    )

    result = rollup.prepare(data)

    assert isinstance(result, list)
    assert result[0]['optional_field'] is None


def test_prepare_converts_id_columns_to_strings():
    """Test that _convert_id_columns_to_strings is called."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1, 2],
            'job_id': [100, 200],
            'host_id': [10, 20],
        }
    )

    result = rollup.prepare(data)

    assert isinstance(result, list)
    assert all(isinstance(record['id'], str) for record in result)
    assert all(isinstance(record['job_id'], str) for record in result)
    assert all(isinstance(record['host_id'], str) for record in result)


def test_rollup_name():
    """Test that rollup has correct name."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.rollup_name == 'indirect_managed_nodes'


def test_collector_names():
    """Test that collector_names is set correctly."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.collector_names == ['main_indirectmanagednodeaudit']
