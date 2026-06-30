import pandas as pd

from metrics_utility.anonymized_rollups.indirect_managed_nodes_anonymized_rollup import (
    IndirectManagedNodesAnonymizedRollup,
)


def test_prepare_extracts_unique_host_ids():
    """Test that prepare() extracts and deduplicates host_remote_ids."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1, 2, 3],
            'host_name': ['host1', 'host2', 'host3'],
            'host_remote_id': ['remote1', 'remote2', 'remote3'],
        }
    )

    result = rollup.prepare(data)

    assert isinstance(result, dict)
    assert result['indirect_node_ids'] == ['remote1', 'remote2', 'remote3']
    assert result['indirect_nodes_total'] == 3


def test_prepare_handles_empty_dataframe():
    """Test that prepare() handles empty DataFrames gracefully."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    empty_data = pd.DataFrame()

    result = rollup.prepare(empty_data)

    assert result == {'indirect_node_ids': [], 'indirect_nodes_total': 0}


def test_prepare_deduplicates_host_ids():
    """Test that prepare() deduplicates duplicate host_remote_ids."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1, 2, 3, 4],
            'host_name': ['host1', 'host2', 'host1', 'host3'],
            'host_remote_id': ['remote1', 'remote2', 'remote1', 'remote3'],
        }
    )

    result = rollup.prepare(data)

    assert result['indirect_node_ids'] == ['remote1', 'remote2', 'remote3']
    assert result['indirect_nodes_total'] == 3


def test_prepare_handles_missing_host_remote_id():
    """Test that prepare() handles DataFrames without host_remote_id column."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        {
            'id': [1, 2],
            'host_name': ['host1', 'host2'],
        }
    )

    result = rollup.prepare(data)

    assert result['indirect_node_ids'] == []
    assert result['indirect_nodes_total'] == 0


def test_merge_returns_data_new():
    """Test that merge() passes through the snapshot batch unchanged."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data_new = {
        'indirect_node_ids': ['remote1', 'remote2'],
        'indirect_nodes_total': 2,
    }

    result = rollup.merge(None, data_new)

    assert result == data_new


def test_base_returns_count_only_no_ids():
    """Test that base() returns only the count, never host IDs."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'indirect_node_ids': ['remote1', 'remote2', 'remote3'],
        'indirect_nodes_total': 3,
    }

    result = rollup.base(data)

    assert result == {'json': {'indirect_nodes_total': 3}}
    assert 'indirect_node_ids' not in result['json']


def test_base_handles_none():
    """Test that base() returns zero count when no data."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    result = rollup.base(None)

    assert result == {'json': {'indirect_nodes_total': 0}}
    assert 'indirect_node_ids' not in result['json']


def test_rollup_name():
    """Test that rollup has correct name."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.rollup_name == 'indirect_managed_nodes'


def test_collector_names():
    """Test that collector_names is set correctly."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.collector_names == ['main_indirectmanagednodeaudit']
