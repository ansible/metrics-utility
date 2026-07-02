import json

import pandas as pd

from metrics_utility.anonymized_rollups.indirect_managed_nodes_anonymized_rollup import (
    IndirectManagedNodesAnonymizedRollup,
)


def _make_dataframe(rows):
    """Build a DataFrame from a list of dicts with sensible defaults."""
    defaults = {
        'id': None,
        'host_name': None,
        'host_remote_id': None,
        'organization_name': 'DefaultOrg',
        'events': '[]',
    }
    filled = []
    for i, row in enumerate(rows):
        r = {**defaults, 'id': i + 1, **row}
        filled.append(r)
    return pd.DataFrame(filled)


def test_prepare_groups_by_org_and_collection():
    """prepare() groups hosts by organization and collection name."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_command"]'},
            {'host_name': 'host2', 'organization_name': 'OrgA', 'events': '["azure.azcollection.azure_rm_vm"]'},
            {'host_name': 'host3', 'organization_name': 'OrgB', 'events': '["cisco.ios.ios_config"]'},
        ]
    )

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 3
    groups = result['groups']
    assert 'OrgA||cisco.ios' in groups
    assert 'OrgA||azure.azcollection' in groups
    assert 'OrgB||cisco.ios' in groups
    assert groups['OrgA||cisco.ios']['host_count'] == 1
    assert groups['OrgA||azure.azcollection']['host_count'] == 1
    assert groups['OrgB||cisco.ios']['host_count'] == 1


def test_prepare_handles_empty_dataframe():
    """prepare() handles empty DataFrames gracefully."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    result = rollup.prepare(pd.DataFrame())

    assert result == {'groups': {}, 'indirect_nodes_total': 0}


def test_prepare_deduplicates_hosts_within_group():
    """prepare() deduplicates hosts that appear multiple times in the same group."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_command"]'},
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_config"]'},
        ]
    )

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 1
    assert result['groups']['OrgA||cisco.ios']['host_count'] == 1
    assert result['groups']['OrgA||cisco.ios']['host_names'] == ['host1']


def test_prepare_handles_empty_events():
    """prepare() handles rows with empty or null events arrays."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '[]'},
            {'host_name': 'host2', 'organization_name': 'OrgA', 'events': None},
        ]
    )

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 2
    no_collection_key = 'OrgA||_no_collection'
    assert no_collection_key in result['groups']
    assert result['groups'][no_collection_key]['host_count'] == 2


def test_prepare_handles_multiple_collections_per_host():
    """A host with events from multiple collections appears in each group."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {
                'host_name': 'host1',
                'organization_name': 'OrgA',
                'events': json.dumps(['cisco.ios.ios_command', 'azure.azcollection.azure_rm_vm']),
            },
        ]
    )

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 1
    assert 'OrgA||cisco.ios' in result['groups']
    assert 'OrgA||azure.azcollection' in result['groups']
    assert result['groups']['OrgA||cisco.ios']['host_count'] == 1
    assert result['groups']['OrgA||azure.azcollection']['host_count'] == 1


def test_prepare_handles_events_as_list():
    """prepare() handles events column already parsed as a Python list."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {
                'host_name': 'host1',
                'organization_name': 'OrgA',
                'events': ['cisco.ios.ios_command'],
            },
        ]
    )

    result = rollup.prepare(data)

    assert 'OrgA||cisco.ios' in result['groups']
    assert result['groups']['OrgA||cisco.ios']['host_count'] == 1


def test_merge_unions_host_names():
    """merge() unions host name sets across two batches."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data_all = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host1', 'host2'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 2,
    }

    data_new = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host2', 'host3'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 2,
    }

    result = rollup.merge(data_all, data_new)

    assert result['groups']['OrgA||cisco.ios']['host_count'] == 3
    assert sorted(result['groups']['OrgA||cisco.ios']['host_names']) == ['host1', 'host2', 'host3']
    assert result['indirect_nodes_total'] == 3


def test_merge_with_none_data_all():
    """merge(None, data_new) returns data_new unchanged."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data_new = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host1'],
                'host_count': 1,
            },
        },
        'indirect_nodes_total': 1,
    }

    result = rollup.merge(None, data_new)

    assert result == data_new


def test_base_strips_pii():
    """base() output does not contain host_names or organization_name."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host1', 'host2'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 2,
    }

    result = rollup.base(data)

    assert 'json' in result
    for group in result['json']['by_collection']:
        assert 'host_names' not in group
        assert 'organization_name' not in group


def test_base_collapses_orgs_into_collection_totals():
    """base() merges org-level groups into collection-level totals."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host1'],
                'host_count': 1,
            },
            'OrgB||cisco.ios': {
                'organization_name': 'OrgB',
                'collection_name': 'cisco.ios',
                'host_names': ['host2'],
                'host_count': 1,
            },
            'OrgA||azure.azcollection': {
                'organization_name': 'OrgA',
                'collection_name': 'azure.azcollection',
                'host_names': ['host3'],
                'host_count': 1,
            },
        },
        'indirect_nodes_total': 3,
    }

    result = rollup.base(data)

    assert result['json']['indirect_nodes_total'] == 3
    by_c = result['json']['by_collection']
    assert len(by_c) == 2
    assert by_c[0]['collection_name'] == 'azure.azcollection'
    assert by_c[0]['host_count'] == 1
    assert by_c[1]['collection_name'] == 'cisco.ios'
    assert by_c[1]['host_count'] == 2


def test_base_deduplicates_hosts_across_orgs():
    """base() deduplicates hosts that appear under the same collection in different orgs."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host1', 'host2'],
                'host_count': 2,
            },
            'OrgB||cisco.ios': {
                'organization_name': 'OrgB',
                'collection_name': 'cisco.ios',
                'host_names': ['host2', 'host3'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 3,
    }

    result = rollup.base(data)

    cisco_group = result['json']['by_collection'][0]
    assert cisco_group['collection_name'] == 'cisco.ios'
    assert cisco_group['host_count'] == 3


def test_base_handles_none():
    """base(None) returns zero count and empty array."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    result = rollup.base(None)

    assert result == {'json': {'indirect_nodes_total': 0, 'by_collection': []}}


def test_rollup_name():
    """Rollup has correct name."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.rollup_name == 'indirect_managed_nodes'


def test_collector_names():
    """collector_names is set correctly."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.collector_names == ['main_indirectmanagednodeaudit']
