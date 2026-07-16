import json

import pandas as pd

from metrics_utility.anonymized_rollups.indirect_managed_nodes_anonymized_rollup import (
    IndirectManagedNodesAnonymizedRollup,
    _extract_collection_names,
    _extract_module_names,
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

    assert result == {'groups': {}, 'module_groups': {}, 'indirect_nodes_total': 0}


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
    output = result['json']
    assert 'groups' not in output
    assert 'host_names' not in output
    for group in output['by_collection']:
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

    assert result == {'json': {'indirect_nodes_total': 0, 'by_collection': [], 'by_module': []}}


def test_extract_collection_names_with_nan():
    """_extract_collection_names returns empty set for NaN (float) input."""
    assert _extract_collection_names(float('nan')) == set()


def test_extract_collection_names_with_invalid_json():
    """_extract_collection_names returns empty set for malformed JSON."""
    assert _extract_collection_names('not valid json') == set()


def test_extract_collection_names_with_unsupported_type():
    """_extract_collection_names returns empty set for unsupported types."""
    assert _extract_collection_names(42) == set()


def test_extract_collection_names_skips_non_fqcn():
    """_extract_collection_names skips entries that are not valid FQCNs."""
    result = _extract_collection_names('["cisco.ios.ios_command", "builtin_module"]')
    assert result == {'cisco.ios'}


def test_prepare_skips_nan_host_name():
    """prepare() skips rows where host_name is NaN."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_command"]'},
        ]
    )
    data.loc[len(data)] = {
        'id': 99,
        'host_name': float('nan'),
        'host_remote_id': None,
        'organization_name': 'OrgA',
        'events': '["cisco.ios.ios_config"]',
    }

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 1
    assert len(result['groups']) == 1


def test_prepare_defaults_missing_organization_name():
    """prepare() defaults to empty string when organization_name is missing."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = pd.DataFrame(
        [
            {
                'id': 1,
                'host_name': 'host1',
                'host_remote_id': None,
                'events': '["cisco.ios.ios_command"]',
            },
        ]
    )

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 1
    assert '||cisco.ios' in result['groups']


def test_merge_with_none_data_new():
    """merge(data_all, None) returns data_all unchanged."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data_all = {
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

    result = rollup.merge(data_all, None)

    assert result == data_all


def test_rollup_name():
    """Rollup has correct name."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.rollup_name == 'indirect_managed_nodes'


def test_collector_names():
    """collector_names is set correctly."""
    rollup = IndirectManagedNodesAnonymizedRollup()
    assert rollup.collector_names == ['main_indirectmanagednodeaudit']


# --- module-level tests ---


def test_extract_module_names_returns_full_fqcn():
    """_extract_module_names returns the full FQCN, not just the collection prefix."""
    result = _extract_module_names('["cisco.ios.ios_command", "azure.azcollection.azure_rm_vm"]')
    assert result == {'cisco.ios.ios_command', 'azure.azcollection.azure_rm_vm'}


def test_extract_module_names_skips_non_fqcn():
    """_extract_module_names skips entries without a valid namespace.collection prefix."""
    result = _extract_module_names('["cisco.ios.ios_command", "builtin_module"]')
    assert result == {'cisco.ios.ios_command'}


def test_extract_module_names_with_nan():
    """_extract_module_names returns empty set for NaN input."""
    assert _extract_module_names(float('nan')) == set()


def test_extract_module_names_with_invalid_json():
    """_extract_module_names returns empty set for malformed JSON."""
    assert _extract_module_names('not valid json') == set()


def test_prepare_groups_by_module():
    """prepare() populates module_groups keyed by org and full FQCN."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_command"]'},
            {'host_name': 'host2', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_config"]'},
            {'host_name': 'host3', 'organization_name': 'OrgB', 'events': '["cisco.ios.ios_command"]'},
        ]
    )

    result = rollup.prepare(data)

    module_groups = result['module_groups']
    assert 'OrgA||cisco.ios.ios_command' in module_groups
    assert 'OrgA||cisco.ios.ios_config' in module_groups
    assert 'OrgB||cisco.ios.ios_command' in module_groups
    assert module_groups['OrgA||cisco.ios.ios_command']['host_count'] == 1
    assert module_groups['OrgA||cisco.ios.ios_config']['host_count'] == 1
    assert module_groups['OrgB||cisco.ios.ios_command']['host_count'] == 1


def test_prepare_deduplicates_hosts_within_module_group():
    """prepare() deduplicates hosts within the same module group."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_command"]'},
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '["cisco.ios.ios_command"]'},
        ]
    )

    result = rollup.prepare(data)

    assert result['module_groups']['OrgA||cisco.ios.ios_command']['host_count'] == 1
    assert result['module_groups']['OrgA||cisco.ios.ios_command']['host_names'] == ['host1']


def test_prepare_host_appears_in_multiple_module_groups():
    """A host touched by multiple modules appears in each module group."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {
                'host_name': 'host1',
                'organization_name': 'OrgA',
                'events': json.dumps(['azure.azcollection.azure_rm_vm', 'azure.azcollection.azure_rm_network_interface']),
            },
        ]
    )

    result = rollup.prepare(data)

    assert result['indirect_nodes_total'] == 1
    assert 'OrgA||azure.azcollection.azure_rm_vm' in result['module_groups']
    assert 'OrgA||azure.azcollection.azure_rm_network_interface' in result['module_groups']
    assert result['module_groups']['OrgA||azure.azcollection.azure_rm_vm']['host_count'] == 1
    assert result['module_groups']['OrgA||azure.azcollection.azure_rm_network_interface']['host_count'] == 1


def test_prepare_no_events_uses_no_module_fallback():
    """prepare() buckets hosts with no events under _no_module in module_groups."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = _make_dataframe(
        [
            {'host_name': 'host1', 'organization_name': 'OrgA', 'events': '[]'},
        ]
    )

    result = rollup.prepare(data)

    assert 'OrgA||_no_module' in result['module_groups']
    assert result['module_groups']['OrgA||_no_module']['host_count'] == 1


def test_merge_unions_module_group_host_names():
    """merge() unions host name sets in module_groups across two batches."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data_all = {
        'groups': {},
        'module_groups': {
            'OrgA||cisco.ios.ios_command': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_command',
                'host_names': ['host1', 'host2'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 2,
    }

    data_new = {
        'groups': {},
        'module_groups': {
            'OrgA||cisco.ios.ios_command': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_command',
                'host_names': ['host2', 'host3'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 2,
    }

    result = rollup.merge(data_all, data_new)

    merged = result['module_groups']['OrgA||cisco.ios.ios_command']
    assert merged['host_count'] == 3
    assert sorted(merged['host_names']) == ['host1', 'host2', 'host3']


def test_base_produces_by_module():
    """base() outputs a by_module list stripped of PII."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {},
        'module_groups': {
            'OrgA||cisco.ios.ios_command': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_command',
                'host_names': ['host1', 'host2'],
                'host_count': 2,
            },
            'OrgA||cisco.ios.ios_config': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_config',
                'host_names': ['host3'],
                'host_count': 1,
            },
        },
        'indirect_nodes_total': 3,
    }

    result = rollup.base(data)

    by_module = result['json']['by_module']
    assert len(by_module) == 2
    assert by_module[0] == {'module_name': 'cisco.ios.ios_command', 'host_count': 2}
    assert by_module[1] == {'module_name': 'cisco.ios.ios_config', 'host_count': 1}
    for entry in by_module:
        assert 'host_names' not in entry
        assert 'organization_name' not in entry


def test_base_deduplicates_module_hosts_across_orgs():
    """base() deduplicates hosts under the same module across different orgs."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {},
        'module_groups': {
            'OrgA||cisco.ios.ios_command': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_command',
                'host_names': ['host1', 'host2'],
                'host_count': 2,
            },
            'OrgB||cisco.ios.ios_command': {
                'organization_name': 'OrgB',
                'module_name': 'cisco.ios.ios_command',
                'host_names': ['host2', 'host3'],
                'host_count': 2,
            },
        },
        'indirect_nodes_total': 3,
    }

    result = rollup.base(data)

    by_module = result['json']['by_module']
    assert len(by_module) == 1
    assert by_module[0]['module_name'] == 'cisco.ios.ios_command'
    assert by_module[0]['host_count'] == 3


def test_base_by_module_sorted_by_module_name():
    """base() sorts by_module entries by module name."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {},
        'module_groups': {
            'OrgA||cisco.ios.ios_config': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_config',
                'host_names': ['host1'],
                'host_count': 1,
            },
            'OrgA||azure.azcollection.azure_rm_vm': {
                'organization_name': 'OrgA',
                'module_name': 'azure.azcollection.azure_rm_vm',
                'host_names': ['host2'],
                'host_count': 1,
            },
        },
        'indirect_nodes_total': 2,
    }

    result = rollup.base(data)

    names = [e['module_name'] for e in result['json']['by_module']]
    assert names == sorted(names)


def test_base_by_collection_unchanged_with_module_data():
    """Adding module_groups does not affect by_collection output."""
    rollup = IndirectManagedNodesAnonymizedRollup()

    data = {
        'groups': {
            'OrgA||cisco.ios': {
                'organization_name': 'OrgA',
                'collection_name': 'cisco.ios',
                'host_names': ['host1'],
                'host_count': 1,
            },
        },
        'module_groups': {
            'OrgA||cisco.ios.ios_command': {
                'organization_name': 'OrgA',
                'module_name': 'cisco.ios.ios_command',
                'host_names': ['host1'],
                'host_count': 1,
            },
        },
        'indirect_nodes_total': 1,
    }

    result = rollup.base(data)

    assert result['json']['by_collection'] == [{'collection_name': 'cisco.ios', 'host_count': 1}]
    assert result['json']['by_module'] == [{'module_name': 'cisco.ios.ios_command', 'host_count': 1}]
