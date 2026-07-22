"""
Unit tests targeting uncovered branches in anonymized_rollups.py:
  - create_anonymized_object: the else/raise ValueError branch
  - anonymize_data: early-return guard (not a dict / falsy)
  - _calculate_host_summary_totals: empty host_ids branch
  - _inject_controller_version: empty list early return
"""

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import (
    _calculate_host_summary_totals,
    _inject_controller_version,
    _installed_collection_name_is_unknown,
    anonymize_data,
    create_anonymized_object,
)
from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup
from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup
from metrics_utility.anonymized_rollups.task_executions_anonymized_rollup import TaskExecutionsAnonymizedRollup


# ---------------------------------------------------------------------------
# create_anonymized_object
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'name, expected_type',
    [
        ('jobs', JobsAnonymizedRollup),
        ('job_host_summary', JobHostSummaryAnonymizedRollup),
        ('events_modules', EventModulesAnonymizedRollup),
        ('execution_environments', ExecutionEnvironmentsAnonymizedRollup),
        ('credentials', CredentialsAnonymizedRollup),
        ('table_metadata', TableMetadataAnonymizedRollup),
        ('controller_version', ControllerVersionAnonymizedRollup),
        ('feature_flags', FeatureFlagsAnonymizedRollup),
        ('task_executions', TaskExecutionsAnonymizedRollup),
    ],
)
def test_create_anonymized_object_known_names(name, expected_type):
    obj = create_anonymized_object(name)
    assert isinstance(obj, expected_type)


def test_create_anonymized_object_unknown_name_raises():
    with pytest.raises(ValueError, match='Invalid rollup name'):
        create_anonymized_object('nonexistent_rollup')


# ---------------------------------------------------------------------------
# anonymize_data – early-return guards
# ---------------------------------------------------------------------------


def test_anonymize_data_none_returns_none():
    """anonymize_data should return immediately (no error) when data is None."""
    result = anonymize_data(None)
    assert result is None


def test_anonymize_data_non_dict_returns_none():
    """anonymize_data should return immediately when data is not a dict."""
    result = anonymize_data(['not', 'a', 'dict'])
    assert result is None


def test_anonymize_data_empty_dict_does_not_raise():
    """anonymize_data with an empty dict should be a no-op."""
    data = {}
    anonymize_data(data)
    assert data == {}


@pytest.mark.parametrize(
    'collection_name, expected_unknown',
    [
        (None, True),
        ('', True),
        ('   ', True),
        (pd.NA, True),
        (float('nan'), True),
        ('ansible.posix', False),
        ('definitely_not_in_known_collections_zzz', True),
    ],
)
def test_installed_collection_name_is_unknown(collection_name, expected_unknown):
    known = {'ansible.posix': 'certified'}
    assert _installed_collection_name_is_unknown(collection_name, known) is expected_unknown


def test_anonymize_data_known_collection_unchanged():
    data = {
        'jobs_by_installed_collections_versions': [
            {
                'collection': 'ansible.posix',
                'version': '1.5.0',
                'jobs_total': 1,
            }
        ],
    }
    anonymize_data(data)
    assert data['jobs_by_installed_collections_versions'][0]['collection'] == 'ansible.posix'
    assert data['jobs_by_installed_collections_versions'][0]['version'] == '1.5.0'


def test_anonymize_data_unknown_installed_collection_removed():
    """Unknown/empty collection names should be removed, not renamed to 'Custom'."""
    data = {
        'jobs_by_installed_collections_versions': [
            {
                'collection': '',
                'version': '1.0.0',
                'jobs_total': 1,
            }
        ],
    }
    anonymize_data(data)
    assert data['jobs_by_installed_collections_versions'] == []


def test_anonymize_data_pd_na_collection_removed():
    """pd.NA collection name should be removed from jobs_by_installed_collections_versions."""
    data = {
        'jobs_by_installed_collections_versions': [
            {
                'collection': pd.NA,
                'version': '1.0.0',
                'jobs_total': 1,
            }
        ],
    }
    anonymize_data(data)
    assert data['jobs_by_installed_collections_versions'] == []


def test_anonymize_data_custom_module_stats_removed():
    """module_stats entries with collection_source == 'Custom' should be removed."""
    data = {
        'module_stats': [
            {'module': 'my_module', 'collection': 'my.col', 'collection_source': 'Custom', 'total': 5},
            {'module': 'ansible.builtin.copy', 'collection': 'ansible.builtin', 'collection_source': 'certified', 'total': 10},
        ],
    }
    anonymize_data(data)
    assert len(data['module_stats']) == 1
    assert data['module_stats'][0]['collection_source'] == 'certified'


def test_anonymize_data_custom_collection_stats_removed():
    """collection_stats entries with collection_source == 'Custom' should be removed."""
    data = {
        'collection_stats': [
            {'collection': 'my.col', 'collection_source': 'Custom', 'total': 3},
            {'collection': 'ansible.posix', 'collection_source': 'certified', 'total': 7},
        ],
    }
    anonymize_data(data)
    assert len(data['collection_stats']) == 1
    assert data['collection_stats'][0]['collection'] == 'ansible.posix'


def test_anonymize_data_custom_role_stats_removed():
    """role_stats entries with collection_source == 'Custom' should be removed."""
    data = {
        'role_stats': [
            {'role': 'my_role', 'collection': 'my.col', 'collection_source': 'Custom', 'total': 2},
            {'role': 'network', 'collection': 'redhat.rhel_system_roles', 'collection_source': 'certified', 'total': 8},
        ],
    }
    anonymize_data(data)
    assert len(data['role_stats']) == 1
    assert data['role_stats'][0]['collection_source'] == 'certified'


# ---------------------------------------------------------------------------
# _calculate_host_summary_totals – empty / None host_ids
# ---------------------------------------------------------------------------


def test_calculate_host_summary_totals_empty_host_ids():
    result = _calculate_host_summary_totals([], host_ids=[])
    assert result['unique_hosts_total'] == 0


def test_calculate_host_summary_totals_none_host_ids():
    result = _calculate_host_summary_totals([], host_ids=None)
    assert result['unique_hosts_total'] == 0


def test_calculate_host_summary_totals_with_host_ids():
    result = _calculate_host_summary_totals([], host_ids=[1, 2, 2, 3])
    # unique: {1, 2, 3}
    assert result['unique_hosts_total'] == 3


# ---------------------------------------------------------------------------
# _inject_controller_version – empty list early return
# ---------------------------------------------------------------------------


def test_inject_controller_version_empty_list_returns_empty():
    result = _inject_controller_version([], ['1.0.0'])
    assert result == []


def test_inject_controller_version_injects_first_version():
    jobs = [{'job_type': 'run'}]
    result = _inject_controller_version(jobs, ['2.5.0', '2.4.0'])
    assert result[0]['controller_version'] == '2.5.0'


def test_inject_controller_version_no_versions_injects_none():
    jobs = [{'job_type': 'run'}]
    result = _inject_controller_version(jobs, [])
    assert result[0]['controller_version'] is None


# ---------------------------------------------------------------------------
# anonymize_data – indirect_nodes_by_collection / indirect_nodes_by_module
# ---------------------------------------------------------------------------


def test_anonymize_data_indirect_nodes_by_collection_removes_private():
    """anonymize_data strips private collection names from indirect_nodes_by_collection."""
    data = {
        'indirect_nodes_by_collection': [
            {'collection': 'cisco.ios', 'host_count': 5},
            {'collection': 'acme.private_collection', 'host_count': 3},
        ],
    }
    anonymize_data(data)
    names = [e['collection'] for e in data['indirect_nodes_by_collection']]
    assert 'cisco.ios' in names
    assert 'acme.private_collection' not in names


def test_anonymize_data_indirect_nodes_by_collection_removes_no_collection_sentinel():
    """anonymize_data strips the _no_collection sentinel from indirect_nodes_by_collection."""
    data = {
        'indirect_nodes_by_collection': [
            {'collection': '_no_collection', 'host_count': 2},
        ],
    }
    anonymize_data(data)
    assert data['indirect_nodes_by_collection'] == []


def test_anonymize_data_indirect_nodes_by_module_removes_private():
    """anonymize_data strips modules whose collection prefix is private from indirect_nodes_by_module."""
    data = {
        'indirect_nodes_by_module': [
            {'module': 'cisco.ios.ios_command', 'host_count': 5},
            {'module': 'acme.private_collection.some_module', 'host_count': 3},
        ],
    }
    anonymize_data(data)
    names = [e['module'] for e in data['indirect_nodes_by_module']]
    assert 'cisco.ios.ios_command' in names
    assert 'acme.private_collection.some_module' not in names


def test_anonymize_data_indirect_nodes_by_module_removes_no_module_sentinel():
    """anonymize_data strips the _no_module sentinel from indirect_nodes_by_module."""
    data = {
        'indirect_nodes_by_module': [
            {'module': '_no_module', 'host_count': 2},
        ],
    }
    anonymize_data(data)
    assert data['indirect_nodes_by_module'] == []
