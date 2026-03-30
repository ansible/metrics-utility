"""
Unit tests targeting uncovered branches in anonymized_rollups.py:
  - create_anonymized_object: the else/raise ValueError branch
  - anonymize_data: early-return guard (not a dict / falsy)
  - _calculate_host_summary_totals: empty host_ids branch
  - _inject_controller_version: empty list early return
"""

import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import (
    _calculate_host_summary_totals,
    _inject_controller_version,
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
    result = anonymize_data(None, 'salt')
    assert result is None


def test_anonymize_data_non_dict_returns_none():
    """anonymize_data should return immediately when data is not a dict."""
    result = anonymize_data(['not', 'a', 'dict'], 'salt')
    assert result is None


def test_anonymize_data_empty_dict_does_not_raise():
    """anonymize_data with an empty dict should be a no-op."""
    data = {}
    anonymize_data(data, 'salt')
    assert data == {}


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
