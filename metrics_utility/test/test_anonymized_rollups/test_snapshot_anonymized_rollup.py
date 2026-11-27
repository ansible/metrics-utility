"""Tests for the shared SnapshotAnonymizedRollup base class."""

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.base_anonymized_rollup import SnapshotAnonymizedRollup
from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup


EMPTY_EXECUTION_ENVIRONMENTS = {
    'execution_environments_total': 0,
    'execution_environments_default_total': 0,
    'execution_environments_custom_total': 0,
}

ROLLUP_CLASSES = [
    ControllerVersionAnonymizedRollup,
    FeatureFlagsAnonymizedRollup,
    TableMetadataAnonymizedRollup,
    ExecutionEnvironmentsAnonymizedRollup,
]

ROLLUPS = [
    pytest.param(ControllerVersionAnonymizedRollup, [], id='controller_version'),
    pytest.param(FeatureFlagsAnonymizedRollup, [], id='feature_flags'),
    pytest.param(TableMetadataAnonymizedRollup, {}, id='table_metadata'),
    pytest.param(ExecutionEnvironmentsAnonymizedRollup, EMPTY_EXECUTION_ENVIRONMENTS, id='execution_environments'),
]


@pytest.mark.parametrize('rollup_cls', ROLLUP_CLASSES)
def test_subclass_of_snapshot_rollup(rollup_cls):
    assert issubclass(rollup_cls, SnapshotAnonymizedRollup)


@pytest.mark.parametrize(('rollup_cls', 'empty'), ROLLUPS)
class TestMerge:
    def test_returns_new_data(self, rollup_cls, empty):
        rollup = rollup_cls()
        data_new = object()
        assert rollup.merge(object(), data_new) is data_new

    def test_returns_new_data_when_old_is_none(self, rollup_cls, empty):
        rollup = rollup_cls()
        data_new = object()
        assert rollup.merge(None, data_new) is data_new

    def test_does_not_mutate_inputs(self, rollup_cls, empty):
        rollup = rollup_cls()
        data_all = {'batch': 'old'}
        data_new = {'batch': 'new'}
        result = rollup.merge(data_all, data_new)
        assert result is data_new
        assert data_all == {'batch': 'old'}


@pytest.mark.parametrize(('rollup_cls', 'empty'), ROLLUPS)
class TestBase:
    def test_none_returns_empty_payload(self, rollup_cls, empty):
        assert rollup_cls().base(None) == {'json': empty}

    def test_none_returns_a_fresh_payload_each_call(self, rollup_cls, empty):
        rollup = rollup_cls()
        first = rollup.base(None)['json']
        second = rollup.base(None)['json']

        assert first == empty
        assert second == empty
        assert first is not second

        if isinstance(first, dict):
            first['mutated'] = True
        else:
            first.append('mutated')

        assert second == empty
        assert rollup.base(None)['json'] == empty

    def test_non_dataframe_data_is_returned_as_is(self, rollup_cls, empty):
        rollup = rollup_cls()
        data = {'custom': 'payload'} if isinstance(empty, dict) else ['custom']
        assert rollup.base(data) == {'json': data}


@pytest.mark.parametrize(
    ('rollup_cls', 'dataframe', 'expected'),
    [
        (
            ControllerVersionAnonymizedRollup,
            pd.DataFrame([{'controller_version': '4.5.0'}]),
            ['4.5.0'],
        ),
        (
            FeatureFlagsAnonymizedRollup,
            pd.DataFrame([{'name': 'my_flag'}]),
            ['my_flag'],
        ),
        (
            TableMetadataAnonymizedRollup,
            pd.DataFrame(
                [
                    {
                        'tablename': 'main_job',
                        'estimated_row_count': 10,
                        'total_size_bytes': 100,
                        'table_size_bytes': 60,
                        'indexes_size_bytes': 40,
                    }
                ]
            ),
            {
                'main_job_estimated_row_count': 10,
                'main_job_total_size_bytes': 100,
                'main_job_table_size_bytes': 60,
                'main_job_indexes_size_bytes': 40,
            },
        ),
        (
            ExecutionEnvironmentsAnonymizedRollup,
            pd.DataFrame([{'managed': 't'}, {'managed': 'f'}]),
            {
                'execution_environments_total': 2,
                'execution_environments_default_total': 1,
                'execution_environments_custom_total': 1,
            },
        ),
    ],
)
def test_dataframe_is_prepared_first(rollup_cls, dataframe, expected):
    assert rollup_cls().base(dataframe) == {'json': expected}


def test_base_class_empty_json_requires_implementation():
    with pytest.raises(NotImplementedError):
        SnapshotAnonymizedRollup('snapshot')._empty_json()
