"""Unit tests for metrics_utility/library/dataframes/main_jobevent.py."""

import math

import pandas as pd

from metrics_utility.library.dataframes.main_jobevent import DataframeMainJobevent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_events(**overrides):
    """Return a minimal one-row events DataFrame with sensible defaults."""
    base = {
        'task_action': ['ansible.builtin.command'],
        'host_name': ['host1'],
        'resolved_action': [None],
        'resolved_role': [None],
        'role': [None],
        'duration': [1.5],
        'job_remote_id': [42],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _make_config(install_uuid='uuid-test'):
    return {'install_uuid': install_uuid}


def _df():
    return DataframeMainJobevent()


# ---------------------------------------------------------------------------
# extract_collection_name
# ---------------------------------------------------------------------------


class TestExtractCollectionName:
    def test_fqcn_three_parts(self):
        assert DataframeMainJobevent.extract_collection_name('ns.col.module') == 'ns.col'

    def test_fqcn_four_parts(self):
        assert DataframeMainJobevent.extract_collection_name('ns.col.role.task') == 'ns.col'

    def test_builtin_no_dots(self):
        assert DataframeMainJobevent.extract_collection_name('command') is None

    def test_two_parts_is_not_fqcn(self):
        # namespace.module is NOT a collection FQCN (requires 3+ parts)
        assert DataframeMainJobevent.extract_collection_name('ns.module') is None

    def test_none_returns_none(self):
        assert DataframeMainJobevent.extract_collection_name(None) is None

    def test_nan_float_returns_none(self):
        # pandas 3.0 passes float('nan') for missing string columns
        assert DataframeMainJobevent.extract_collection_name(float('nan')) is None

    def test_integer_returns_none(self):
        assert DataframeMainJobevent.extract_collection_name(42) is None

    def test_empty_string_returns_none(self):
        assert DataframeMainJobevent.extract_collection_name('') is None

    def test_namespace_and_collection_preserved(self):
        result = DataframeMainJobevent.extract_collection_name('ansible.builtin.copy')
        assert result == 'ansible.builtin'

    def test_underscore_and_digits_in_name(self):
        result = DataframeMainJobevent.extract_collection_name('my_ns.col2.mod_3')
        assert result == 'my_ns.col2'


# ---------------------------------------------------------------------------
# extract_role_name
# ---------------------------------------------------------------------------


class TestExtractRoleName:
    def test_fqcn_three_parts(self):
        assert DataframeMainJobevent.extract_role_name('ns.col.role') == 'ns.col.role'

    def test_fqcn_four_parts_truncates_to_three(self):
        assert DataframeMainJobevent.extract_role_name('ns.col.role.task') == 'ns.col.role'

    def test_standalone_two_parts(self):
        assert DataframeMainJobevent.extract_role_name('ns.role') == 'ns.role'

    def test_single_word_returns_none(self):
        assert DataframeMainJobevent.extract_role_name('role') is None

    def test_none_returns_none(self):
        assert DataframeMainJobevent.extract_role_name(None) is None

    def test_nan_float_returns_none(self):
        # pandas 3.0 passes float('nan') for missing string columns
        assert DataframeMainJobevent.extract_role_name(float('nan')) is None

    def test_integer_returns_none(self):
        assert DataframeMainJobevent.extract_role_name(99) is None

    def test_empty_string_returns_none(self):
        assert DataframeMainJobevent.extract_role_name('') is None

    def test_underscore_parts(self):
        assert DataframeMainJobevent.extract_role_name('my_ns.my_col.my_role') == 'my_ns.my_col.my_role'


# ---------------------------------------------------------------------------
# static schema methods
# ---------------------------------------------------------------------------


class TestSchemaMethods:
    def test_unique_index_columns(self):
        assert DataframeMainJobevent.unique_index_columns() == [
            'host_name',
            'module_name',
            'collection_name',
            'role_name',
            'install_uuid',
            'job_remote_id',
        ]

    def test_data_columns(self):
        assert DataframeMainJobevent.data_columns() == ['task_runs', 'duration']

    def test_cast_types(self):
        ct = DataframeMainJobevent.cast_types()
        assert ct['duration'] == 'float64'
        assert ct['task_runs'] == 'int64'

    def test_operations_is_empty(self):
        assert DataframeMainJobevent.operations() == {}

    def test_tarball_names(self):
        assert 'main_jobevent.csv' in DataframeMainJobevent.TARBALL_NAMES
        assert 'config.json' in DataframeMainJobevent.TARBALL_NAMES


# ---------------------------------------------------------------------------
# prepare()
# ---------------------------------------------------------------------------


class TestPrepare:
    def test_returns_none_when_all_rows_filtered(self):
        events = _make_events(task_action=[None], host_name=[None])
        result = _df().prepare((events, _make_config()))
        assert result is None

    def test_filters_null_task_action(self):
        events = _make_events(
            task_action=['ansible.builtin.command', None],
            host_name=['host1', 'host2'],
            resolved_action=[None, None],
            resolved_role=[None, None],
            role=[None, None],
            duration=[1.0, 2.0],
            job_remote_id=[1, 2],
        )
        result = _df().prepare((events, _make_config()))
        assert len(result) == 1
        assert 'host1' in result['host_name'].values

    def test_filters_null_host_name(self):
        events = _make_events(
            task_action=['cmd', 'cmd'],
            host_name=['host1', None],
            resolved_action=[None, None],
            resolved_role=[None, None],
            role=[None, None],
            duration=[1.0, 2.0],
            job_remote_id=[1, 2],
        )
        result = _df().prepare((events, _make_config()))
        assert len(result) == 1

    def test_install_uuid_set_from_config(self):
        events = _make_events()
        result = _df().prepare((events, _make_config(install_uuid='my-uuid')))
        assert (result['install_uuid'] == 'my-uuid').all()

    def test_resolved_action_takes_priority_over_task_action(self):
        events = _make_events(task_action=['original.mod'], resolved_action=['resolved.ns.mod'])
        result = _df().prepare((events, _make_config()))
        assert result['module_name'].iloc[0] == 'resolved.ns.mod'

    def test_task_action_used_when_resolved_action_is_null(self):
        events = _make_events(task_action=['ns.col.mod'], resolved_action=[None])
        result = _df().prepare((events, _make_config()))
        assert result['module_name'].iloc[0] == 'ns.col.mod'

    def test_resolved_role_takes_priority_over_role(self):
        events = _make_events(role=['orig.role'], resolved_role=['ns.col.resolved_role'])
        result = _df().prepare((events, _make_config()))
        assert result['role_name'].iloc[0] == 'ns.col.resolved_role'

    def test_collection_name_derived_from_fqcn_module(self):
        events = _make_events(task_action=['ansible.builtin.copy'])
        result = _df().prepare((events, _make_config()))
        assert result['collection_name'].iloc[0] == 'ansible.builtin'

    def test_collection_name_falls_back_to_no_collection(self):
        events = _make_events(task_action=['command'])
        result = _df().prepare((events, _make_config()))
        assert result['collection_name'].iloc[0] == 'No collection used'

    def test_role_name_falls_back_to_no_role(self):
        events = _make_events(role=[None], resolved_role=[None])
        result = _df().prepare((events, _make_config()))
        assert result['role_name'].iloc[0] == 'No role used'

    def test_columns_renamed(self):
        events = _make_events()
        result = _df().prepare((events, _make_config()))
        assert 'module_name' in result.columns
        assert 'role_name' in result.columns
        assert 'task_action' not in result.columns
        assert 'role' not in result.columns

    def test_nan_module_name_rows_dropped(self):
        # prepare() filters out rows where module_name ends up null;
        # this happens when resolved_action is None and task_action is also None —
        # already caught by the notnull filter, so test via direct path:
        # resolved_action=None, task_action='valid' → kept
        events = _make_events(task_action=['valid.ns.mod'], resolved_action=[None])
        result = _df().prepare((events, _make_config()))
        assert len(result) == 1


# ---------------------------------------------------------------------------
# group()
# ---------------------------------------------------------------------------


class TestGroup:
    def _prepared(self, rows=1, duration=2.0):
        events = pd.DataFrame(
            {
                'host_name': ['host1'] * rows,
                'module_name': ['ansible.builtin.copy'] * rows,
                'collection_name': ['ansible.builtin'] * rows,
                'role_name': ['No role used'] * rows,
                'install_uuid': ['uuid-1'] * rows,
                'job_remote_id': [10] * rows,
                'duration': [duration] * rows,
            }
        )
        return events

    def test_produces_task_runs_count(self):
        df = self._prepared(rows=3)
        result = _df().group(df)
        result = result.reset_index()
        assert result['task_runs'].iloc[0] == 3

    def test_sums_duration(self):
        df = self._prepared(rows=2, duration=1.5)
        result = _df().group(df)
        result = result.reset_index()
        assert math.isclose(result['duration'].iloc[0], 3.0)

    def test_null_duration_filled_with_zero(self):
        df = self._prepared(rows=1)
        df['duration'] = None
        result = _df().group(df)
        result = result.reset_index()
        assert result['duration'].iloc[0] == 0.0

    def test_result_dtypes(self):
        df = self._prepared(rows=1)
        result = _df().group(df)
        result = result.reset_index()
        assert result['task_runs'].dtype == 'int64'
        assert result['duration'].dtype == 'float64'

    def test_groups_by_all_index_columns(self):
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2'],
                'module_name': ['ns.col.mod', 'ns.col.mod'],
                'collection_name': ['ns.col', 'ns.col'],
                'role_name': ['No role used', 'No role used'],
                'install_uuid': ['uuid', 'uuid'],
                'job_remote_id': [1, 1],
                'duration': [1.0, 2.0],
            }
        )
        result = _df().group(df)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# regroup()
# ---------------------------------------------------------------------------


class TestRegroup:
    def test_sums_task_runs_and_duration(self):
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host1'],
                'module_name': ['mod', 'mod'],
                'collection_name': ['ns.col', 'ns.col'],
                'role_name': ['No role used', 'No role used'],
                'install_uuid': ['uuid', 'uuid'],
                'job_remote_id': [1, 1],
                'task_runs': [3, 7],
                'duration': [1.0, 2.0],
            }
        )
        result = _df().regroup(df)
        result = result.reset_index()
        assert result['task_runs'].iloc[0] == 10
        assert math.isclose(result['duration'].iloc[0], 3.0)


# ---------------------------------------------------------------------------
# add_raw() / from_tarballs() integration
# ---------------------------------------------------------------------------


class TestAddRaw:
    def _batch(self, install_uuid='uid', n=1, module='ns.col.mod', duration=1.0):
        events = pd.DataFrame(
            {
                'task_action': [module] * n,
                'host_name': ['host1'] * n,
                'resolved_action': [None] * n,
                'resolved_role': [None] * n,
                'role': [None] * n,
                'duration': [duration] * n,
                'job_remote_id': [1] * n,
            }
        )
        config = {'install_uuid': install_uuid}
        return (events, config)

    def test_single_batch_populates_rollup(self):
        obj = _df()
        obj.add_raw(self._batch(n=2))
        assert obj.rollup is not None
        obj.rollup = obj.postprocess(obj.rollup)
        assert obj.rollup['task_runs'].iloc[0] == 2

    def test_two_batches_merge_correctly(self):
        obj = _df()
        obj.add_raw(self._batch(n=1, duration=1.0))
        obj.add_raw(self._batch(n=1, duration=2.0))
        obj.rollup = obj.postprocess(obj.rollup)
        assert obj.rollup['task_runs'].iloc[0] == 2
        assert math.isclose(obj.rollup['duration'].iloc[0], 3.0)

    def test_empty_batch_skipped(self):
        obj = _df()
        empty_events = pd.DataFrame(
            {
                'task_action': [None],
                'host_name': [None],
                'resolved_action': [None],
                'resolved_role': [None],
                'role': [None],
                'duration': [0.0],
                'job_remote_id': [1],
            }
        )
        obj.add_raw((empty_events, {'install_uuid': 'x'}))
        assert obj.rollup is None

    def test_from_tarballs_sets_rollup_to_none_on_no_data(self):
        obj = _df()
        empty = pd.DataFrame(
            {
                'task_action': [None],
                'host_name': [None],
                'resolved_action': [None],
                'resolved_role': [None],
                'role': [None],
                'duration': [0.0],
                'job_remote_id': [1],
            }
        )
        obj.from_tarballs([(empty, {'install_uuid': 'x'})])
        assert obj.rollup is None
