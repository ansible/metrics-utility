"""Tests for the shared stats-merge helpers on BaseAnonymizedRollup."""

import copy

import pytest

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup


class _NumericRollup(BaseAnonymizedRollup):
    STATS_NUMERIC_COLUMNS = ['total', 'count']
    STATS_LIST_COLUMNS = ['items']


@pytest.fixture
def rollup():
    return _NumericRollup('test')


class TestMergeStatsJson:
    def test_returns_new_when_all_is_empty(self, rollup):
        stats_new = [{'group': 'a', 'total': 1}]
        assert rollup._merge_stats_json([], stats_new, 'group') == stats_new

    def test_returns_all_when_new_is_empty(self, rollup):
        stats_all = [{'group': 'a', 'total': 1}]
        assert rollup._merge_stats_json(stats_all, [], 'group') == stats_all

    def test_both_empty_returns_empty_list(self, rollup):
        assert rollup._merge_stats_json([], [], 'group') == []

    def test_sums_numeric_columns_and_unions_lists(self, rollup):
        stats_all = [{'group': 'a', 'total': 1, 'count': None, 'items': ['x', 'y']}]
        stats_new = [{'group': 'a', 'total': 2, 'count': 3, 'items': ['y', 'z']}]

        [merged] = rollup._merge_stats_json(stats_all, stats_new, 'group')

        assert merged == {'group': 'a', 'total': 3, 'count': 3, 'items': ['x', 'y', 'z']}

    def test_keeps_disjoint_groups(self, rollup):
        stats_all = [{'group': 'a', 'total': 1, 'items': ['x']}]
        stats_new = [{'group': 'b', 'total': 2, 'items': ['y']}]

        result = rollup._merge_stats_json(stats_all, stats_new, 'group')

        assert {item['group'] for item in result} == {'a', 'b'}

    def test_missing_values_and_non_lists_are_treated_as_empty(self, rollup):
        stats_all = [{'group': 'a', 'items': 'not-a-list'}]
        stats_new = [{'group': 'a', 'items': None}]

        [merged] = rollup._merge_stats_json(stats_all, stats_new, 'group')

        assert merged['total'] == 0
        assert merged['count'] == 0
        assert merged['items'] == []

    def test_does_not_mutate_inputs(self, rollup):
        stats_all = [{'group': 'a', 'total': 1, 'count': 1, 'items': ['x']}]
        stats_new = [{'group': 'a', 'total': 2, 'count': 2, 'items': ['y']}]
        original_all = copy.deepcopy(stats_all)
        original_new = copy.deepcopy(stats_new)

        result = rollup._merge_stats_json(stats_all, stats_new, 'group')

        assert stats_all == original_all
        assert stats_new == original_new
        assert result[0] is not stats_all[0]
        assert result[0] is not stats_new[0]

    def test_accepts_custom_column_overrides(self, rollup):
        stats_all = [{'group': 'a', 'total': 1, 'other': 10, 'items': ['x']}]
        stats_new = [{'group': 'a', 'total': 2, 'other': 5, 'items': ['y']}]

        [merged] = rollup._merge_stats_json(stats_all, stats_new, 'group', numeric_cols=['other'], list_cols=[])

        assert merged['other'] == 15
        assert merged['total'] == 1
        assert merged['items'] == ['x']


class TestMergeHelpers:
    def test_create_merged_item_returns_none_for_empty_inputs(self, rollup):
        assert rollup._create_merged_item({}, {}, ['total'], ['items']) is None

    def test_create_merged_item_copies_single_input(self, rollup):
        item = {'group': 'a', 'total': 1, 'items': ['x']}

        merged = rollup._create_merged_item(item, {}, ['total'], ['items'])

        assert merged == item
        assert merged is not item

    def test_merge_numeric_columns_treats_none_as_zero(self, rollup):
        merged = {}
        rollup._merge_numeric_columns(merged, {'total': None}, {'total': 5}, ['total'])
        assert merged == {'total': 5}

    def test_merge_list_columns_unions_sorts_and_ignores_non_lists(self, rollup):
        merged = {}
        rollup._merge_list_columns(merged, {'items': ['b', 'a']}, {'items': None}, ['items'])
        assert merged == {'items': ['a', 'b']}

    def test_base_hooks_are_noops(self):
        rollup = BaseAnonymizedRollup('test')
        item = {'value': 1}

        rollup._merge_extreme_columns(item, {'value': 3}, {'value': 5})
        rollup._recompute_totals(item)

        assert item == {'value': 1}


@pytest.mark.parametrize('cls', [JobsAnonymizedRollup, JobHostSummaryAnonymizedRollup])
def test_rollups_reuse_base_merge_helpers(cls):
    assert cls._merge_stats_json is BaseAnonymizedRollup._merge_stats_json
    assert cls._merge_numeric_columns is BaseAnonymizedRollup._merge_numeric_columns
    assert cls._merge_list_columns is BaseAnonymizedRollup._merge_list_columns


def test_job_host_summary_uses_default_column_sets():
    rollup = JobHostSummaryAnonymizedRollup()
    stats_all = [{'ansible_version': '2.9', 'ok_total': 1, 'launch_types': ['manual'], 'launch_type_total': 1}]
    stats_new = [{'ansible_version': '2.9', 'ok_total': 2, 'launch_types': ['job'], 'launch_type_total': 1}]

    [merged] = rollup._merge_stats_json(stats_all, stats_new, 'ansible_version')

    assert merged['ok_total'] == 3
    assert merged['launch_types'] == ['job', 'manual']
    assert merged['launch_type_total'] == 2
