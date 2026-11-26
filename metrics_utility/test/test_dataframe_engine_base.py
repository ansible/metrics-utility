"""Unit tests for automation_controller_billing/dataframe_engine/base.py utility functions."""

import datetime

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dataframe_engine.base import (
    Base,
    combine_json,
    combine_json_values,
    combine_set,
    granularity_cast,
    list_dates,
    merge_setdicts,
    merge_sets,
)


# ---------------------------------------------------------------------------
# granularity_cast
# ---------------------------------------------------------------------------


class TestGranularityCast:
    def test_daily_returns_date_unchanged(self):
        d = datetime.date(2024, 3, 15)
        assert granularity_cast(d, 'daily') == datetime.date(2024, 3, 15)

    def test_monthly_resets_day_to_1(self):
        d = datetime.date(2024, 3, 15)
        assert granularity_cast(d, 'monthly') == datetime.date(2024, 3, 1)

    def test_monthly_already_first_day_unchanged(self):
        d = datetime.date(2024, 3, 1)
        assert granularity_cast(d, 'monthly') == datetime.date(2024, 3, 1)

    def test_yearly_resets_month_and_day_to_1(self):
        d = datetime.date(2024, 7, 20)
        assert granularity_cast(d, 'yearly') == datetime.date(2024, 1, 1)

    def test_yearly_already_jan_1_unchanged(self):
        d = datetime.date(2024, 1, 1)
        assert granularity_cast(d, 'yearly') == datetime.date(2024, 1, 1)

    def test_unknown_granularity_returns_date_unchanged(self):
        d = datetime.date(2024, 5, 10)
        assert granularity_cast(d, 'weekly') == datetime.date(2024, 5, 10)

    def test_works_with_datetime_object(self):
        dt = datetime.datetime(2024, 6, 15, 12, 30)
        result = granularity_cast(dt, 'monthly')
        assert result.day == 1
        assert result.month == 6


# ---------------------------------------------------------------------------
# list_dates
# ---------------------------------------------------------------------------


class TestListDates:
    def test_daily_single_day(self):
        d = datetime.date(2024, 1, 1)
        result = list_dates(d, d, 'daily')
        assert result == [datetime.date(2024, 1, 1)]

    def test_daily_range_includes_both_endpoints(self):
        start = datetime.date(2024, 1, 1)
        end = datetime.date(2024, 1, 3)
        result = list_dates(start, end, 'daily')
        assert result == [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
        ]

    def test_monthly_range_spans_three_months(self):
        start = datetime.date(2024, 1, 15)
        end = datetime.date(2024, 3, 20)
        result = list_dates(start, end, 'monthly')
        # granularity_cast truncates to month start
        assert result == [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 2, 1),
            datetime.date(2024, 3, 1),
        ]

    def test_yearly_range_spans_three_years(self):
        start = datetime.date(2022, 6, 1)
        end = datetime.date(2024, 6, 1)
        result = list_dates(start, end, 'yearly')
        assert result == [
            datetime.date(2022, 1, 1),
            datetime.date(2023, 1, 1),
            datetime.date(2024, 1, 1),
        ]

    def test_monthly_same_month_returns_single_entry(self):
        start = datetime.date(2024, 5, 5)
        end = datetime.date(2024, 5, 28)
        result = list_dates(start, end, 'monthly')
        assert result == [datetime.date(2024, 5, 1)]

    def test_daily_returns_correct_count(self):
        start = datetime.date(2024, 1, 1)
        end = datetime.date(2024, 1, 31)
        result = list_dates(start, end, 'daily')
        assert len(result) == 31


# ---------------------------------------------------------------------------
# combine_json
# ---------------------------------------------------------------------------


class TestCombineJson:
    def test_merges_two_dicts(self):
        assert combine_json({'a': 1}, {'b': 2}) == {'a': 1, 'b': 2}

    def test_second_dict_overwrites_first(self):
        assert combine_json({'a': 1}, {'a': 99}) == {'a': 99}

    def test_none_first_treated_as_empty(self):
        assert combine_json(None, {'b': 2}) == {'b': 2}

    def test_none_second_treated_as_empty(self):
        assert combine_json({'a': 1}, None) == {'a': 1}

    def test_both_none_returns_empty_dict(self):
        assert combine_json(None, None) == {}

    def test_non_dict_first_treated_as_empty(self):
        assert combine_json('not a dict', {'b': 2}) == {'b': 2}

    def test_non_dict_second_treated_as_empty(self):
        assert combine_json({'a': 1}, 42) == {'a': 1}

    def test_empty_dicts(self):
        assert combine_json({}, {}) == {}


# ---------------------------------------------------------------------------
# combine_set
# ---------------------------------------------------------------------------


class TestCombineSet:
    def test_union_of_two_sets(self):
        assert combine_set({1, 2}, {3, 4}) == {1, 2, 3, 4}

    def test_overlapping_sets_deduplicates(self):
        assert combine_set({1, 2, 3}, {2, 3, 4}) == {1, 2, 3, 4}

    def test_list_is_converted_to_set(self):
        assert combine_set([1, 2], [3, 4]) == {1, 2, 3, 4}

    def test_mixed_set_and_list(self):
        assert combine_set({1, 2}, [3, 4]) == {1, 2, 3, 4}

    def test_non_set_non_list_first_treated_as_empty(self):
        assert combine_set(None, {1, 2}) == {1, 2}

    def test_non_set_non_list_second_treated_as_empty(self):
        assert combine_set({1, 2}, None) == {1, 2}

    def test_both_none_returns_empty_set(self):
        assert combine_set(None, None) == set()

    def test_empty_inputs(self):
        assert combine_set(set(), set()) == set()

    def test_two_sets_returns_new_set_and_does_not_mutate_inputs(self):
        left = {1, 2}
        right = {2, 3}
        result = combine_set(left, right)
        assert result == {1, 2, 3}
        assert result is not left and result is not right
        result.add(4)
        assert left == {1, 2}
        assert right == {2, 3}

    def test_two_sets_same_object_does_not_mutate(self):
        left = {1, 2}
        result = combine_set(left, left)
        assert result == {1, 2}
        assert result is not left

    def test_set_subclass_is_handled_without_mutation(self):
        class MySet(set):
            pass

        left = MySet({1, 2})
        right = MySet({2, 3})
        result = combine_set(left, right)
        assert result == {1, 2, 3}
        assert left == {1, 2}
        assert right == {2, 3}

    def test_lists_are_not_mutated(self):
        left = [1, 2, 2]
        right = [2, 3]
        result = combine_set(left, right)
        assert result == {1, 2, 3}
        assert left == [1, 2, 2]
        assert right == [2, 3]

    def test_nan_and_none_treated_as_empty(self):
        assert combine_set(float('nan'), float('nan')) == set()
        assert combine_set(None, float('nan')) == set()
        assert combine_set(float('nan'), {1}) == {1}


# ---------------------------------------------------------------------------
# merge_sets
# ---------------------------------------------------------------------------


class TestMergeSets:
    def test_merges_list_of_sets(self):
        result = merge_sets([{1, 2}, {3, 4}, {5}])
        assert result == {1, 2, 3, 4, 5}

    def test_deduplicates_across_sets(self):
        result = merge_sets([{1, 2}, {2, 3}])
        assert result == {1, 2, 3}

    def test_empty_list_returns_empty_set(self):
        assert merge_sets([]) == set()

    def test_single_set(self):
        assert merge_sets([{7, 8}]) == {7, 8}


# ---------------------------------------------------------------------------
# combine_json_values
# ---------------------------------------------------------------------------


class TestCombineJsonValues:
    def test_merges_scalar_values_into_sets(self):
        result = combine_json_values({'a': 'x'}, {'a': 'y'})
        assert result == {'a': {'x', 'y'}}

    def test_none_values_are_filtered_out(self):
        result = combine_json_values({'a': None}, {'a': 'y'})
        assert result == {'a': {'y'}}

    def test_empty_string_values_are_filtered_out(self):
        result = combine_json_values({'a': ''}, {'a': 'y'})
        assert result == {'a': {'y'}}

    def test_set_values_are_unioned(self):
        result = combine_json_values({'a': {'x'}}, {'a': {'y'}})
        assert result == {'a': {'x', 'y'}}

    def test_disjoint_keys_are_merged(self):
        result = combine_json_values({'a': '1'}, {'b': '2'})
        assert result == {'a': {'1'}, 'b': {'2'}}

    def test_non_dict_first_treated_as_empty(self):
        result = combine_json_values(None, {'a': 'x'})
        assert result == {'a': {'x'}}

    def test_non_dict_second_treated_as_empty(self):
        result = combine_json_values({'a': 'x'}, None)
        assert result == {'a': {'x'}}

    def test_both_empty_dicts_returns_empty(self):
        assert combine_json_values({}, {}) == {}

    def test_duplicate_scalar_stored_once(self):
        result = combine_json_values({'a': 'same'}, {'a': 'same'})
        assert result == {'a': {'same'}}

    def test_nested_dict_values_are_hashable(self):
        """Nested dict facts must not raise TypeError when combined (AAP-101848)."""
        from metrics_utility.automation_controller_billing.helpers import HashableDict

        result = combine_json_values(
            {'tags': {'Name': 'test-instance', 'Env': 'prod'}},
            {'tags': {'Name': 'test-instance', 'Env': 'prod'}},
        )
        assert len(result['tags']) == 1
        tags_value = next(iter(result['tags']))
        assert isinstance(tags_value, HashableDict)
        assert tags_value == {'Name': 'test-instance', 'Env': 'prod'}

    def test_nested_tuple_frozenset_and_list_values(self):
        result = combine_json_values(
            {'t': (1, 2), 'f': frozenset({'a'}), 'l': [1, 2]},
            {'t': (1, 2), 'f': frozenset({'b'}), 'l': [1, 2]},
        )
        assert result == {
            't': {(1, 2)},
            'f': {frozenset({'a'}), frozenset({'b'})},
            'l': {(1, 2)},
        }

    def test_deeply_nested_values_are_hashable(self):
        from metrics_utility.automation_controller_billing.helpers import HashableDict

        result = combine_json_values(
            {'meta': {'a': [{'b': 1}]}},
            {'meta': {'a': [{'b': 1}]}},
        )
        assert len(result['meta']) == 1
        (value,) = result['meta']
        assert isinstance(value, HashableDict)

    def test_na_value_is_preserved(self):
        """Unlike merge_json_sets, combine_json_values does not filter 'NA'."""
        result = combine_json_values({'serial': 'NA'}, {'serial': 'abc'})
        assert result == {'serial': {'NA', 'abc'}}

    def test_input_sets_are_not_mutated(self):
        first = {'x', 'y'}
        second = {'y', 'z'}
        result = combine_json_values({'a': first}, {'a': second})
        assert result == {'a': {'x', 'y', 'z'}}
        assert first == {'x', 'y'}
        assert second == {'y', 'z'}
        assert result['a'] is not first and result['a'] is not second

    def test_single_set_value_is_copied(self):
        original = {'x'}
        result = combine_json_values({'a': original}, {})
        # Short-circuit treats an empty second dict as no input, but the set is still copied.
        assert result == {'a': {'x'}}
        assert result['a'] is not original
        result['a'].add('y')
        assert original == {'x'}

    def test_input_dicts_are_not_mutated(self):
        first = {'a': 'x', 'nested': {'k': 1}}
        second = {'a': 'y'}
        combine_json_values(first, second)
        assert first == {'a': 'x', 'nested': {'k': 1}}
        assert second == {'a': 'y'}

    def test_scalar_types_are_preserved(self):
        result = combine_json_values({'i': 1, 'f': 1.5, 'b': True, 's': 'x'}, {})
        assert result == {'i': {1}, 'f': {1.5}, 'b': {True}, 's': {'x'}}

    @pytest.mark.parametrize('empty1', [None, {}, float('nan'), pd.NA])
    @pytest.mark.parametrize('empty2', [None, {}, float('nan'), pd.NA])
    def test_empty_or_missing_inputs_short_circuit(self, empty1, empty2):
        assert combine_json_values(empty1, empty2) == {}


# ---------------------------------------------------------------------------
# merge_setdicts
# ---------------------------------------------------------------------------


class TestMergeSetdicts:
    def test_reduces_list_of_dicts(self):
        dicts = [{'a': {'x'}}, {'a': {'y'}}, {'b': {'z'}}]
        result = merge_setdicts(dicts)
        assert result == {'a': {'x', 'y'}, 'b': {'z'}}

    def test_single_dict_unchanged(self):
        dicts = [{'a': {'x'}}]
        result = merge_setdicts(dicts)
        assert result == {'a': {'x'}}

    def test_empty_list_returns_empty_dict(self):
        result = merge_setdicts([])
        assert result == {}


# ---------------------------------------------------------------------------
# Base.dates()
# ---------------------------------------------------------------------------


class ConcreteBase(Base):
    """Minimal concrete subclass to make Base instantiable for testing."""

    @staticmethod
    def unique_index_columns():
        return ['host_name']

    @staticmethod
    def data_columns():
        return ['count']

    @staticmethod
    def cast_types():
        return {'count': 'int64'}

    @staticmethod
    def operations():
        return {}


class TestBaseDates:
    def _make_base(self, extra_params):
        return ConcreteBase(extractor=None, month=datetime.date(2024, 3, 1), extra_params=extra_params)

    def test_defaults_to_full_calendar_month(self):
        base = self._make_base({})
        dates = base.dates()
        assert dates[0] == datetime.date(2024, 3, 1)
        assert dates[-1] == datetime.date(2024, 3, 31)
        assert len(dates) == 31

    def test_uses_since_date_and_until_date_when_provided(self):
        extra_params = {
            'since_date': datetime.date(2024, 3, 5),
            'until_date': datetime.date(2024, 3, 7),
        }
        base = self._make_base(extra_params)
        dates = base.dates()
        assert dates == [
            datetime.date(2024, 3, 5),
            datetime.date(2024, 3, 6),
            datetime.date(2024, 3, 7),
        ]

    def test_single_day_range(self):
        extra_params = {
            'since_date': datetime.date(2024, 6, 15),
            'until_date': datetime.date(2024, 6, 15),
        }
        base = self._make_base(extra_params)
        dates = base.dates()
        assert dates == [datetime.date(2024, 6, 15)]

    def test_month_boundary_february_leap_year(self):
        base = ConcreteBase(extractor=None, month=datetime.date(2024, 2, 1), extra_params={})
        dates = base.dates()
        assert dates[0] == datetime.date(2024, 2, 1)
        assert dates[-1] == datetime.date(2024, 2, 29)
        assert len(dates) == 29


# ---------------------------------------------------------------------------
# Base.summarize_merged_dataframes()
# ---------------------------------------------------------------------------


class TestBaseSummarizeMergedDataframes:
    def _make_base(self):
        return ConcreteBase(extractor=None, month=datetime.date(2024, 1, 1), extra_params={})

    def _make_merged_df(self, x_vals, y_vals):
        """Build a simple merged DataFrame with _x/_y suffix columns."""
        return pd.DataFrame({'count_x': x_vals, 'count_y': y_vals})

    def test_default_operation_sums_columns(self):
        base = self._make_base()
        df = self._make_merged_df([1.0, 2.0], [3.0, 4.0])
        result = base.summarize_merged_dataframes(df, ['count'])
        assert list(result['count']) == [4.0, 6.0]
        assert 'count_x' not in result.columns
        assert 'count_y' not in result.columns

    def test_min_operation(self):
        base = self._make_base()
        df = pd.DataFrame({'val_x': [10.0, 5.0], 'val_y': [3.0, 8.0]})
        result = base.summarize_merged_dataframes(df, ['val'], operations={'val': 'min'})
        assert list(result['val']) == [3.0, 5.0]

    def test_max_operation(self):
        base = self._make_base()
        df = pd.DataFrame({'val_x': [10.0, 5.0], 'val_y': [3.0, 8.0]})
        result = base.summarize_merged_dataframes(df, ['val'], operations={'val': 'max'})
        assert list(result['val']) == [10.0, 8.0]

    def test_combine_set_operation(self):
        base = self._make_base()
        df = pd.DataFrame({'tags_x': [{1, 2}, {3}], 'tags_y': [{2, 3}, {4}]})
        result = base.summarize_merged_dataframes(df, ['tags'], operations={'tags': 'combine_set'})
        assert result['tags'][0] == {1, 2, 3}
        assert result['tags'][1] == {3, 4}

    def test_combine_set_with_lists_missing_values_and_duplicate_index(self):
        base = self._make_base()
        df = pd.DataFrame(
            {
                'tags_x': [{'a', 'b'}, ['c', 'c'], None, pd.NA],
                'tags_y': [['b', 'd'], None, {'e'}, float('nan')],
            },
            index=[2, 1, 2, 1],
        )

        result = base.summarize_merged_dataframes(df, ['tags'], operations={'tags': 'combine_set'})

        expected = pd.DataFrame({'tags': [{'a', 'b', 'd'}, {'c'}, {'e'}, set()]}, index=[2, 1, 2, 1])
        pd.testing.assert_frame_equal(result, expected)

    def test_combine_set_empty_dataframe(self):
        base = self._make_base()
        df = pd.DataFrame({'tags_x': pd.Series(dtype=object), 'tags_y': pd.Series(dtype=object)})

        result = base.summarize_merged_dataframes(df, ['tags'], operations={'tags': 'combine_set'})

        assert result.empty
        assert list(result.columns) == ['tags']

    @pytest.mark.parametrize('missing', ['tags_x', 'tags_y'])
    def test_combine_set_missing_source_column_raises(self, missing):
        base = self._make_base()
        df = pd.DataFrame({'tags_x': [{'left'}], 'tags_y': [{'right'}]}).drop(columns=missing)

        with pytest.raises(KeyError, match=missing):
            base.summarize_merged_dataframes(df, ['tags'], operations={'tags': 'combine_set'})

    def test_combine_set_across_outer_merged_batches(self):
        class SetBase(ConcreteBase):
            @staticmethod
            def data_columns():
                return ['tags']

            @staticmethod
            def cast_types():
                return {'tags': object}

            @staticmethod
            def operations():
                return {'tags': 'combine_set'}

        base = SetBase(extractor=None, month=datetime.date(2024, 1, 1), extra_params={})
        batches = [
            pd.DataFrame({'host_name': ['a', 'b'], 'tags': [{'one'}, {'two'}]}).set_index('host_name'),
            pd.DataFrame({'host_name': ['a', 'c'], 'tags': [{'three'}, {'four'}]}).set_index('host_name'),
            pd.DataFrame({'host_name': ['a', 'b'], 'tags': [{'one'}, ['five']]}).set_index('host_name'),
        ]
        result = None
        for batch in batches:
            result = base.merge(result, batch)

        expected = pd.DataFrame(
            {'tags': [{'one', 'three'}, {'two', 'five'}, {'four'}]},
            index=pd.Index(['a', 'b', 'c'], name='host_name', dtype=object),
        )
        pd.testing.assert_frame_equal(result.sort_index(), expected)

    def test_combine_json_operation(self):
        base = self._make_base()
        df = pd.DataFrame({'meta_x': [{'a': 1}, None], 'meta_y': [{'b': 2}, {'c': 3}]})
        result = base.summarize_merged_dataframes(df, ['meta'], operations={'meta': 'combine_json'})
        assert result['meta'][0] == {'a': 1, 'b': 2}

    def test_combine_json_values_operation(self):
        base = self._make_base()
        df = pd.DataFrame({'facts_x': [{'cpu': 'x86'}], 'facts_y': [{'cpu': 'arm'}]})
        result = base.summarize_merged_dataframes(df, ['facts'], operations={'facts': 'combine_json_values'})
        assert result['facts'][0] == {'cpu': {'x86', 'arm'}}

    def test_combine_json_values_nested_facts_missing_values_and_duplicate_index(self):
        from metrics_utility.automation_controller_billing.helpers import HashableDict

        df = pd.DataFrame(
            {
                'facts_x': [{'cpu': {'x86'}, 'tags': {'site': 'a', 'rack': [1, 2]}}, None, pd.NA, {'ignore': None}],
                'facts_y': [{'cpu': 'arm', 'tags': {'site': 'a', 'rack': [1, 2]}}, {'count': 0}, float('nan'), {'ignore': ''}],
            },
            index=[2, 1, 2, 1],
        )

        result = self._make_base().summarize_merged_dataframes(df, ['facts'], operations={'facts': 'combine_json_values'})

        expected = pd.DataFrame(
            {'facts': [{'cpu': {'x86', 'arm'}, 'tags': {HashableDict({'site': 'a', 'rack': (1, 2)})}}, {'count': {0}}, {}, {}]},
            index=[2, 1, 2, 1],
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_combine_json_values_empty_dataframe(self):
        df = pd.DataFrame({'facts_x': pd.Series(dtype=object), 'facts_y': pd.Series(dtype=object)})

        result = self._make_base().summarize_merged_dataframes(df, ['facts'], operations={'facts': 'combine_json_values'})

        assert result.empty
        assert list(result.columns) == ['facts']

    @pytest.mark.parametrize('missing', ['facts_x', 'facts_y'])
    def test_combine_json_values_missing_source_column_raises(self, missing):
        df = pd.DataFrame({'facts_x': [{'cpu': 'left'}], 'facts_y': [{'cpu': 'right'}]}).drop(columns=missing)

        with pytest.raises(KeyError, match=missing):
            self._make_base().summarize_merged_dataframes(df, ['facts'], operations={'facts': 'combine_json_values'})

    def test_combine_json_values_across_outer_merged_batches(self):
        class FactsBase(ConcreteBase):
            @staticmethod
            def data_columns():
                return ['facts']

            @staticmethod
            def cast_types():
                return {'facts': object}

            @staticmethod
            def operations():
                return {'facts': 'combine_json_values'}

        base = FactsBase(extractor=None, month=datetime.date(2024, 1, 1), extra_params={})
        batches = [
            pd.DataFrame({'host_name': ['a', 'b'], 'facts': [{'cpu': {'x86'}}, {'cpu': {'arm'}}]}).set_index('host_name'),
            pd.DataFrame({'host_name': ['a', 'c'], 'facts': [{'cpu': {'arm'}}, {'cpu': {'ppc'}}]}).set_index('host_name'),
            pd.DataFrame({'host_name': ['a', 'b'], 'facts': [{'cpu': {'x86'}}, {'os': 'linux'}]}).set_index('host_name'),
        ]
        result = None
        for batch in batches:
            result = base.merge(result, batch)

        expected = pd.DataFrame(
            {'facts': [{'cpu': {'x86', 'arm'}}, {'cpu': {'arm'}, 'os': {'linux'}}, {'cpu': {'ppc'}}]},
            index=pd.Index(['a', 'b', 'c'], name='host_name', dtype=object),
        )
        pd.testing.assert_frame_equal(result.sort_index(), expected)

    def test_multiple_columns_processed(self):
        base = self._make_base()
        df = pd.DataFrame(
            {
                'a_x': [1.0, 2.0],
                'a_y': [3.0, 4.0],
                'b_x': [10.0, 20.0],
                'b_y': [10.0, 20.0],
            }
        )
        result = base.summarize_merged_dataframes(df, ['a', 'b'])
        assert list(result['a']) == [4.0, 6.0]
        assert list(result['b']) == [20.0, 40.0]

    def test_originals_deleted_after_merge(self):
        base = self._make_base()
        df = pd.DataFrame({'count_x': [1.0], 'count_y': [2.0]})
        result = base.summarize_merged_dataframes(df, ['count'])
        assert 'count_x' not in result.columns
        assert 'count_y' not in result.columns
