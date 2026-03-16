"""Test suite for base_traditional dataframe helpers."""

import numpy as np

from metrics_utility.library.dataframes.base_traditional import (
    combine_json,
    combine_json_values,
    combine_set,
    merge_arrays,
    merge_json_sets,
    merge_setdicts,
    merge_sets,
    parse_json,
    parse_json_array,
)


class TestParseJsonArray:
    """Tests for parse_json_array."""

    def test_list_input_returned_as_is(self):
        """Already-a-list input (psycopg3 JsonbLoader path) is returned unchanged."""
        value = ['a', 'b', 'c']
        assert parse_json_array(value) == value

    def test_empty_list_input(self):
        """Empty list is returned unchanged."""
        value = []
        assert parse_json_array(value) == []

    def test_null_returns_empty_list(self):
        """None / pd.NA produces an empty list."""
        assert parse_json_array(None) == []
        assert parse_json_array(np.nan) == []

    def test_valid_json_list_string(self):
        """A JSON-encoded list string is parsed correctly."""
        assert parse_json_array('["x", "y"]') == ['x', 'y']

    def test_valid_json_non_list_string_returns_empty(self):
        """A JSON-encoded dict (not a list) returns []."""
        assert parse_json_array('{"key": "value"}') == []

    def test_invalid_json_string_returns_empty(self):
        """Malformed JSON returns []."""
        assert parse_json_array('not-json') == []

    def test_type_error_returns_empty(self):
        """Non-string, non-null, non-list input triggers TypeError in json.loads → []."""
        # json.loads(5) raises TypeError in Python 3
        assert parse_json_array(5) == []

    def test_object_raises_type_error_returns_empty(self):
        """An object that causes TypeError in json.loads (e.g. a tuple) returns []."""
        # json.loads(('a', 'b')) raises TypeError; pd.isnull on a tuple is False
        assert parse_json_array(('a', 'b')) == []


class TestParseJson:
    """Tests for parse_json."""

    def test_valid_json_string(self):
        assert parse_json('{"a": 1}') == {'a': 1}

    def test_invalid_json_string_returns_empty_dict(self):
        assert parse_json('bad json') == {}

    def test_dict_returned_as_is(self):
        d = {'x': 42}
        assert parse_json(d) == d

    def test_none_returns_empty_dict(self):
        assert parse_json(None) == {}

    def test_integer_returns_empty_dict(self):
        assert parse_json(123) == {}


class TestCombineJson:
    """Tests for combine_json."""

    def test_two_dicts_merged(self):
        assert combine_json({'a': 1}, {'b': 2}) == {'a': 1, 'b': 2}

    def test_second_overwrites_first(self):
        assert combine_json({'a': 1}, {'a': 99}) == {'a': 99}

    def test_non_dict_inputs_ignored(self):
        assert combine_json(None, {'b': 2}) == {'b': 2}
        assert combine_json({'a': 1}, None) == {'a': 1}
        assert combine_json(None, None) == {}


class TestCombineSet:
    """Tests for combine_set."""

    def test_two_sets_unioned(self):
        assert combine_set({1, 2}, {3, 4}) == {1, 2, 3, 4}

    def test_list_inputs_converted_to_sets(self):
        assert combine_set([1, 2], [2, 3]) == {1, 2, 3}

    def test_non_set_non_list_treated_as_empty(self):
        assert combine_set(None, {5}) == {5}
        assert combine_set({1}, None) == {1}

    def test_both_empty(self):
        assert combine_set(None, None) == set()


class TestCombineJsonValues:
    """Tests for combine_json_values."""

    def test_values_collected_into_sets(self):
        result = combine_json_values({'os': 'linux'}, {'os': 'windows'})
        assert result == {'os': {'linux', 'windows'}}

    def test_none_values_ignored(self):
        result = combine_json_values({'key': None}, {'key': 'value'})
        assert result == {'key': {'value'}}

    def test_empty_string_values_ignored(self):
        result = combine_json_values({'key': ''}, {'key': 'val'})
        assert result == {'key': {'val'}}

    def test_set_values_merged(self):
        result = combine_json_values({'tags': {'a', 'b'}}, {'tags': {'c'}})
        assert result == {'tags': {'a', 'b', 'c'}}

    def test_non_dict_inputs_skipped(self):
        result = combine_json_values(None, {'k': 'v'})
        assert result == {'k': {'v'}}


class TestMergeSets:
    """Tests for merge_sets."""

    def test_merges_multiple_sets(self):
        assert merge_sets([{1, 2}, {3}, {2, 4}]) == {1, 2, 3, 4}

    def test_single_set(self):
        assert merge_sets([{7, 8}]) == {7, 8}

    def test_empty_list(self):
        assert merge_sets([]) == set()


class TestMergeSetdicts:
    """Tests for merge_setdicts."""

    def test_reduces_list_of_dicts(self):
        result = merge_setdicts([{'os': 'linux'}, {'os': 'linux', 'arch': 'x86'}])
        assert result == {'os': {'linux'}, 'arch': {'x86'}}


class TestMergeJsonSets:
    """Tests for merge_json_sets."""

    def test_dict_values_collected(self):
        result = merge_json_sets(['{"os": "linux"}', '{"os": "windows"}'])
        assert result == {'os': {'linux', 'windows'}}

    def test_na_value_ignored(self):
        result = merge_json_sets(['{"os": "NA"}'])
        assert result == {}

    def test_none_value_ignored(self):
        result = merge_json_sets(['{"os": null}'])
        assert result == {}

    def test_already_dict(self):
        result = merge_json_sets([{'os': 'linux'}])
        assert result == {'os': {'linux'}}


class TestMergeArrays:
    """Tests for merge_arrays."""

    def test_flattens_and_deduplicates(self):
        result = merge_arrays([['a', 'b'], ['b', 'c']])
        assert set(result) == {'a', 'b', 'c'}

    def test_none_values_filtered(self):
        result = merge_arrays([None, ['x'], ['y']])
        assert set(result) == {'x', 'y'}

    def test_empty_input(self):
        assert merge_arrays([]) == []
