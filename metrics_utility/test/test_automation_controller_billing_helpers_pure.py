"""Unit tests for the pure helper functions in automation_controller_billing/helpers.py.

These functions have no database dependency and can be tested in isolation:
parse_json, parse_json_array, merge_json_sets, merge_arrays.
"""

from metrics_utility.automation_controller_billing.helpers import (
    merge_arrays,
    merge_json_sets,
    parse_json,
    parse_json_array,
)


# ---------------------------------------------------------------------------
# parse_json
# ---------------------------------------------------------------------------


class TestParseJson:
    def test_valid_json_string_returns_dict(self):
        assert parse_json('{"key": "value"}') == {'key': 'value'}

    def test_dict_passthrough_unchanged(self):
        d = {'a': 1}
        assert parse_json(d) == d

    def test_invalid_json_string_returns_empty_dict(self):
        assert parse_json('not json') == {}

    def test_none_returns_empty_dict(self):
        assert parse_json(None) == {}

    def test_integer_returns_empty_dict(self):
        assert parse_json(42) == {}

    def test_list_returns_empty_dict(self):
        assert parse_json([1, 2, 3]) == {}

    def test_empty_string_returns_empty_dict(self):
        assert parse_json('') == {}

    def test_nested_json_string_parsed(self):
        result = parse_json('{"a": {"b": 1}}')
        assert result == {'a': {'b': 1}}

    def test_empty_json_object_string(self):
        assert parse_json('{}') == {}


# ---------------------------------------------------------------------------
# parse_json_array
# ---------------------------------------------------------------------------


class TestParseJsonArray:
    def test_valid_json_array_string_returns_list(self):
        assert parse_json_array('["a", "b"]') == ['a', 'b']

    def test_empty_array_string_returns_empty_list(self):
        assert parse_json_array('[]') == []

    def test_json_object_string_returns_empty_list(self):
        assert parse_json_array('{"key": "val"}') == []

    def test_none_returns_empty_list(self):
        assert parse_json_array(None) == []

    def test_nan_returns_empty_list(self):

        assert parse_json_array(float('nan')) == []

    def test_malformed_json_returns_empty_list(self):
        assert parse_json_array('not json at all') == []

    def test_json_number_returns_empty_list(self):
        assert parse_json_array('42') == []

    def test_array_with_mixed_types_parsed(self):
        result = parse_json_array('[1, "two", null]')
        assert result == [1, 'two', None]


# ---------------------------------------------------------------------------
# merge_json_sets
# ---------------------------------------------------------------------------


class TestMergeJsonSets:
    def test_merges_scalar_values_from_multiple_dicts(self):
        values = ['{"cpu": "x86"}', '{"cpu": "arm"}']
        result = merge_json_sets(values)
        assert result == {'cpu': {'x86', 'arm'}}

    def test_null_values_are_filtered_out(self):
        values = ['{"key": null}', '{"key": "val"}']
        result = merge_json_sets(values)
        assert result == {'key': {'val'}}

    def test_empty_string_values_are_filtered_out(self):
        values = ['{"key": ""}', '{"key": "val"}']
        result = merge_json_sets(values)
        assert result == {'key': {'val'}}

    def test_na_string_values_are_filtered_out(self):
        values = ['{"serial": "NA"}', '{"serial": "abc123"}']
        result = merge_json_sets(values)
        assert result == {'serial': {'abc123'}}

    def test_dict_values_passed_directly(self):
        values = [{'a': 'x'}, {'a': 'y'}]
        result = merge_json_sets(values)
        assert result == {'a': {'x', 'y'}}

    def test_disjoint_keys_across_dicts(self):
        values = ['{"a": "1"}', '{"b": "2"}']
        result = merge_json_sets(values)
        assert result == {'a': {'1'}, 'b': {'2'}}

    def test_empty_iterable_returns_empty_dict(self):
        assert merge_json_sets([]) == {}

    def test_set_values_are_merged(self):
        values = [{'key': {'x', 'y'}}, {'key': {'y', 'z'}}]
        result = merge_json_sets(values)
        assert result == {'key': {'x', 'y', 'z'}}

    def test_malformed_json_string_skipped(self):
        values = ['not json', '{"key": "val"}']
        result = merge_json_sets(values)
        assert result == {'key': {'val'}}


# ---------------------------------------------------------------------------
# merge_arrays
# ---------------------------------------------------------------------------


class TestMergeArrays:
    def test_flattens_and_deduplicates(self):
        result = merge_arrays([[1, 2], [2, 3], [4]])
        assert set(result) == {1, 2, 3, 4}

    def test_none_entries_are_ignored(self):
        result = merge_arrays([None, [1, 2], None, [3]])
        assert set(result) == {1, 2, 3}

    def test_empty_sublists_produce_empty_result(self):
        result = merge_arrays([[], []])
        assert result == []

    def test_single_list(self):
        result = merge_arrays([[5, 6, 7]])
        assert set(result) == {5, 6, 7}

    def test_all_none_returns_empty_list(self):
        result = merge_arrays([None, None])
        assert result == []

    def test_empty_iterable_returns_empty_list(self):
        result = merge_arrays([])
        assert result == []

    def test_string_elements_deduplicated(self):
        result = merge_arrays([['host1', 'host2'], ['host2', 'host3']])
        assert set(result) == {'host1', 'host2', 'host3'}
