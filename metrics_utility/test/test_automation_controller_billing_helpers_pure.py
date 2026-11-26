"""Unit tests for the pure helper functions in automation_controller_billing/helpers.py.

These functions have no database dependency and can be tested in isolation:
parse_json, parse_json_array, merge_json_sets, merge_arrays.
"""

import json

from metrics_utility.automation_controller_billing.helpers import (
    HashableDict,
    make_hashable,
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

    def test_nested_dict_values_are_hashable(self):
        """Nested dict facts (e.g. tags) must not raise TypeError (AAP-101848)."""
        values = [
            '{"tags": {"Name": "test-instance", "Env": "prod"}}',
            '{"tags": {"Name": "test-instance", "Env": "prod"}}',
            '{"cpu": "x86"}',
        ]
        result = merge_json_sets(values)
        assert result['cpu'] == {'x86'}
        assert len(result['tags']) == 1
        tags_value = next(iter(result['tags']))
        assert isinstance(tags_value, HashableDict)
        assert tags_value == {'Name': 'test-instance', 'Env': 'prod'}
        # Report rendering sorts set members then json.dumps — must stay native objects.
        assert json.loads(json.dumps(sorted(result['tags']))) == [{'Env': 'prod', 'Name': 'test-instance'}]

    def test_nested_list_values_are_hashable(self):
        values = [{'ports': [80, 443]}, {'ports': [80, 443]}]
        result = merge_json_sets(values)
        assert result == {'ports': {(80, 443)}}

    def test_nested_tuple_values_are_hashable(self):
        result = merge_json_sets([{'ports': (80, 443)}, {'ports': (22,)}])
        assert result == {'ports': {(80, 443), (22,)}}

    def test_frozenset_values_are_hashable(self):
        result = merge_json_sets([{'groups': frozenset({'a', 'b'})}, {'groups': frozenset({'b', 'c'})}])
        assert result == {'groups': {frozenset({'a', 'b'}), frozenset({'b', 'c'})}}

    def test_deeply_nested_structures_are_hashable(self):
        values = [
            {'tags': {'nested': {'list': [1, {'deep': 'x'}]}}},
            {'tags': {'nested': {'list': [1, {'deep': 'x'}]}}},
        ]
        result = merge_json_sets(values)
        assert len(result['tags']) == 1
        (value,) = result['tags']
        assert isinstance(value, HashableDict)
        assert json.loads(json.dumps(value)) == {'nested': {'list': [1, {'deep': 'x'}]}}

    def test_set_values_do_not_mutate_input(self):
        first = {'x', 'y'}
        second = {'y', 'z'}
        merge_json_sets([{'key': first}, {'key': second}])
        assert first == {'x', 'y'}
        assert second == {'y', 'z'}

    def test_single_set_value_is_copied(self):
        original = {'x', 'y'}
        result = merge_json_sets([{'key': original}])
        assert result['key'] == {'x', 'y'}
        assert result['key'] is not original
        result['key'].add('z')
        assert original == {'x', 'y'}

    def test_input_dicts_are_not_mutated(self):
        first = {'a': 'x', 'nested': {'k': 1}}
        second = {'a': 'y', 'items': [1, 2]}
        result = merge_json_sets([first, second])
        assert first == {'a': 'x', 'nested': {'k': 1}}
        assert second == {'a': 'y', 'items': [1, 2]}
        assert result == {'a': {'x', 'y'}, 'nested': {HashableDict({'k': 1})}, 'items': {(1, 2)}}

    def test_mixed_string_and_dict_inputs_parity(self):
        """The pre-parsed dict fast path must match the JSON-string path exactly."""
        as_strings = ['{"a": "x"}', '{"a": "y"}', '{"b": {"c": [1]}}']
        as_dicts = [json.loads(v) for v in as_strings]
        assert merge_json_sets(as_strings) == merge_json_sets(as_dicts)

    def test_scalar_types_are_preserved(self):
        result = merge_json_sets([{'i': 1, 'f': 1.5, 'b': True, 's': 'x'}])
        assert result == {'i': {1}, 'f': {1.5}, 'b': {True}, 's': {'x'}}


# ---------------------------------------------------------------------------
# make_hashable / HashableDict
# ---------------------------------------------------------------------------


class TestMakeHashable:
    def test_dict_becomes_hashable_dict(self):
        value = make_hashable({'a': 1})
        assert isinstance(value, HashableDict)
        assert hash(value) == hash(HashableDict({'a': 1}))

    def test_hashable_dict_passthrough_identity(self):
        original = HashableDict({'a': 1})
        assert make_hashable(original) is original

    def test_list_becomes_tuple(self):
        assert make_hashable([1, {'b': 2}]) == (1, HashableDict({'b': 2}))

    def test_tuple_with_nested_dict(self):
        assert make_hashable(({'a': 1}, 2)) == (HashableDict({'a': 1}), 2)

    def test_set_becomes_frozenset(self):
        value = make_hashable({1, 2, 3})
        assert isinstance(value, frozenset)
        assert value == frozenset({1, 2, 3})

    def test_set_with_nested_dict(self):
        value = make_hashable([{'x': 1}, {'x': 1}])
        # Nested dicts become HashableDict so duplicates collapse when used in a set.
        assert set(value) == {HashableDict({'x': 1})}

    def test_scalar_passthrough(self):
        assert make_hashable(42) == 42
        assert make_hashable('host') == 'host'
        assert make_hashable(None) is None

    def test_nested_structures_round_trip_json(self):
        value = make_hashable({'tags': {'Env': 'prod'}, 'ports': [22]})
        assert json.loads(json.dumps(value)) == {'tags': {'Env': 'prod'}, 'ports': [22]}

    def test_hashable_dict_lt_comparable_values(self):
        left = HashableDict({'a': 1})
        right = HashableDict({'a': 2})
        assert left < right
        assert sorted([right, left]) == [left, right]

    def test_hashable_dict_lt_handles_incomparable_nested_values(self):
        left = HashableDict({'tags': HashableDict({'Name': 'x'})})
        right = HashableDict({'tags': 'plain'})
        # Must not raise TypeError when nested values under the same key differ in type.
        _ = left < right
        ordered = sorted([right, left])
        assert len(ordered) == 2
        assert left in ordered and right in ordered

    def test_hashable_dict_lt_non_dict_returns_not_implemented(self):
        assert HashableDict({'a': 1}).__lt__(5) is NotImplemented


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
