"""Unit tests for automation_controller_billing/report/base.py helper methods.

All helpers under test are pure or depend only on extra_params / a small
DataFrame – no database or file I/O required.
"""

import pandas as pd

from metrics_utility.automation_controller_billing.report.base import Base


# ---------------------------------------------------------------------------
# Minimal concrete subclass
# ---------------------------------------------------------------------------


class ConcreteReport(Base):
    """Instantiable subclass for testing Base helpers."""

    def __init__(self, extra_params=None):
        from openpyxl import Workbook

        self.wb = Workbook()
        self.extra_params = extra_params or {}
        self.dataframes = {}


def _report(dedup=False):
    deduplicator = 'ccsp-experimental' if dedup else None
    return ConcreteReport(extra_params={'deduplicator': deduplicator, 'optional_sheets': None})


# ---------------------------------------------------------------------------
# has_dedup_enabled
# ---------------------------------------------------------------------------


class TestHasDedupEnabled:
    def test_returns_true_when_ccsp_experimental(self):
        assert _report(dedup=True).has_dedup_enabled() is True

    def test_returns_false_when_no_deduplicator(self):
        assert _report(dedup=False).has_dedup_enabled() is False

    def test_returns_false_for_other_deduplicator_name(self):
        r = ConcreteReport(extra_params={'deduplicator': 'ccsp', 'optional_sheets': None})
        assert r.has_dedup_enabled() is False


# ---------------------------------------------------------------------------
# convert_cell
# ---------------------------------------------------------------------------


class TestConvertCell:
    def test_scalar_string_returned_unchanged(self):
        assert _report().convert_cell('hello') == 'hello'

    def test_scalar_int_returned_unchanged(self):
        assert _report().convert_cell(42) == 42

    def test_none_returned_unchanged(self):
        assert _report().convert_cell(None) is None

    def test_set_converted_to_sorted_json_array(self):
        import json

        result = _report().convert_cell({'b', 'a', 'c'})
        parsed = json.loads(result)
        assert parsed == ['a', 'b', 'c']

    def test_list_of_strings_sorted_and_json_encoded(self):
        import json

        result = _report().convert_cell(['c', 'a', 'b'])
        parsed = json.loads(result)
        assert parsed == ['a', 'b', 'c']

    def test_list_with_set_items_converted(self):
        import json

        result = _report().convert_cell([{'x', 'y'}])
        parsed = json.loads(result)
        assert isinstance(parsed[0], list)
        assert set(parsed[0]) == {'x', 'y'}

    def test_dict_with_set_values_serialised(self):
        import json

        result = _report().convert_cell({'key': {'b', 'a'}})
        parsed = json.loads(result)
        assert parsed['key'] == ['a', 'b']

    def test_dict_with_scalar_values_serialised(self):
        import json

        result = _report().convert_cell({'a': 1, 'b': 2})
        parsed = json.loads(result)
        assert parsed == {'a': 1, 'b': 2}

    def test_empty_set_produces_empty_json_array(self):
        import json

        result = _report().convert_cell(set())
        assert json.loads(result) == []

    def test_empty_dict_produces_empty_json_object(self):
        import json

        result = _report().convert_cell({})
        assert json.loads(result) == {}


# ---------------------------------------------------------------------------
# calculate_dedup_count
# ---------------------------------------------------------------------------


class TestCalculateDedupCount:
    def test_counts_elements_in_set(self):
        r = _report()
        series = pd.Series([{1, 2, 3}, {4}])
        result = r.calculate_dedup_count(series)
        assert list(result) == [3, 1]

    def test_counts_elements_in_list(self):
        r = _report()
        series = pd.Series([['a', 'b'], ['c']])
        result = r.calculate_dedup_count(series)
        assert list(result) == [2, 1]

    def test_scalar_counts_as_1(self):
        r = _report()
        series = pd.Series(['hostname'])
        result = r.calculate_dedup_count(series)
        assert list(result) == [1]

    def test_empty_set_counts_as_0(self):
        r = _report()
        series = pd.Series([set()])
        result = r.calculate_dedup_count(series)
        assert list(result) == [0]


# ---------------------------------------------------------------------------
# add_dedup_count_column
# ---------------------------------------------------------------------------


class TestAddDedupCountColumn:
    def test_adds_count_column_when_base_exists(self):
        r = _report()
        df = pd.DataFrame({'host_names_before_dedup': [{'h1', 'h2'}, {'h3'}]})
        result = r.add_dedup_count_column(df, 'host_names_before_dedup', 'host_names_before_dedup_count')
        assert 'host_names_before_dedup_count' in result.columns
        assert list(result['host_names_before_dedup_count']) == [2, 1]

    def test_no_op_when_base_column_absent(self):
        r = _report()
        df = pd.DataFrame({'other_col': [1, 2]})
        result = r.add_dedup_count_column(df, 'host_names_before_dedup', 'host_names_before_dedup_count')
        assert 'host_names_before_dedup_count' not in result.columns


# ---------------------------------------------------------------------------
# handle_dedup_columns_for_scope
# ---------------------------------------------------------------------------


class TestHandleDedupColumnsForScope:
    def test_adds_dedup_columns_when_enabled_and_column_present(self):
        r = _report(dedup=True)
        df = pd.DataFrame({'host_names_before_dedup': [{'h1'}, {'h2'}]})
        columns = ['host_name']
        convert_cols = []
        new_columns, new_convert = r.handle_dedup_columns_for_scope(df, columns, convert_cols)
        assert 'host_names_before_dedup' in new_columns
        assert 'host_names_before_dedup_count' in new_columns
        assert 'host_names_before_dedup' in new_convert

    def test_no_change_when_dedup_disabled(self):
        r = _report(dedup=False)
        df = pd.DataFrame({'host_names_before_dedup': [{'h1'}]})
        columns = ['host_name']
        convert_cols = []
        new_columns, new_convert = r.handle_dedup_columns_for_scope(df, columns, convert_cols)
        assert new_columns == ['host_name']
        assert new_convert == []

    def test_no_change_when_column_absent_even_if_dedup_enabled(self):
        r = _report(dedup=True)
        df = pd.DataFrame({'other': [1]})
        columns = ['host_name']
        convert_cols = []
        new_columns, new_convert = r.handle_dedup_columns_for_scope(df, columns, convert_cols)
        assert 'host_names_before_dedup' not in new_columns


# ---------------------------------------------------------------------------
# add_dedup_labels_if_needed
# ---------------------------------------------------------------------------


class TestAddDedupLabelsIfNeeded:
    def test_adds_labels_for_present_columns_when_dedup_enabled(self):
        r = _report(dedup=True)
        labels = {}
        result = r.add_dedup_labels_if_needed(labels, ['host_names_before_dedup'])
        assert 'host_names_before_dedup' in result

    def test_no_labels_added_when_dedup_disabled(self):
        r = _report(dedup=False)
        labels = {}
        result = r.add_dedup_labels_if_needed(labels, ['host_names_before_dedup'])
        assert result == {}

    def test_only_adds_labels_for_matching_columns(self):
        r = _report(dedup=True)
        labels = {}
        result = r.add_dedup_labels_if_needed(labels, ['host_names_before_dedup_count'])
        assert 'host_names_before_dedup_count' in result
        assert 'host_names_before_dedup' not in result
