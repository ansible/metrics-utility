"""Tests covering the combine_set/combine_json/combine_json_values lambda branches
in BaseTraditional.summarize_merged_dataframes() and the validate='one_to_one'
path in both Base.merge() and BaseTraditional.merge()."""

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.library.dataframes.base_traditional import BaseTraditional


# --- BaseTraditional.summarize_merged_dataframes() lambda branches ---


def _bt():
    return BaseTraditional()


def test_summarize_combine_set():
    df = pd.DataFrame({'col_x': [['a', 'b']], 'col_y': [['b', 'c']]})
    result = _bt().summarize_merged_dataframes(df, ['col'], operations={'col': 'combine_set'})
    assert result['col'].iloc[0] == {'a', 'b', 'c'}


def test_summarize_combine_json():
    df = pd.DataFrame({'col_x': [{'a': 1}], 'col_y': [{'b': 2}]})
    result = _bt().summarize_merged_dataframes(df, ['col'], operations={'col': 'combine_json'})
    assert result['col'].iloc[0] == {'a': 1, 'b': 2}


def test_summarize_combine_json_values():
    df = pd.DataFrame({'col_x': [{'k': 'v1'}], 'col_y': [{'k': 'v2'}]})
    result = _bt().summarize_merged_dataframes(df, ['col'], operations={'col': 'combine_json_values'})
    assert result['col'].iloc[0] == {'k': {'v1', 'v2'}}


def test_summarize_lambda_branches_bind_per_column():
    # Exercises all three lambda operations in a single call with multiple columns.
    # A single-column call cannot expose the closure-over-loop-variable bug (S1515)
    # because col is only ever one value; this multi-column call would regress if
    # the c=col default-arg capture were removed.
    df = pd.DataFrame(
        {
            'a_x': [{'x': 1}],
            'a_y': [{'y': 2}],
            'b_x': [['p']],
            'b_y': [['q']],
            'c_x': [{'k': 'v1'}],
            'c_y': [{'k': 'v2'}],
        }
    )
    ops = {'a': 'combine_json', 'b': 'combine_set', 'c': 'combine_json_values'}
    result = _bt().summarize_merged_dataframes(df, ['a', 'b', 'c'], operations=ops)
    assert result['a'].iloc[0] == {'x': 1, 'y': 2}
    assert result['b'].iloc[0] == {'p', 'q'}
    assert result['c'].iloc[0] == {'k': {'v1', 'v2'}}


# --- validate='one_to_one' in BaseTraditional.merge() ---


class _ConcreteTraditional(BaseTraditional):
    @staticmethod
    def unique_index_columns():
        return ['id']

    @staticmethod
    def data_columns():
        return ['val']

    @staticmethod
    def cast_types():
        return {}

    @staticmethod
    def operations():
        return {}


def test_base_traditional_merge_outer():
    bt = _ConcreteTraditional()
    df1 = pd.DataFrame({'id': [1], 'val': [10]}).set_index('id')
    df2 = pd.DataFrame({'id': [2], 'val': [20]}).set_index('id')
    result = bt.merge(df1, df2)
    assert len(result) == 2


def test_base_traditional_merge_raises_on_duplicate_keys():
    bt = _ConcreteTraditional()
    df1 = pd.DataFrame({'id': [1, 1], 'val': [10, 20]}).set_index('id')
    df2 = pd.DataFrame({'id': [2], 'val': [30]}).set_index('id')
    with pytest.raises(pd.errors.MergeError):
        bt.merge(df1, df2)


# --- validate='one_to_one' in Base.merge() ---


class _ConcreteBase(Base):
    @staticmethod
    def unique_index_columns():
        return ['id']

    @staticmethod
    def data_columns():
        return ['val']

    @staticmethod
    def cast_types():
        return {}

    @staticmethod
    def operations():
        return {}


def test_base_engine_merge_outer():
    b = _ConcreteBase(extractor=None, month=None, extra_params={})
    df1 = pd.DataFrame({'id': [1], 'val': [10]}).set_index('id')
    df2 = pd.DataFrame({'id': [2], 'val': [20]}).set_index('id')
    result = b.merge(df1, df2)
    assert len(result) == 2


def test_base_engine_merge_raises_on_duplicate_keys():
    b = _ConcreteBase(extractor=None, month=None, extra_params={})
    df1 = pd.DataFrame({'id': [1, 1], 'val': [10, 20]}).set_index('id')
    df2 = pd.DataFrame({'id': [2], 'val': [30]}).set_index('id')
    with pytest.raises(pd.errors.MergeError):
        b.merge(df1, df2)
