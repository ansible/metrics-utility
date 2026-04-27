"""Tests covering the combine_set/combine_json/combine_json_values lambda branches
in BaseTraditional.summarize_merged_dataframes() and the validate='many_to_many'
path in both Base.merge() and BaseTraditional.merge()."""

import pandas as pd

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
    assert 'k' in result['col'].iloc[0]


# --- validate='many_to_many' in BaseTraditional.merge() ---


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


def test_base_traditional_merge_many_to_many():
    bt = _ConcreteTraditional()
    df1 = pd.DataFrame({'id': [1], 'val': [10]}).set_index('id')
    df2 = pd.DataFrame({'id': [2], 'val': [20]}).set_index('id')
    result = bt.merge(df1, df2)
    assert len(result) == 2


# --- validate='many_to_many' in Base.merge() ---


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


def test_base_engine_merge_many_to_many():
    b = _ConcreteBase(extractor=None, month=None, extra_params={})
    df1 = pd.DataFrame({'id': [1], 'val': [10]}).set_index('id')
    df2 = pd.DataFrame({'id': [2], 'val': [20]}).set_index('id')
    result = b.merge(df1, df2)
    assert len(result) == 2
