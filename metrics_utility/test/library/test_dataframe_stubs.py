from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.library.dataframes.base_traditional import BaseTraditional


def test_base_engine_stub_methods():
    base = Base(extractor=None, month=None, extra_params={})
    assert base.build_dataframe() is None
    assert Base.unique_index_columns() == []
    assert Base.data_columns() == []
    assert Base.cast_types() == {}
    assert Base.operations() == {}


def test_base_traditional_stub_methods():
    assert BaseTraditional.cast_types() == {}
    assert BaseTraditional.data_columns() == []
    assert BaseTraditional.operations() == {}
    assert BaseTraditional.unique_index_columns() == []
