from metrics_utility.automation_controller_billing.dataframe_engine.base import Base
from metrics_utility.dataframe_schema import DataframeSchemaMixin, JobHostSummarySchema
from metrics_utility.library.dataframes.base_traditional import BaseTraditional
from metrics_utility.metric_utils import (
    JOB_HOST_SUMMARY_CAST_TYPES,
    JOB_HOST_SUMMARY_DATA_COLUMNS,
    JOB_HOST_SUMMARY_INDEX_COLUMNS,
    JOB_HOST_SUMMARY_OPERATIONS,
)


def test_dataframe_schema_mixin_stubs():
    assert DataframeSchemaMixin.cast_types() == {}
    assert DataframeSchemaMixin.data_columns() == []
    assert DataframeSchemaMixin.operations() == {}
    assert DataframeSchemaMixin.unique_index_columns() == []


def test_job_host_summary_schema():
    assert JobHostSummarySchema.unique_index_columns() == JOB_HOST_SUMMARY_INDEX_COLUMNS
    assert JobHostSummarySchema.data_columns() == JOB_HOST_SUMMARY_DATA_COLUMNS
    assert JobHostSummarySchema.cast_types() == JOB_HOST_SUMMARY_CAST_TYPES
    assert JobHostSummarySchema.operations() == JOB_HOST_SUMMARY_OPERATIONS


def test_base_engine_inherits_schema_mixin():
    base = Base(extractor=None, month=None, extra_params={})
    assert base.build_dataframe() is None
    assert Base.cast_types() == {}
    assert Base.data_columns() == []
    assert Base.operations() == {}
    assert Base.unique_index_columns() == []


def test_base_traditional_inherits_schema_mixin():
    assert BaseTraditional.cast_types() == {}
    assert BaseTraditional.data_columns() == []
    assert BaseTraditional.operations() == {}
    assert BaseTraditional.unique_index_columns() == []
