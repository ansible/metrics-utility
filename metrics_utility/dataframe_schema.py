from metrics_utility.metric_utils import (
    JOB_HOST_SUMMARY_CAST_TYPES,
    JOB_HOST_SUMMARY_DATA_COLUMNS,
    JOB_HOST_SUMMARY_INDEX_COLUMNS,
    JOB_HOST_SUMMARY_OPERATIONS,
)


class DataframeSchemaMixin:
    """Default stub implementations for dataframe schema methods.

    Subclasses are expected to override these to return the actual schema
    for their specific data type.
    """

    @staticmethod
    def cast_types():
        return {}

    @staticmethod
    def data_columns():
        return []

    @staticmethod
    def operations():
        return {}

    @staticmethod
    def unique_index_columns():
        return []


class JobHostSummarySchema(DataframeSchemaMixin):
    """Shared schema for job host summary dataframes.

    Used by both DataframeJobhostSummaryUsage (billing engine) and
    DataframeJobHostSummary (traditional library) to avoid duplicating
    the identical static schema definitions.
    """

    @staticmethod
    def unique_index_columns():
        return JOB_HOST_SUMMARY_INDEX_COLUMNS

    @staticmethod
    def data_columns():
        return JOB_HOST_SUMMARY_DATA_COLUMNS

    @staticmethod
    def cast_types():
        return JOB_HOST_SUMMARY_CAST_TYPES

    @staticmethod
    def operations():
        return JOB_HOST_SUMMARY_OPERATIONS
