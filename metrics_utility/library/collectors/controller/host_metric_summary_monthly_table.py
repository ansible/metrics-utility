"""Collector for host metric summary monthly data."""

from ..util import DataframeOutput, collector


@collector
def host_metric_summary_monthly_table(*, db=None, output=DataframeOutput()):
    """Return full dump of host metric summary monthly table."""
    query = """
        SELECT
            main_hostmetricsummarymonthly.id,
            main_hostmetricsummarymonthly.date,
            main_hostmetricsummarymonthly.license_capacity,
            main_hostmetricsummarymonthly.license_consumed,
            main_hostmetricsummarymonthly.hosts_added,
            main_hostmetricsummarymonthly.hosts_deleted,
            main_hostmetricsummarymonthly.indirectly_managed_hosts
        FROM main_hostmetricsummarymonthly
        ORDER BY main_hostmetricsummarymonthly.id ASC
    """
    return output.sql(db, query)
