"""Raw AWX host metric analytics collector."""

from datetime import datetime

from ..util import DataframeOutput, collector


@collector
def host_metric_table(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Return host metrics changed by automation or deletion in a time window."""
    since_sql, until_sql = _window(since, until)
    query = f"""
        SELECT
            main_hostmetric.id,
            main_hostmetric.hostname,
            main_hostmetric.first_automation,
            main_hostmetric.last_automation,
            main_hostmetric.last_deleted,
            main_hostmetric.deleted,
            main_hostmetric.automated_counter,
            main_hostmetric.deleted_counter,
            main_hostmetric.used_in_inventories
        FROM main_hostmetric
        WHERE (
            (main_hostmetric.last_automation > {since_sql} AND main_hostmetric.last_automation <= {until_sql})
            OR (main_hostmetric.last_deleted > {since_sql} AND main_hostmetric.last_deleted <= {until_sql})
        )
        ORDER BY main_hostmetric.id ASC
    """
    return output.sql(db, query)


def _window(since, until):
    for name, value in (('since', since), ('until', until)):
        if value is not None and not isinstance(value, datetime):
            raise TypeError(f'host_metric_table: {name} must be a datetime, got {type(value).__name__}')
        if value is not None and value.tzinfo is None:
            raise ValueError(f'host_metric_table: {name} must be timezone-aware')
    if since is None or until is None:
        raise ValueError('host_metric_table requires both since and until')
    return f"'{since.isoformat()}'", f"'{until.isoformat()}'"
