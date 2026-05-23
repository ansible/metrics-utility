from datetime import UTC, datetime, timedelta

from ..util import DataframeOutput, collector


@collector
def task_executions_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    """
    Collect task execution statistics from the metrics-service database.

    Gathers observability data about collector task executions from the
    tasks_taskexecution table.

    This is a snapshot-style collector (runs once per day) that provides
    internal observability about how well the collector pipeline is running.

    Args:
        db: Database connection (metrics-service DB).
        since: Start datetime (inclusive). Defaults to start of previous day (UTC).
        until: End datetime (exclusive). Defaults to start of today (UTC).
        output: Output adapter.

    Returns:
        DataFrame with columns: started_at, completed_at, collector_type
    """
    if since is None or until is None:
        now = datetime.now(UTC)
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        since = since if since is not None else today_midnight - timedelta(days=1)
        until = until if until is not None else today_midnight

    query = f"""
        SELECT
            started_at,
            completed_at,
            result_data->'collector_type' AS collector_type
        FROM tasks_taskexecution
        WHERE result_data->'collector_type' IS NOT NULL
          AND started_at >= '{since.isoformat()}'
          AND started_at < '{until.isoformat()}'
        ORDER BY collector_type
    """

    return output.sql(db, query)
