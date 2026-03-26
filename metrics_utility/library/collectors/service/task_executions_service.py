from datetime import datetime, timedelta, timezone

import pandas as pd

from ..util import DataframeOutput, collector


@collector
def task_executions_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    """
    Collect task execution statistics from the metrics-service database.

    Gathers observability data about collector task executions from
    tasks_hourlymetricscollection and tasks_taskexecution tables.

    This is a snapshot-style collector (runs once per day) that provides
    internal observability about how well the collector pipeline is running.

    Args:
        db: Database connection (metrics-service DB).
        since: Start datetime (inclusive). Defaults to start of previous day (UTC).
        until: End datetime (exclusive). Defaults to start of today (UTC).

    Returns:
        DataFrame with columns: collector_type, status, execution_time_seconds
    """
    if since is None or until is None:
        now = datetime.now(timezone.utc)
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        since = since if since is not None else today_midnight - timedelta(days=1)
        until = until if until is not None else today_midnight

    query = f"""
        SELECT
            hmc.collector_type,
            hmc.status,
            te.execution_time_seconds
        FROM tasks_hourlymetricscollection hmc
        LEFT JOIN tasks_taskexecution te ON hmc.task_execution_id = te.id
        WHERE hmc.collection_timestamp >= '{since.isoformat()}'
          AND hmc.collection_timestamp < '{until.isoformat()}'
        ORDER BY hmc.collector_type
    """

    try:
        return output.sql(db, query)
    except Exception:
        return pd.DataFrame(columns=['collector_type', 'status', 'execution_time_seconds'])
