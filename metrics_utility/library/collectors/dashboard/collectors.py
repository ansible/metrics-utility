"""Collector functions for dashboard metrics.

These collectors use the @register decorator pattern from metrics-utility to
define data collection functions that can be called by metrics-service.

Each collector:
1. Executes SQL queries against the AWX database
2. Processes results to match automation-reports data format
3. Returns JSON-serializable data structures
"""

import decimal

from datetime import datetime
from typing import TypedDict

from ..util import collector
from .queries import get_job_host_summaries_query, get_job_labels_query, get_jobs_query


class AWXJobHostSummaryType(TypedDict):
    id: int
    host_name: str
    host_id: int | None


class AWXJobType(TypedDict):
    id: int
    name: str
    status: str
    unified_job_template_id: int | None
    organization_id: int | None
    started: datetime | None
    finished: datetime | None
    elapsed: decimal.Decimal
    launched_by_id: int | None
    launched_by_username: str | None
    project_id: int | None
    project_name: str | None
    labels: list[int]
    host_summaries: list[AWXJobHostSummaryType]
    num_hosts: int
    created: datetime | None
    modified: datetime | None


class DashboardJobsResultType(TypedDict):
    count: int
    results: list[AWXJobType]


def _dashboard_job_labels(since: datetime, until: datetime, db, **kwargs) -> dict[int, list[int]]:
    """
    Collect job labels for the dashboard.

    This collector retrieves all labels associated with jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        db: Database connection
        **kwargs: Additional parameters

    Returns:
        Dict with keys:
            - unifiedjob_id: list of label IDs associated with the job

    Output format:
    {
        job_id_1: [label_id_1, label_id_2, ...],
        job_id_2: [label_id_3, label_id_4, ...],
        ...
    }
    """

    query, params = get_job_labels_query(since, until)
    result = {}
    with db.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        for row in cursor:
            data = dict(zip(columns, row))
            job_id = data['unifiedjob_id']
            label_id = data['label_id']
            tmp = result.get(job_id, [])
            tmp.append(label_id)
            result[job_id] = tmp
    return result


def _dashboard_job_host_summaries(since: datetime, until: datetime, db, **kwargs) -> dict[int, list[AWXJobHostSummaryType]]:
    """
    Collect job host summaries for the dashboard.

    This collector retrieves host summary data for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        db: Database connection
        **kwargs: Additional parameters

    Returns:
        Dict with keys:
            - unifiedjob_id: list of host summary dicts associated with the job

    Output format:
    {
        job_id_1: [
            {
                id: int,
                host_name: str,
                host_id: int | None
            },
            ...],
        ...
    }
    """
    query, params = get_job_host_summaries_query(since, until)
    result = {}
    with db.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        for row in cursor:
            data = dict(zip(columns, row))
            job_id = data['job_id']
            host_summary = {
                'id': data['id'],
                'host_id': data['host_id'],
                'host_name': data['host_name'],
            }
            tmp = result.get(job_id, [])
            tmp.append(host_summary)
            result[job_id] = tmp
    return result


@collector
def dashboard_jobs(*, db=None, since: datetime = None, until: datetime = None) -> DashboardJobsResultType:
    """
    Collect job data for the dashboard.

    This collector retrieves job data for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        db: Database connection
        **kwargs: Additional parameters

    Returns:
        dict with keys:
            - count: total number of jobs retrieved
            - results: list of job dicts with job data

    Output format:
        {
            count: int,
            results: [
            {
                id: int,
                name: str,
                unified_job_template_id: int | None,
                organization_id: int | None,
                started: datetime | None,
                finished: datetime | None,
                status: str,
                elapsed: Decimal,
                launched_by_id: int | None,
                launched_by_username : str | None,
                project_id: int | None,
                project_name: str | None,
                created: datetime,
                modified: datetime,
                labels: list[int],
                host_summaries: list[{
                    id: int,
                    host_name: str,
                    host_id: int | None
                }],
                num_hosts: int
            }, ...]
         }
    """

    all_labels = _dashboard_job_labels(since, until, db)
    all_host_summaries = _dashboard_job_host_summaries(since, until, db)
    query, params = get_jobs_query(since, until)
    with db.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        results = []
        for row in cursor:
            data = dict(zip(columns, row))
            host_summaries = all_host_summaries.get(data['id'], [])
            result = {
                'id': data['id'],
                'name': data['name'],
                'unified_job_template_id': data['unified_job_template_id'],
                'organization_id': data['organization_id'],
                'started': data['started'],
                'finished': data['finished'],
                'status': data['status'],
                'elapsed': data['elapsed'],
                'launched_by_id': data['launched_by_id'],
                'launched_by_username': data['launched_by_username'],
                'project_id': data['project_id'],
                'project_name': data['project_name'],
                'created': data['created'],
                'modified': data['modified'],
                'labels': all_labels.get(data['id'], []),
                'host_summaries': host_summaries,
                'num_hosts': len(host_summaries),
            }
            results.append(result)
    return {'count': len(results), 'results': results}
