"""Collector functions for dashboard metrics.

These collectors use the @register decorator pattern from metrics-utility to
define data collection functions that can be called by metrics-service.

Each collector:
1. Executes SQL queries against the AWX database
2. Processes results to match automation-reports data format
3. Returns JSON-serializable data structures
"""

import decimal
import logging

from datetime import datetime
from typing import List, TypedDict

from ..util import collector
from .queries import (
    get_job_host_summaries_for_ids_query,
    get_job_labels_for_ids_query,
    get_jobs_batch_query,
    get_jobs_query,
    get_min_max_job_id_query,
)


logger = logging.getLogger(__name__)


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
    results: List[AWXJobType]


@collector
def dashboard_jobs(
    *,
    db=None,
    since: datetime = None,
    until: datetime = None,
    after_id: int = None,
    batch_size: int = None,
    date_field: str = 'modified',
) -> DashboardJobsResultType:
    """
    Collect job data for the dashboard.

    This collector retrieves job data for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        db: Database connection
        after_id: If provided alongside ``batch_size``, enables cursor-based pagination.
        batch_size: Maximum rows per batch when paginating.
        date_field: Column used for the date range filter — ``'modified'`` (default, used by the
            CLI billing/CCSP pipeline) or ``'finished'`` (used by dashboard/anonymized collection
            to exploit the existing finished index).

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

    if (after_id is None) != (batch_size is None):
        raise ValueError('after_id and batch_size must both be provided or both omitted')
    if batch_size is not None and batch_size <= 0:
        raise ValueError('batch_size must be greater than 0')

    if after_id is not None:
        query, params = get_jobs_batch_query(since, until, after_id, batch_size, date_field=date_field)
    else:
        query, params = get_jobs_query(since, until, date_field=date_field)

    with db.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor]

    if not rows:
        return {'count': 0, 'results': []}

    # Push enrichment joins into SQL for both batched and non-batched paths so that
    # labels and host summaries are never loaded as full-window in-memory dicts.
    job_ids = [row['id'] for row in rows]
    labels_query, labels_params = get_job_labels_for_ids_query(job_ids)
    summaries_query, summaries_params = get_job_host_summaries_for_ids_query(job_ids)

    all_labels: dict[int, list[int]] = {}
    with db.cursor() as cursor:
        cursor.execute(labels_query, labels_params)
        cols = [col[0] for col in cursor.description]
        for lrow in cursor:
            data = dict(zip(cols, lrow))
            all_labels.setdefault(data['unifiedjob_id'], []).append(data['label_id'])

    all_host_summaries: dict[int, list] = {}
    with db.cursor() as cursor:
        cursor.execute(summaries_query, summaries_params)
        cols = [col[0] for col in cursor.description]
        for srow in cursor:
            data = dict(zip(cols, srow))
            all_host_summaries.setdefault(data['job_id'], []).append(
                {
                    'id': data['id'],
                    'host_id': data['host_id'],
                    'host_name': data['host_name'],
                }
            )

    results = []
    for data in rows:
        host_summaries = all_host_summaries.get(data['id'], [])
        results.append(
            {
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
        )
    return {'count': len(results), 'results': results}


def collect_dashboard_jobs(
    *,
    db,
    since: datetime,
    until: datetime,
    page_size: int | None = None,
    max_records: int | None = None,
    date_field: str = 'modified',
) -> DashboardJobsResultType:
    """
    Cursor-paginated driver for :func:`dashboard_jobs`.

    Iterates over the job window in ``page_size``-row pages, collecting each
    page via :func:`dashboard_jobs` and accumulating the results.  The
    ``max_records`` safety cap stops collection once the total row count
    reaches the configured limit, preventing OOM errors in large deployments.

    Page size and record cap are read from environment variables when not
    supplied explicitly:

    * ``METRICS_UTILITY_DASHBOARD_PAGE_SIZE`` (default: 10 000)
    * ``METRICS_UTILITY_MAX_DASHBOARD_RECORDS`` (default: 0 — no cap)

    Args:
        db: Database connection.
        since: Start of date range (inclusive).
        until: End of date range (exclusive).
        page_size: Rows per page.  Reads ``METRICS_UTILITY_DASHBOARD_PAGE_SIZE``
            when *None*.
        max_records: Maximum total rows to return.  ``0`` means no limit.
            Reads ``METRICS_UTILITY_MAX_DASHBOARD_RECORDS`` when *None*.
        date_field: Column used for the date range filter — ``'modified'``
            (default) or ``'finished'``.

    Returns:
        dict with ``count`` (total rows collected) and ``results`` (list of
        job dicts in ascending ID order).
    """
    from metrics_utility.base.utils import get_dashboard_page_size, get_max_dashboard_records

    if page_size is None:
        page_size = get_dashboard_page_size()
    if page_size <= 0:
        raise ValueError('page_size must be greater than 0')

    if max_records is None:
        max_records = get_max_dashboard_records()

    # Determine cursor start: use min job ID in the window minus 1 so the
    # first `id > after_id` condition includes the min ID.
    bounds_query, bounds_params = get_min_max_job_id_query(since, until, date_field=date_field)
    with db.cursor() as cursor:
        cursor.execute(bounds_query, bounds_params)
        row = cursor.fetchone()

    if row is None or row[0] is None:
        return {'count': 0, 'results': []}

    min_id, max_id = row[0], row[1]
    after_id = min_id - 1

    all_results: list = []
    total_collected = 0

    while after_id <= max_id:
        remaining = page_size
        if max_records > 0:
            remaining = min(page_size, max_records - total_collected)
            if remaining <= 0:
                logger.warning(
                    'collect_dashboard_jobs: MAX_DASHBOARD_RECORDS limit (%d) reached after %d records — '
                    'stopping pagination early.',
                    max_records,
                    total_collected,
                )
                break

        collector_instance = dashboard_jobs(
            db=db,
            since=since,
            until=until,
            after_id=after_id,
            batch_size=remaining,
            date_field=date_field,
        )
        page = collector_instance.gather()

        page_results = page.get('results', [])
        if not page_results:
            break

        all_results.extend(page_results)
        total_collected += len(page_results)

        after_id = page_results[-1]['id']

        if len(page_results) < remaining:
            # Fewer rows than requested means we've reached the end.
            break

    return {'count': total_collected, 'results': all_results}
