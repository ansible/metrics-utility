import hashlib

from collections.abc import Iterable
from datetime import datetime

from ..util import as_int_list, as_list


_ALLOWED_DATE_FIELDS = frozenset({'modified', 'finished'})


def _validate_date_field(date_field: str) -> None:
    if date_field not in _ALLOWED_DATE_FIELDS:
        raise ValueError(f'date_field must be one of {sorted(_ALLOWED_DATE_FIELDS)}, got {date_field!r}')


def get_min_max_job_id_query(since: datetime, until: datetime, date_field: str = 'modified') -> tuple[str, list]:
    """
    Return the min and max job IDs for the filtered window.

    Used to establish cursor bounds before a batched collection run.
    Mirrors the ``JOIN main_job`` in ``get_jobs_batch_query`` so bounds only
    cover actual Job-type records and not WorkflowJobs, ProjectUpdates, etc.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        date_field: Column used for the date range filter — ``'modified'`` (default) or ``'finished'``.

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    where_clause, params = get_where_clause(since, until, date_field=date_field)
    query = f"""
        SELECT MIN(uj.id) AS min_id, MAX(uj.id) AS max_id
        FROM main_unifiedjob uj
        JOIN main_job mj ON mj.unifiedjob_ptr_id = uj.id
        {where_clause}
    """
    return query, params


def get_where_clause(since: datetime, until: datetime, date_field: str = 'modified') -> tuple[str, list]:
    """
    Generate SQL WHERE clause for filtering jobs by a date range.
    Excludes sync and workflow jobs and includes only jobs with status 'failed' or 'successful'.

    Workflow child jobs (``launch_type = 'workflow'``) are excluded because they have no
    reliable user attribution (``launched_by_id`` is NULL for them in ``_JOBS_BASE_SQL``),
    which caused the Successful/Failed totals to be inflated relative to the Top 5 Users
    breakdown (AAP-74848 / AAP-85129).

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        date_field: Column used for the date range filter — ``'modified'`` (default, used by the
            CLI billing/CCSP pipeline to pick up records changed since the last run) or
            ``'finished'`` (used by dashboard/anonymized collection where job completion time
            is the meaningful boundary and the ``finished`` index can be exploited).

    Returns:
        Tuple of (SQL WHERE clause string with placeholders, [params])
    """
    _validate_date_field(date_field)
    where_clause = f"""
    WHERE uj.launch_type NOT IN (%s, %s)
    AND (uj.status= %s OR uj.status = %s)
    AND uj.{date_field} >= %s
    AND uj.{date_field} < %s
    """
    params = ['sync', 'workflow', 'failed', 'successful', since.isoformat(), until.isoformat()]
    return where_clause, params


def get_job_labels_query(since: datetime, until: datetime, date_field: str = 'modified') -> tuple[str, list]:
    """
    Generate SQL query to fetch job labels for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        date_field: Column used for the date range filter — ``'modified'`` (default) or ``'finished'``.

    Returns:
        Tuple of (SQL query string with placeholders, [params])

    Database schema:
        - main_unifiedjob_labels
        - main_unifiedjob (for filtering by date range)
    """
    where_clause, params = get_where_clause(since, until, date_field=date_field)
    query = f"""
            SELECT
                l.unifiedjob_id,
                l.label_id
            FROM main_unifiedjob_labels l
            JOIN main_unifiedjob uj on uj.id = l.unifiedjob_id
            {where_clause}
           ORDER BY l.unifiedjob_id
        """
    return query, params


def get_job_host_summaries_query(since: datetime, until: datetime, date_field: str = 'modified') -> tuple[str, list]:
    """
    Generate SQL query to fetch job host summaries for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        date_field: Column used for the date range filter — ``'modified'`` (default) or ``'finished'``.

    Returns:
        Tuple of (SQL query string with placeholders, [params])

    Database schema:
        - main_jobhostsummary
        - main_unifiedjob (for filtering by date range)
    """
    where_clause, params = get_where_clause(since, until, date_field=date_field)
    query = f"""
         SELECT
            hs.id, hs.host_name, hs.host_id, hs.job_id
        FROM main_jobhostsummary hs
        JOIN main_unifiedjob uj on uj.id = hs.job_id {where_clause}
         order by hs.job_id
        """
    return query, params


# Shared SELECT and JOIN fragment used by both get_jobs_query and get_jobs_batch_query.
# Both functions build on this base so that adding a column or join in one place
# automatically applies to the batched path and prevents schema drift in the shared
# dict-building loop in dashboard_jobs.
_JOBS_BASE_SQL = """SELECT
    uj.id,
    COALESCE(ujt.name, uj.name) as name,
    uj.unified_job_template_id,
    uj.organization_id,
    uj.started,
    uj.finished,
    uj.status,
    uj.elapsed,
    CASE WHEN
    uj.launch_type ='manual' or uj.launch_type ='relaunch' then u.id
    ELSE null
    END as launched_by_id,
    CASE
    WHEN uj.launch_type ='manual' or uj.launch_type ='relaunch' then u.username
    ELSE null
    END as launched_by_username,
    mj.project_id,
    ujp.name as project_name,
    uj.created,
    uj.modified,
    o.name as organization_name
    FROM main_unifiedjob uj
    JOIN main_job mj on mj.unifiedjob_ptr_id = uj.id
    LEFT JOIN main_unifiedjobtemplate ujt ON ujt.id = uj.unified_job_template_id
    LEFT JOIN auth_user u on u.id = uj.created_by_id
    LEFT JOIN main_unifiedjobtemplate ujp on ujp.id = mj.project_id
    LEFT JOIN main_organization o on o.id = uj.organization_id"""


def get_jobs_query(since: datetime, until: datetime, date_field: str = 'modified') -> tuple[str, list]:
    """
    Generate SQL query to fetch jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        date_field: Column used for the date range filter and sort — ``'modified'`` (default) or
            ``'finished'``. Use ``'finished'`` for dashboard/anonymized collection to exploit the
            existing ``main_unifiedjob_finished_eccf6159`` index.

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    where_clause, params = get_where_clause(since, until, date_field=date_field)
    query = f'{_JOBS_BASE_SQL} {where_clause} order by uj.{date_field}'
    return query, params


def get_jobs_batch_query(since: datetime, until: datetime, after_id: int, batch_size: int, date_field: str = 'modified') -> tuple[str, list]:
    """
    Cursor-paginated variant of ``get_jobs_query``.

    Returns up to *batch_size* rows with ``id > after_id``, ordered by ``id``
    so successive calls advance the cursor without skipping or re-fetching rows.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        after_id: Exclusive lower bound on job ID (use 0 or min_id - 1 for first page)
        batch_size: Maximum number of rows to return
        date_field: Column used for the date range filter — ``'modified'`` (default) or ``'finished'``.

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    where_clause, params = get_where_clause(since, until, date_field=date_field)
    params += [after_id, batch_size]
    query = f"""{_JOBS_BASE_SQL}
    {where_clause} AND uj.id > %s
    ORDER BY uj.id
    LIMIT %s"""
    return query, params


def get_job_labels_for_ids_query(job_ids: list) -> tuple[str, list]:
    """
    Fetch labels for a specific set of job IDs.

    More efficient than ``get_job_labels_query`` when the job set is already
    known (e.g. after a batch fetch) because it avoids re-joining against
    the full date-range filter.

    Args:
        job_ids: List of unified job IDs to fetch labels for

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    query = """
        SELECT l.unifiedjob_id, l.label_id
        FROM main_unifiedjob_labels l
        WHERE l.unifiedjob_id = ANY(%s)
        ORDER BY l.unifiedjob_id
    """
    return query, [job_ids]


def get_job_host_summaries_for_ids_query(job_ids: list) -> tuple[str, list]:
    """
    Fetch host summaries for a specific set of job IDs.

    More efficient than ``get_job_host_summaries_query`` when the job set is
    already known (e.g. after a batch fetch).

    Args:
        job_ids: List of unified job IDs to fetch host summaries for

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    query = """
        SELECT hs.id, hs.host_name, hs.host_id, hs.job_id
        FROM main_jobhostsummary hs
        WHERE hs.job_id = ANY(%s)
        ORDER BY hs.job_id
    """
    return query, [job_ids]


# --- Daily dashboard reconciliation -------------------------------------------------------------
# The reconciliation compares the Controller with the dashboard data stored by metrics-service.
# The dashboard stores every unified job type (including sync and workflow launches), so these
# queries filter only on ``main_unifiedjob.finished`` (indexed) and the statuses passed by the caller.
# metrics-service owns the dashboard job definition; it is intentionally not duplicated here.


def _validate_window(since: datetime, until: datetime) -> None:
    for name, value in (('since', since), ('until', until)):
        if not isinstance(value, datetime):
            raise TypeError(f'{name} must be a datetime, got {type(value).__name__}')
        if value.tzinfo is None:
            raise ValueError(f'{name} must be timezone-aware')


def _validate_statuses(statuses: Iterable[str]) -> list[str]:
    statuses = as_list(statuses, 'statuses')
    if not statuses:
        raise ValueError('statuses must not be empty')
    for status in statuses:
        if not isinstance(status, str) or not status:
            raise ValueError(f'statuses must contain non-empty strings, got {status!r}')
    return statuses


def _get_reconcile_where_clause(since: datetime, until: datetime, statuses: Iterable[str]) -> tuple[str, list]:
    """
    Build the WHERE clause shared by the reconciliation queries.

    Selects unified jobs of every type finished in ``[since, until)`` with one of *statuses*.

    Raises:
        TypeError: If *since* or *until* is not a datetime.
        ValueError: If *since* or *until* is timezone-naive, or *statuses* is empty, not an iterable, or contains a value
            that is not a non-empty string.
    """
    _validate_window(since, until)
    statuses = _validate_statuses(statuses)
    where_clause = """
    WHERE uj.finished >= %s
    AND uj.finished < %s
    AND uj.status = ANY(%s::text[])
    """
    return where_clause, [since, until, statuses]


def job_ids_digest(job_ids: Iterable[int]) -> str:
    """
    Return the digest :func:`get_job_minute_aggregate_query` computes for a set of job IDs.

    The lowercase hex SHA-256 of the distinct job IDs in ascending order, joined with commas (e.g. ``'3,7'``).
    Lets a caller compute the same digest for its own job IDs, or check its database-side computation. Duplicate
    IDs are ignored, as job IDs are unique on the Controller.

    Raises:
        ValueError: If *job_ids* is not an iterable of integers in the bigint range (strings, floats and
            booleans are rejected, as they would never match the database digest).
    """
    text = ','.join(str(job_id) for job_id in sorted(set(as_int_list(job_ids, 'job_ids'))))
    return hashlib.sha256(text.encode()).hexdigest()


def get_job_minute_aggregate_query(since: datetime, until: datetime, statuses: Iterable[str]) -> tuple[str, list]:
    """
    Return the job count and a digest of the job IDs per finish minute.

    Used by the daily dashboard reconciliation as the cheap completeness check: when the count and the digest of
    every minute match the dashboard data, the window is treated as complete. Minutes without jobs are not
    returned. The digest is the SHA-256 of the comma-separated job IDs in ascending order (see
    :func:`job_ids_digest`), so it compares the sets of job IDs: a missing job and an extra local job in the same
    minute (e.g. one deleted on the Controller) are detected. SHA-256 is used because ``md5()`` is not available
    in PostgreSQL in FIPS mode. When *since* or *until* is not aligned to a minute, the first and last minutes only
    cover the part inside the window, so the local side must apply the same ``[since, until)`` filter to compare them.

    Args:
        since: Start of the ``finished`` range (inclusive, timezone-aware)
        until: End of the ``finished`` range (exclusive, timezone-aware)
        statuses: Job statuses to include, any iterable of strings (e.g. ``['failed', 'successful']``)

    Returns:
        Tuple of (SQL query string with placeholders, [params]). Columns: ``minute``
        (``finished`` truncated to the minute in UTC, independent of the session time zone), ``job_count``,
        ``job_ids_sha256`` (lowercase hex digest, see :func:`job_ids_digest`); ordered by ``minute``.

    Raises:
        TypeError: If *since* or *until* is not a datetime.
        ValueError: If *since* or *until* is timezone-naive, or *statuses* is invalid.
    """
    where_clause, params = _get_reconcile_where_clause(since, until, statuses)
    query = f"""
        SELECT
            date_trunc('minute', uj.finished, 'UTC') AS minute,
            COUNT(*) AS job_count,
            encode(sha256(convert_to(string_agg(uj.id::text, ',' ORDER BY uj.id), 'UTF8')), 'hex') AS job_ids_sha256
        FROM main_unifiedjob uj
        {where_clause}
        GROUP BY 1
        ORDER BY 1
    """
    return query, params


def get_job_ids_for_minutes_query(since: datetime, until: datetime, statuses: Iterable[str], minutes: Iterable[datetime]) -> tuple[str, list]:
    """
    Return the IDs and finish times of the jobs that finished in the given minutes.

    Used by the daily dashboard reconciliation only for the minutes whose aggregate did not match, to
    find the individual missing (or extra) jobs.

    Args:
        since: Start of the ``finished`` range (inclusive, timezone-aware)
        until: End of the ``finished`` range (exclusive, timezone-aware)
        statuses: Job statuses to include, any iterable of strings (e.g. ``['failed', 'successful']``)
        minutes: Non-empty iterable of timezone-aware datetimes truncated to the minute (zero seconds and
            microseconds), as returned in the ``minute`` column of :func:`get_job_minute_aggregate_query`:
            a list, a pandas Series (e.g. ``df['minute']``) or an object array (``Series.to_numpy()``).
            A ``datetime64`` array (``Series.values``) is rejected because it has no timezone.

    Returns:
        Tuple of (SQL query string with placeholders, [params]). Columns: ``job_id``, ``finished``;
        ordered by ``job_id``.

    Raises:
        TypeError: If *since* or *until* is not a datetime, or *minutes* contains a value that is not a datetime.
        ValueError: If *since* or *until* is timezone-naive, *statuses* is invalid, or *minutes* is a
            string, a mapping, a DataFrame or not iterable, is empty, or contains a datetime that is
            timezone-naive or not truncated to the minute.
    """
    where_clause, params = _get_reconcile_where_clause(since, until, statuses)
    minutes = as_list(minutes, 'minutes')
    if not minutes:
        raise ValueError('minutes must not be empty')
    for minute in minutes:
        if not isinstance(minute, datetime):
            raise TypeError(f'minutes must contain datetimes, got {type(minute).__name__}')
        if minute.tzinfo is None:
            raise ValueError(f'minutes must contain timezone-aware datetimes, got {minute.isoformat()}')
        if minute.second or minute.microsecond:
            # A value that is not truncated to the minute would silently match no job.
            raise ValueError(f'minutes must be truncated to the minute, got {minute.isoformat()}')
    query = f"""
        SELECT uj.id AS job_id, uj.finished
        FROM main_unifiedjob uj
        {where_clause}
        AND date_trunc('minute', uj.finished, 'UTC') = ANY(%s::timestamptz[])
        ORDER BY uj.id
    """
    return query, [*params, minutes]


def get_job_host_summary_counts_query(since: datetime, until: datetime, statuses: Iterable[str]) -> tuple[str, list]:
    """
    Return the number of host summaries per job.

    Used by the daily dashboard reconciliation to find jobs whose host summaries were not (fully)
    stored. Only jobs with at least one host summary are returned; a job that is missing from the
    result has no host summaries and the caller should treat its count as 0.

    Args:
        since: Start of the ``finished`` range (inclusive, timezone-aware)
        until: End of the ``finished`` range (exclusive, timezone-aware)
        statuses: Job statuses to include, any iterable of strings (e.g. ``['failed', 'successful']``)

    Returns:
        Tuple of (SQL query string with placeholders, [params]). Columns: ``job_id``,
        ``host_summary_count``; ordered by ``job_id``.

    Raises:
        TypeError: If *since* or *until* is not a datetime.
        ValueError: If *since* or *until* is timezone-naive, or *statuses* is invalid.
    """
    where_clause, params = _get_reconcile_where_clause(since, until, statuses)
    query = f"""
        SELECT hs.job_id, COUNT(*) AS host_summary_count
        FROM main_jobhostsummary hs
        JOIN main_unifiedjob uj ON uj.id = hs.job_id
        {where_clause}
        GROUP BY hs.job_id
        ORDER BY hs.job_id
    """
    return query, params


def get_job_label_counts_query(since: datetime, until: datetime, statuses: Iterable[str]) -> tuple[str, list]:
    """
    Return the number of labels per job.

    Used by the daily dashboard reconciliation to find jobs whose labels were not (fully) stored. Only jobs
    with at least one label are returned; a job that is missing from the result has no labels and the
    caller should treat its count as 0.

    Args:
        since: Start of the ``finished`` range (inclusive, timezone-aware)
        until: End of the ``finished`` range (exclusive, timezone-aware)
        statuses: Job statuses to include, any iterable of strings (e.g. ``['failed', 'successful']``)

    Returns:
        Tuple of (SQL query string with placeholders, [params]). Columns: ``job_id``, ``label_count``;
        ordered by ``job_id``.

    Raises:
        TypeError: If *since* or *until* is not a datetime.
        ValueError: If *since* or *until* is timezone-naive, or *statuses* is invalid.
    """
    where_clause, params = _get_reconcile_where_clause(since, until, statuses)
    query = f"""
        SELECT l.unifiedjob_id AS job_id, COUNT(*) AS label_count
        FROM main_unifiedjob_labels l
        JOIN main_unifiedjob uj ON uj.id = l.unifiedjob_id
        {where_clause}
        GROUP BY l.unifiedjob_id
        ORDER BY l.unifiedjob_id
    """
    return query, params
