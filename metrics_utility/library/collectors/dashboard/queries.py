from datetime import datetime


def get_min_max_job_id_query(since: datetime, until: datetime) -> tuple[str, list]:
    """
    Return the min and max job IDs for the filtered window.

    Used to establish cursor bounds before a batched collection run.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    where_clause, params = get_where_clause(since, until)
    query = f"""
        SELECT MIN(uj.id) AS min_id, MAX(uj.id) AS max_id
        FROM main_unifiedjob uj
        {where_clause}
    """
    return query, params


def get_where_clause(since: datetime, until: datetime) -> tuple[str, list]:
    """
    Generate SQL WHERE clause for filtering jobs by job modification date range.
    Excludes sync jobs and includes only jobs with status 'failed' or 'successful'.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)

    Returns:
        Tuple of (SQL WHERE clause string with placeholders, [params])
    """
    where_clause = """
    WHERE uj.launch_type != %s
    AND (uj.status= %s OR uj.status = %s)
    AND uj.modified >= %s
    AND uj.modified < %s
    """
    params = ['sync', 'failed', 'successful', since.isoformat(), until.isoformat()]
    return where_clause, params


def get_job_labels_query(since: datetime, until: datetime) -> tuple[str, list]:
    """
    Generate SQL query to fetch job labels for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)

    Returns:
        Tuple of (SQL query string with placeholders, [params])

    Database schema:
        - main_unifiedjob_labels
        - main_unifiedjob (for filtering by date range)
    """
    where_clause, params = get_where_clause(since, until)
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


def get_job_host_summaries_query(since: datetime, until: datetime) -> tuple[str, list]:
    """
    Generate SQL query to fetch job host summaries for jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)

    Returns:
        Tuple of (SQL query string with placeholders, [params])

    Database schema:
        - main_jobhostsummary
        - main_unifiedjob (for filtering by date range)
    """
    where_clause, params = get_where_clause(since, until)
    query = f"""
         SELECT
            hs.id, hs.host_name, hs.host_id, hs.job_id
        FROM main_jobhostsummary hs
        JOIN main_unifiedjob uj on uj.id = hs.job_id {where_clause}
         order by hs.job_id
        """
    return query, params


def get_jobs_query(since: datetime, until: datetime) -> tuple[str, list]:
    """
    Generate SQL query to fetch jobs executed within the specified date range.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)

    Returns:
        Tuple of (SQL query string with placeholders, [params])

    Database schema:
        - main_unifiedjob
        - main_job
        - main_unifiedjobtemplate
        - auth_user
        - main_project
        - main_unifiedjobtemplate
    """
    where_clause, params = get_where_clause(since, until)
    query = f"""SELECT
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
    uj.modified
    FROM main_unifiedjob uj
    JOIN main_job mj on mj.unifiedjob_ptr_id = uj.id
    LEFT JOIN main_unifiedjobtemplate ujt ON ujt.id = uj.unified_job_template_id
    LEFT JOIN auth_user u on u.id = uj.created_by_id
    LEFT JOIN main_unifiedjobtemplate ujp on ujp.id = mj.project_id {where_clause} order by uj.modified"""
    return query, params


def get_jobs_batch_query(since: datetime, until: datetime, after_id: int, batch_size: int) -> tuple[str, list]:
    """
    Cursor-paginated variant of ``get_jobs_query``.

    Returns up to *batch_size* rows with ``id > after_id``, ordered by ``id``
    so successive calls advance the cursor without skipping or re-fetching rows.

    Args:
        since: Start of date range (inclusive)
        until: End of date range (exclusive)
        after_id: Exclusive lower bound on job ID (use 0 or min_id - 1 for first page)
        batch_size: Maximum number of rows to return

    Returns:
        Tuple of (SQL query string with placeholders, [params])
    """
    where_clause, params = get_where_clause(since, until)
    params += [after_id, batch_size]
    query = f"""SELECT
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
    uj.modified
    FROM main_unifiedjob uj
    JOIN main_job mj on mj.unifiedjob_ptr_id = uj.id
    LEFT JOIN main_unifiedjobtemplate ujt ON ujt.id = uj.unified_job_template_id
    LEFT JOIN auth_user u on u.id = uj.created_by_id
    LEFT JOIN main_unifiedjobtemplate ujp on ujp.id = mj.project_id
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
