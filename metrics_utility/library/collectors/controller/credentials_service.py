"""Collector for managed credential types used in jobs within a time window."""

from ..util import DataframeOutput, collector, date_where


@collector
def credentials_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect credential types and independent credential/job usage counts.

    Args:
        db: Django database connection.
        since: Inclusive start datetime for the job ``finished`` filter.
        until: Exclusive end datetime for the job ``finished`` filter.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with credential type and count columns, or list of CSV paths.
    """
    finished_where = date_where('main_unifiedjob.finished', since, until)
    query = f"""
        WITH credential_counts AS (
            SELECT
                main_credential.credential_type_id,
                COUNT(*) AS credential_count
            FROM main_credential
            GROUP BY main_credential.credential_type_id
        ), used_by_finished_jobs AS (
            SELECT
                main_credential.credential_type_id,
                COUNT(DISTINCT main_unifiedjob.id) AS used_by_finished_job_count
            FROM main_unifiedjob_credentials
            JOIN main_unifiedjob
                ON main_unifiedjob.id = main_unifiedjob_credentials.unifiedjob_id
            JOIN main_credential
                ON main_credential.id = main_unifiedjob_credentials.credential_id
            WHERE {finished_where}
            GROUP BY main_credential.credential_type_id
        )
        SELECT
            main_credentialtype.id,
            main_credentialtype.name,
            main_credentialtype.name AS credential_type,
            main_credentialtype.managed,
            COALESCE(credential_counts.credential_count, 0) AS credential_count,
            COALESCE(used_by_finished_jobs.used_by_finished_job_count, 0) AS used_by_finished_job_count
        FROM main_credentialtype
        LEFT JOIN credential_counts
            ON credential_counts.credential_type_id = main_credentialtype.id
        LEFT JOIN used_by_finished_jobs
            ON used_by_finished_jobs.credential_type_id = main_credentialtype.id
    """

    return output.sql(db, query)
