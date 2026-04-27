"""Collector for managed credential types used in jobs within a time window."""

from ..util import DataframeOutput, collector, date_where


@collector
def credentials_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect distinct managed credential type names for jobs finished in the time window.

    Args:
        db: Django database connection.
        since: Inclusive start datetime for the job ``finished`` filter.
        until: Exclusive end datetime for the job ``finished`` filter.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with a ``credential_type`` column, or list of CSV paths.
    """
    query = f"""
        SELECT DISTINCT
            main_credentialtype.name as credential_type
        FROM main_unifiedjob_credentials
        JOIN main_unifiedjob ON main_unifiedjob.id = main_unifiedjob_credentials.unifiedjob_id
        JOIN main_credential ON main_credential.id = main_unifiedjob_credentials.credential_id
        JOIN main_credentialtype ON main_credentialtype.id = main_credential.credential_type_id
        WHERE
            {date_where('main_unifiedjob.finished', since, until)}
            AND main_credentialtype.managed = true
    """

    return output.sql(db, query)
