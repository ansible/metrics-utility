"""Collector for execution environment records from the Controller database."""

from ..util import DataframeOutput, collector


@collector
def execution_environments(*, db=None, output=DataframeOutput()):
    """Collect all execution environments from the Controller database.

    This is a snapshot collector — it returns all rows without a time-window filter.

    Args:
        db: Django database connection.
        output: Output adapter (defaults to :class:`~..util.DataframeOutput`).

    Returns:
        pandas DataFrame with execution environment fields, or list of CSV paths.
    """
    query = """
        SELECT
            id,
            created,
            modified,
            description,
            image,
            managed,
            created_by_id,
            credential_id,
            modified_by_id,
            organization_id,
            name,
            pull
        FROM main_executionenvironment
    """

    return output.sql(db, query)
