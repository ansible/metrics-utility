from ..util import collector, copy_table


@collector
def controller_version_service(*, db=None):
    """
    Collect distinct controller versions from enabled instances.

    Returns a list of controller versions found in main_instance table
    where enabled = true and version is not null/empty.
    """
    query = """
        SELECT DISTINCT
            version as controller_version
        FROM main_instance
        WHERE enabled = true
            AND version IS NOT NULL
            AND version != ''
        ORDER BY version ASC
    """

    return copy_table(db=db, query=query)
