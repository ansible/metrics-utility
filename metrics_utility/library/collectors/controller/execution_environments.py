from ..util import collector, copy_table


@collector
def execution_environments(*, db=None, output_dir=None):
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

    return copy_table(db=db, table='main_executionenvironment', query=query, output_dir=output_dir)
