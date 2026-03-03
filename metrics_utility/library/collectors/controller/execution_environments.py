from ..util import DataframeOutput, collector


@collector
def execution_environments(*, db=None, output=DataframeOutput()):
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
