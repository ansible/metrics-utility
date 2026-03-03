from ..util import DataframeOutput, collector


@collector
def credentials_service(*, db=None, since=None, until=None, output=DataframeOutput()):
    query = f"""
        SELECT DISTINCT
            main_credentialtype.name as credential_type
        FROM main_unifiedjob_credentials
        JOIN main_unifiedjob ON main_unifiedjob.id = main_unifiedjob_credentials.unifiedjob_id
        JOIN main_credential ON main_credential.id = main_unifiedjob_credentials.credential_id
        JOIN main_credentialtype ON main_credentialtype.id = main_credential.credential_type_id
        WHERE
            main_unifiedjob.finished >= '{since.isoformat()}'
            AND main_unifiedjob.finished < '{until.isoformat()}'
            AND main_credentialtype.managed = true
    """

    return output.sql(db, query)
