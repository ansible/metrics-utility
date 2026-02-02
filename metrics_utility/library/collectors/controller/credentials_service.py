from ..util import collector, copy_table


@collector
def credentials_service(*, db=None, since=None, until=None, output_dir=None):
    query = f"""
        SELECT
            main_credentialtype.name as credential_type,
            main_unifiedjob_credentials.unifiedjob_id as job_id,
            django_content_type.model
        FROM main_unifiedjob_credentials
        JOIN main_unifiedjob ON main_unifiedjob.id = main_unifiedjob_credentials.unifiedjob_id
        JOIN main_credential ON main_credential.id = main_unifiedjob_credentials.credential_id
        JOIN main_credentialtype ON main_credentialtype.id = main_credential.credential_type_id
        LEFT JOIN django_content_type ON main_unifiedjob.polymorphic_ctype_id = django_content_type.id
        WHERE
            main_unifiedjob.finished >= '{since.isoformat()}'
            AND main_unifiedjob.finished < '{until.isoformat()}'
            AND main_unifiedjob.launch_type != 'sync'
        ORDER BY main_unifiedjob.id ASC, main_credentialtype.name ASC
    """

    return copy_table(db=db, table='credentials', query=query, output_dir=output_dir)
