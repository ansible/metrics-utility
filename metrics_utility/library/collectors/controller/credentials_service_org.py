"""Collector for managed credential types used by jobs, grouped by job organization."""

from ..util import DataframeOutput, collector, date_where


@collector
def credentials_service_org(*, db=None, since=None, until=None, output=DataframeOutput()):
    """Collect managed credential types used by jobs in each organization.

    Attribution follows the job's organization, not the credential's owning
    organization. Jobs without an organization remain in a NULL group.
    """
    query = f"""
        SELECT DISTINCT
            main_unifiedjob.organization_id,
            main_organization.name AS organization_name,
            main_credentialtype.name AS credential_type
        FROM main_unifiedjob_credentials
        JOIN main_unifiedjob
            ON main_unifiedjob.id = main_unifiedjob_credentials.unifiedjob_id
        JOIN main_credential
            ON main_credential.id = main_unifiedjob_credentials.credential_id
        JOIN main_credentialtype
            ON main_credentialtype.id = main_credential.credential_type_id
        LEFT JOIN main_organization
            ON main_organization.id = main_unifiedjob.organization_id
        WHERE {date_where('main_unifiedjob.finished', since, until)}
            AND main_credentialtype.managed = true
        ORDER BY organization_name NULLS LAST, organization_id, credential_type
    """

    return output.sql(db, query)
