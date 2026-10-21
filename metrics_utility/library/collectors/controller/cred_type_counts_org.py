"""Collector for credential counts grouped by owning organization and type."""

from ..util import DataframeOutput, collector


@collector
def cred_type_counts_org(*, db=None, output=DataframeOutput()):
    """Return credential counts by credential-owning organization and type.

    Credentials without an organization remain in a NULL organization group.
    """
    query = """
        SELECT
            main_credential.organization_id,
            main_organization.name AS organization_name,
            main_credentialtype.id AS credential_type_id,
            main_credentialtype.name AS credential_type,
            main_credentialtype.managed,
            COUNT(main_credential.id) AS credential_count
        FROM main_credential
        JOIN main_credentialtype
            ON main_credentialtype.id = main_credential.credential_type_id
        LEFT JOIN main_organization
            ON main_organization.id = main_credential.organization_id
        GROUP BY
            main_credential.organization_id,
            main_organization.name,
            main_credentialtype.id,
            main_credentialtype.name,
            main_credentialtype.managed
        ORDER BY organization_name NULLS LAST, organization_id, credential_type
    """

    return output.sql(db, query)
