"""Collector for credential counts grouped by credential type."""

from ..util import DictOutput, collector


@collector
def cred_type_counts(*, db=None, output=DictOutput()):
    """Return credential counts keyed by credential type ID."""
    query = """
        SELECT
            main_credentialtype.id,
            main_credentialtype.name,
            main_credentialtype.managed,
            COUNT(main_credential.id) AS num_credentials
        FROM main_credentialtype
        LEFT JOIN main_credential
            ON main_credential.credential_type_id = main_credentialtype.id
        GROUP BY main_credentialtype.id, main_credentialtype.name, main_credentialtype.managed
    """
    counts = {}
    with db.cursor() as cursor:
        cursor.execute(query)
        for cred_type_id, name, managed, num_credentials in cursor.fetchall():
            counts[cred_type_id] = {
                'name': name,
                'credential_count': num_credentials,
                'managed': managed,
            }
    return output.dict(counts)
