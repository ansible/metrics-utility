"""Collector for per-organization user and team counts."""

from ..util import DictOutput, collector


@collector
def org_counts(*, db=None, output=DictOutput()):
    """Return user and team counts keyed by organization ID."""
    query = """
        SELECT
            main_organization.id,
            main_organization.name,
            COUNT(DISTINCT main_rbac_roles_members.user_id) AS num_users,
            COUNT(DISTINCT main_team.id) AS num_teams
        FROM main_organization
        LEFT JOIN main_rbac_roles
            ON main_rbac_roles.id = main_organization.member_role_id
        LEFT JOIN main_rbac_roles_members
            ON main_rbac_roles_members.role_id = main_rbac_roles.id
        LEFT JOIN main_team
            ON main_team.organization_id = main_organization.id
        GROUP BY main_organization.id, main_organization.name
    """
    counts = {}
    with db.cursor() as cursor:
        cursor.execute(query)
        for org_id, name, num_users, num_teams in cursor.fetchall():
            counts[org_id] = {
                'name': name,
                'users': num_users,
                'teams': num_teams,
            }
    return output.dict(counts)
