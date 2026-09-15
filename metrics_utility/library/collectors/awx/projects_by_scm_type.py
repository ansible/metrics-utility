"""Collector for project counts grouped by SCM type."""

from ..util import DictOutput, collector


@collector
def projects_by_scm_type(*, db=None, output=DictOutput()):
    """Return project counts keyed by SCM type."""
    query = """
        SELECT
            COALESCE(NULLIF(scm_type, ''), 'manual') AS scm_type,
            COUNT(*) AS count
        FROM main_project
        GROUP BY COALESCE(NULLIF(scm_type, ''), 'manual')
        ORDER BY scm_type
    """
    with db.cursor() as cursor:
        cursor.execute(query)
        counts = dict(cursor.fetchall())
    return output.dict(counts)
