"""
Collector functions for AWX filter option entities.

Provides full-table snapshots of organizations, job templates, projects, and labels
from the Controller DB. Used by the metrics-service task system to populate local
cache tables so that filter dropdown endpoints have no runtime dependency on Controller.
"""

from __future__ import annotations

from typing import Any


def get_organizations_query() -> tuple[str, list]:
    """Return (sql, params) to fetch all AWX organizations."""
    return 'SELECT id, name FROM main_organization ORDER BY name', []


def get_job_templates_query() -> tuple[str, list]:
    """Return (sql, params) to fetch all AWX job templates."""
    return (
        'SELECT ujt.id, ujt.name FROM main_unifiedjobtemplate ujt JOIN main_jobtemplate jt ON jt.unifiedjobtemplate_ptr_id = ujt.id ORDER BY ujt.name'
    ), []


def get_projects_query() -> tuple[str, list]:
    """Return (sql, params) to fetch all AWX projects."""
    return (
        'SELECT ujt.id, ujt.name FROM main_unifiedjobtemplate ujt JOIN main_project pj ON pj.unifiedjobtemplate_ptr_id = ujt.id ORDER BY ujt.name'
    ), []


def get_labels_query() -> tuple[str, list]:
    """Return (sql, params) to fetch all AWX labels."""
    return 'SELECT id, name FROM main_label ORDER BY name', []


def _fetch_id_name(db: Any, query: str, params: list) -> list[dict]:
    """Execute a SELECT id, name query and return [{id, name}, ...] dicts."""
    with db.cursor() as cursor:
        cursor.execute(query, params)
        return [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]


def fetch_organizations(db: Any) -> list[dict]:
    """Fetch all AWX organizations as [{id, name}, ...] from the Controller DB."""
    query, params = get_organizations_query()
    return _fetch_id_name(db, query, params)


def fetch_job_templates(db: Any) -> list[dict]:
    """Fetch all AWX job templates as [{id, name}, ...] from the Controller DB."""
    query, params = get_job_templates_query()
    return _fetch_id_name(db, query, params)


def fetch_projects(db: Any) -> list[dict]:
    """Fetch all AWX projects as [{id, name}, ...] from the Controller DB."""
    query, params = get_projects_query()
    return _fetch_id_name(db, query, params)


def fetch_labels(db: Any) -> list[dict]:
    """Fetch all AWX labels as [{id, name}, ...] from the Controller DB."""
    query, params = get_labels_query()
    return _fetch_id_name(db, query, params)
