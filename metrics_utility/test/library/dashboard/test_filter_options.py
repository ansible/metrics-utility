from unittest.mock import MagicMock

from metrics_utility.library.collectors.dashboard.filter_options import (
    fetch_job_templates,
    fetch_labels,
    fetch_organizations,
    fetch_projects,
    get_job_templates_query,
    get_labels_query,
    get_organizations_query,
    get_projects_query,
)


def _make_mock_db(rows):
    """Return a mock db whose cursor().fetchall() returns *rows*."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = rows
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    return mock_db, mock_cursor


class TestGetOrganizationsQuery:
    def test_queries_main_organization(self):
        sql, params = get_organizations_query()
        assert 'main_organization' in sql
        assert params == []

    def test_selects_id_and_name(self):
        sql, _ = get_organizations_query()
        assert 'id' in sql and 'name' in sql

    def test_ordered_by_name(self):
        sql, _ = get_organizations_query()
        assert 'ORDER BY name' in sql


class TestGetJobTemplatesQuery:
    def test_queries_main_unifiedjobtemplate(self):
        sql, params = get_job_templates_query()
        assert 'main_unifiedjobtemplate' in sql
        assert params == []

    def test_joins_main_jobtemplate(self):
        sql, _ = get_job_templates_query()
        assert 'main_jobtemplate' in sql
        assert 'JOIN' in sql

    def test_ordered_by_name(self):
        sql, _ = get_job_templates_query()
        assert 'ORDER BY' in sql and 'name' in sql


class TestGetProjectsQuery:
    def test_queries_main_unifiedjobtemplate(self):
        sql, params = get_projects_query()
        assert 'main_unifiedjobtemplate' in sql
        assert params == []

    def test_joins_main_project(self):
        sql, _ = get_projects_query()
        assert 'main_project' in sql
        assert 'JOIN' in sql

    def test_ordered_by_name(self):
        sql, _ = get_projects_query()
        assert 'ORDER BY' in sql and 'name' in sql


class TestGetLabelsQuery:
    def test_queries_main_label(self):
        sql, params = get_labels_query()
        assert 'main_label' in sql
        assert params == []

    def test_selects_id_and_name(self):
        sql, _ = get_labels_query()
        assert 'id' in sql and 'name' in sql

    def test_ordered_by_name(self):
        sql, _ = get_labels_query()
        assert 'ORDER BY name' in sql


class TestFetchOrganizations:
    def test_returns_id_name_dicts(self):
        mock_db, _ = _make_mock_db([(1, 'Default'), (2, 'Operations')])
        result = fetch_organizations(mock_db)
        assert result == [{'id': 1, 'name': 'Default'}, {'id': 2, 'name': 'Operations'}]

    def test_empty_result(self):
        mock_db, _ = _make_mock_db([])
        assert fetch_organizations(mock_db) == []

    def test_executes_expected_sql(self):
        mock_db, mock_cursor = _make_mock_db([])
        fetch_organizations(mock_db)
        called_sql = mock_cursor.execute.call_args[0][0]
        assert 'main_organization' in called_sql


class TestFetchJobTemplates:
    def test_returns_id_name_dicts(self):
        mock_db, _ = _make_mock_db([(10, 'Deploy'), (11, 'Smoke Test')])
        result = fetch_job_templates(mock_db)
        assert result == [{'id': 10, 'name': 'Deploy'}, {'id': 11, 'name': 'Smoke Test'}]

    def test_empty_result(self):
        mock_db, _ = _make_mock_db([])
        assert fetch_job_templates(mock_db) == []

    def test_executes_expected_sql(self):
        mock_db, mock_cursor = _make_mock_db([])
        fetch_job_templates(mock_db)
        called_sql = mock_cursor.execute.call_args[0][0]
        assert 'main_jobtemplate' in called_sql


class TestFetchProjects:
    def test_returns_id_name_dicts(self):
        mock_db, _ = _make_mock_db([(5, 'ansible-playbooks')])
        result = fetch_projects(mock_db)
        assert result == [{'id': 5, 'name': 'ansible-playbooks'}]

    def test_empty_result(self):
        mock_db, _ = _make_mock_db([])
        assert fetch_projects(mock_db) == []

    def test_executes_expected_sql(self):
        mock_db, mock_cursor = _make_mock_db([])
        fetch_projects(mock_db)
        called_sql = mock_cursor.execute.call_args[0][0]
        assert 'main_project' in called_sql


class TestFetchLabels:
    def test_returns_id_name_dicts(self):
        mock_db, _ = _make_mock_db([(42, 'production'), (43, 'staging')])
        result = fetch_labels(mock_db)
        assert result == [{'id': 42, 'name': 'production'}, {'id': 43, 'name': 'staging'}]

    def test_empty_result(self):
        mock_db, _ = _make_mock_db([])
        assert fetch_labels(mock_db) == []

    def test_executes_expected_sql(self):
        mock_db, mock_cursor = _make_mock_db([])
        fetch_labels(mock_db)
        called_sql = mock_cursor.execute.call_args[0][0]
        assert 'main_label' in called_sql
