import datetime

from unittest.mock import patch

import pandas as pd

from metrics_utility.library.collectors.controller.unified_jobs_dashboard import unified_jobs_dashboard


class TestUnifiedJobsDashboard:
    def setup_method(self):
        self.mock_db = None  # sql output adapter, not used directly in query tests
        self.since = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        self.until = datetime.datetime(2024, 2, 1, tzinfo=datetime.UTC)

    def _get_query(self, mock_copy_pandas):
        return mock_copy_pandas.call_args[0][1]

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_uses_finished_filter(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'main_unifiedjob.finished >=' in query
        assert 'main_unifiedjob.finished <' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_modified_column(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'main_unifiedjob.modified' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_project_id_and_name(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'project_id' in query
        assert 'project_name' in query
        assert 'ujp.name AS project_name' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_launched_by_fields(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'launched_by_id' in query
        assert 'launched_by_username' in query
        assert 'CASE' in query
        assert "'manual'" in query
        assert "'relaunch'" in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_label_ids_subquery(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'label_ids' in query
        assert 'STRING_AGG' in query
        assert 'main_unifiedjob_labels' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_num_hosts_subquery(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'num_hosts' in query
        assert 'COUNT(*)' in query
        assert 'main_jobhostsummary' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_auth_user_join(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'LEFT JOIN auth_user ON auth_user.id = main_unifiedjob.created_by_id' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_ujp_join_for_project_name(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'LEFT JOIN main_unifiedjobtemplate AS ujp' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_includes_base_unified_jobs_columns(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'main_unifiedjob' in query
        assert 'main_unifiedjobtemplate' in query
        assert 'django_content_type' in query
        assert 'main_job' in query
        assert 'main_organization' in query
        assert 'main_executionenvironment' in query
        assert 'main_inventory' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_does_not_filter_by_status(self, mock_copy_pandas):
        """Filtering to terminal states is the caller's responsibility, not the collector's."""
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert "status = 'failed'" not in query
        assert "status = 'successful'" not in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_does_not_exclude_sync_jobs(self, mock_copy_pandas):
        """Sync job filtering is handled by the dashboard task layer, not the collector."""
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert "launch_type != 'sync'" not in query
        assert "launch_type <> 'sync'" not in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_orders_by_id(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'ORDER BY main_unifiedjob.id ASC' in query
