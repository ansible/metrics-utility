import datetime

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from django.db import connection

from metrics_utility.library.collectors.controller.unified_jobs_dashboard import unified_jobs_dashboard
from metrics_utility.library.collectors.util import CollectionOutput
from metrics_utility.test.library.dashboard import reconcile_data


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

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_no_pagination_by_default(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'main_unifiedjob.id >' not in query
        assert 'LIMIT' not in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_cursor_pagination(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, after_id=41, batch_size=500).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'AND main_unifiedjob.id > 41' in query
        assert query.index('ORDER BY main_unifiedjob.id ASC') < query.index('LIMIT 500')

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_accepts_numpy_integer_after_id(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, after_id=np.int64(41)).gather()
        assert 'AND main_unifiedjob.id > 41' in self._get_query(mock_copy_pandas)

    @pytest.mark.parametrize('batch_size', [0, -1, '5', True, 1.5])
    def test_rejects_invalid_batch_size(self, batch_size):
        collector = unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, batch_size=batch_size)
        with pytest.raises(ValueError, match='batch_size'):
            collector.gather()

    @pytest.mark.parametrize('after_id', ['1; DROP TABLE x', True, 1.5])
    def test_rejects_invalid_after_id(self, after_id):
        collector = unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, after_id=after_id)
        with pytest.raises(ValueError) as error:
            collector.gather()
        assert str(error.value) == f'after_id must be an integer, got {after_id!r}'

    def _get_params(self, mock_copy_pandas):
        return mock_copy_pandas.call_args[0][2]

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_no_job_ids_filter_by_default(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        assert 'ANY(' not in self._get_query(mock_copy_pandas)
        assert self._get_params(mock_copy_pandas) is None

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_job_ids_filter(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, job_ids=[3, 1, 2]).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'AND main_unifiedjob.id = ANY(%s::bigint[])' in query
        assert self._get_params(mock_copy_pandas) == [[3, 1, 2]]
        # IDs are bound as a parameter, never interpolated into the SQL
        assert '3, 1, 2' not in query
        assert 'ORDER BY main_unifiedjob.id ASC' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_job_ids_keep_the_same_columns(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until).gather()
        default_query = self._get_query(mock_copy_pandas)
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, job_ids=[1]).gather()
        job_ids_query = self._get_query(mock_copy_pandas)
        # The only difference is the ID filter: same columns, joins, subqueries and ordering.
        id_filter = 'AND main_unifiedjob.id = ANY(%s::bigint[])'
        assert job_ids_query.count(id_filter) == 1
        assert ' '.join(job_ids_query.replace(id_filter, '').split()) == ' '.join(default_query.split())

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_job_ids_without_window_select_by_id_only(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, job_ids=[1]).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'main_unifiedjob.finished >=' not in query
        assert 'main_unifiedjob.finished <' not in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_job_ids_combine_with_window_and_pagination(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, after_id=5, batch_size=10, job_ids=(7, 8)).gather()
        query = self._get_query(mock_copy_pandas)
        assert 'main_unifiedjob.finished >=' in query
        assert 'AND main_unifiedjob.id > 5' in query
        assert 'AND main_unifiedjob.id = ANY(%s::bigint[])' in query
        assert 'LIMIT 10' in query
        assert self._get_params(mock_copy_pandas) == [[7, 8]]

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_accepts_numpy_integer_job_ids(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, job_ids=[np.int64(4), 5]).gather()
        params = self._get_params(mock_copy_pandas)
        assert params == [[4, 5]]
        assert all(type(job_id) is int for job_id in params[0])

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_empty_job_ids_is_valid(self, mock_copy_pandas):
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, job_ids=[]).gather()
        assert self._get_params(mock_copy_pandas) == [[]]

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_accepts_iterable_job_ids(self, mock_copy_pandas):
        """Any iterable as_list accepts; see TestAsList for the full set."""
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, job_ids=pd.Series([4, 5])).gather()
        params = self._get_params(mock_copy_pandas)
        assert params == [[4, 5]]
        assert all(type(job_id) is int for job_id in params[0])

    def test_invalid_job_ids_error_names_only_the_offending_value(self):
        collector = unified_jobs_dashboard(db=self.mock_db, job_ids=[1, 'bad'])
        with pytest.raises(ValueError) as error:
            collector.gather()
        assert str(error.value) == "job_ids must contain integers, got 'bad'"

    def test_rejects_dataframe_job_ids(self):
        """Iterating a DataFrame yields column labels, which would silently fetch the wrong jobs."""
        collector = unified_jobs_dashboard(db=self.mock_db, job_ids=pd.DataFrame({0: [1]}))
        with pytest.raises(ValueError, match='job_ids must be an iterable of integers, got DataFrame'):
            collector.gather()

    def test_non_iterable_job_ids_error_names_only_the_type(self):
        collector = unified_jobs_dashboard(db=self.mock_db, job_ids='1,2')
        with pytest.raises(ValueError) as error:
            collector.gather()
        assert str(error.value) == 'job_ids must be an iterable of integers, got str'

    @pytest.mark.parametrize(
        'job_ids',
        [b'1', 5, [True], [np.True_], [1.5], ['1'], [None], [1, '2; DROP TABLE x'], [2**63], [-(2**63) - 1]],
    )
    def test_rejects_invalid_job_ids(self, job_ids):
        collector = unified_jobs_dashboard(db=self.mock_db, job_ids=job_ids)
        with pytest.raises(ValueError, match='job_ids'):
            collector.gather()

    def test_boolean_mask_is_rejected(self):
        """A comparison mask instead of the ID column must not be read as IDs 0 and 1 (older numpy accepts it)."""
        collector = unified_jobs_dashboard(db=self.mock_db, job_ids=np.array([False, True, True]))
        # The repr of the value differs between numpy versions (np.False_ / False), so only the prefix is checked.
        with pytest.raises(ValueError, match=r'^job_ids must contain integers, got '):
            collector.gather()

    def test_two_argument_output_adapter_works_without_job_ids(self):
        """Output adapters implementing only sql(db, query) keep working when job_ids is not passed."""

        class TwoArgumentOutput:
            def sql(self, db, query):
                return query

        query = unified_jobs_dashboard(db=self.mock_db, since=self.since, until=self.until, output=TwoArgumentOutput()).gather()
        assert 'FROM main_unifiedjob' in query

    @patch('metrics_utility.library.collectors.util._copy_table_pandas')
    def test_accepts_series_with_any_index_labels(self, mock_copy_pandas):
        """A Series whose index labels match DataFrame attributes (e.g. 'columns') is still a list of IDs."""
        mock_copy_pandas.return_value = pd.DataFrame()
        unified_jobs_dashboard(db=self.mock_db, job_ids=pd.Series([4, 6], index=['columns', 'id'])).gather()
        assert self._get_params(mock_copy_pandas) == [[4, 6]]


class TestUnifiedJobsDashboardDatabase:
    """Run the collector against fixed rows inserted into the Controller test database (rolled back afterwards)."""

    def test_job_ids_return_only_those_jobs_with_the_same_columns(self, reconcile_jobs):
        names = ['sync_successful', 'job_running', 'job_successful']
        job_ids = [reconcile_jobs.ids[name] for name in names]

        by_ids = unified_jobs_dashboard(db=connection, job_ids=job_ids).gather()
        by_window = unified_jobs_dashboard(db=connection, since=reconcile_data.SINCE, until=reconcile_data.UNTIL).gather()

        assert list(by_ids['id']) == sorted(job_ids)
        assert list(by_ids.columns) == list(by_window.columns)
        expected = by_window[by_window['id'].isin(job_ids)].reset_index(drop=True)
        pd.testing.assert_frame_equal(by_ids.reset_index(drop=True), expected)
        expected_hosts = {reconcile_jobs.ids['sync_successful']: 0, reconcile_jobs.ids['job_running']: 1, reconcile_jobs.ids['job_successful']: 2}
        assert dict(zip(by_ids['id'], by_ids['num_hosts'])) == expected_hosts

    def test_job_ids_combine_with_window(self, reconcile_jobs):
        job_ids = [reconcile_jobs.ids['job_successful'], reconcile_jobs.ids['job_at_until']]
        result = unified_jobs_dashboard(db=connection, since=reconcile_data.SINCE, until=reconcile_data.UNTIL, job_ids=job_ids).gather()

        assert list(result['id']) == [reconcile_jobs.ids['job_successful']]

    def test_empty_and_unknown_job_ids_return_no_rows_with_the_same_columns(self, reconcile_jobs):
        reference = unified_jobs_dashboard(db=connection, job_ids=[reconcile_jobs.ids['job_successful']]).gather()
        for job_ids in ([], [-1]):
            result = unified_jobs_dashboard(db=connection, job_ids=job_ids).gather()
            assert result.empty
            assert list(result.columns) == list(reference.columns)

    def test_job_ids_on_raw_connection(self, reconcile_jobs):
        """metrics-service passes the raw psycopg connection, not Django's wrapper."""
        job_ids = (reconcile_jobs.ids['job_successful'], reconcile_jobs.ids['sync_successful'])
        result = unified_jobs_dashboard(db=connection.connection, job_ids=job_ids).gather()

        assert list(result['id']) == sorted(job_ids)

    def test_job_ids_with_csv_output(self, reconcile_jobs, tmp_path):
        """The CSV output streams the query through COPY, which must bind the job_ids parameter too."""
        job_ids = [reconcile_jobs.ids['job_successful'], reconcile_jobs.ids['job_failed']]
        files = unified_jobs_dashboard(db=connection, job_ids=job_ids, output=CollectionOutput(str(tmp_path))).gather()

        frames = [pd.read_csv(path) for path in files]
        assert sorted(pd.concat(frames)['id']) == sorted(job_ids)
