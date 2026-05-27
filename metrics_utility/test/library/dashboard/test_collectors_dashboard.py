import decimal

from datetime import datetime
from unittest.mock import MagicMock, call, patch

from metrics_utility.library.collectors.dashboard.collectors import collect_dashboard_jobs, dashboard_jobs


class TestCollectorsDashboard:
    def setup_method(self):
        self.since = datetime.fromisoformat('2024-01-01T00:00:00Z')
        self.until = datetime.fromisoformat('2024-02-01T00:00:00Z')
        self.mock_db = MagicMock()
        self.expected_job_labels = {1: [10, 11], 2: [20]}
        self.expected_job_host_summaries = {
            1: [{'id': 1, 'host_name': 'host1', 'host_id': 100}, {'id': 2, 'host_name': 'host2', 'host_id': 200}],
            2: [{'id': 3, 'host_name': 'host3', 'host_id': None}],
        }

    def _make_jobs_cursor(self, rows=None):
        """Return a mock cursor that yields *rows* for the jobs query."""
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [
            ('id',),
            ('name',),
            ('unified_job_template_id',),
            ('organization_id',),
            ('started',),
            ('finished',),
            ('status',),
            ('elapsed',),
            ('launched_by_id',),
            ('launched_by_username',),
            ('project_id',),
            ('project_name',),
            ('created',),
            ('modified',),
        ]
        mock_cursor.__iter__.return_value = iter(rows or [])
        return mock_cursor

    def _make_labels_cursor(self, rows=None):
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [('unifiedjob_id',), ('label_id',)]
        mock_cursor.__iter__.return_value = iter(rows or [])
        return mock_cursor

    def _make_summaries_cursor(self, rows=None):
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [('id',), ('host_name',), ('host_id',), ('job_id',)]
        mock_cursor.__iter__.return_value = iter(rows or [])
        return mock_cursor

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_job_labels_for_ids_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.get_job_host_summaries_for_ids_query')
    def test_dashboard_jobs(self, mock_summaries_query, mock_labels_query):
        """Non-batched path uses SQL JOINs for enrichment (no in-memory helpers)."""
        mock_labels_query.return_value = ('SELECT 1', [])
        mock_summaries_query.return_value = ('SELECT 1', [])

        job_rows = [
            (
                1,
                'Job 1',
                1,
                1,
                datetime(2024, 1, 10),
                datetime(2024, 1, 10, 0, 5),
                'successful',
                decimal.Decimal('300.0'),
                1,
                'test_user',
                2,
                'test_project',
                datetime(2024, 1, 10),
                datetime(2024, 1, 10),
            ),
            (
                2,
                'Job 2',
                2,
                3,
                datetime(2024, 1, 15),
                datetime(2024, 1, 15, 0, 10),
                'failed',
                decimal.Decimal('600.0'),
                None,
                None,
                None,
                None,
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
            ),
            (
                3,
                'Job 3',
                None,
                None,
                datetime(2024, 1, 20),
                datetime(2024, 1, 20, 0, 15),
                'successful',
                decimal.Decimal('900.0'),
                None,
                None,
                None,
                None,
                datetime(2024, 1, 20),
                datetime(2024, 1, 20),
            ),
        ]
        label_rows = [(1, 10), (1, 11), (2, 20)]
        summary_rows = [(1, 'host1', 100, 1), (2, 'host2', 200, 1), (3, 'host3', None, 2)]

        self.mock_db.cursor.side_effect = [
            self._make_jobs_cursor(job_rows),
            self._make_labels_cursor(label_rows),
            self._make_summaries_cursor(summary_rows),
        ]

        collector = dashboard_jobs(since=self.since, until=self.until, db=self.mock_db)
        result = collector.gather()

        assert isinstance(result, dict)
        assert result['count'] == 3
        assert len(result['results']) == 3

        # Labels and summaries must be fetched via id-scoped SQL JOINs, not in-memory helpers.
        mock_labels_query.assert_called_once_with([1, 2, 3])
        mock_summaries_query.assert_called_once_with([1, 2, 3])

        expected_jobs = [
            {
                'id': 1,
                'name': 'Job 1',
                'unified_job_template_id': 1,
                'organization_id': 1,
                'started': datetime(2024, 1, 10),
                'finished': datetime(2024, 1, 10, 0, 5),
                'status': 'successful',
                'elapsed': decimal.Decimal('300.0'),
                'launched_by_id': 1,
                'launched_by_username': 'test_user',
                'project_id': 2,
                'project_name': 'test_project',
                'created': datetime(2024, 1, 10),
                'modified': datetime(2024, 1, 10),
                'labels': self.expected_job_labels[1],
                'host_summaries': self.expected_job_host_summaries[1],
                'num_hosts': len(self.expected_job_host_summaries[1]),
            },
            {
                'id': 2,
                'name': 'Job 2',
                'unified_job_template_id': 2,
                'organization_id': 3,
                'started': datetime(2024, 1, 15),
                'finished': datetime(2024, 1, 15, 0, 10),
                'status': 'failed',
                'elapsed': decimal.Decimal('600.0'),
                'launched_by_id': None,
                'launched_by_username': None,
                'project_id': None,
                'project_name': None,
                'created': datetime(2024, 1, 15),
                'modified': datetime(2024, 1, 15),
                'labels': self.expected_job_labels[2],
                'host_summaries': self.expected_job_host_summaries[2],
                'num_hosts': len(self.expected_job_host_summaries[2]),
            },
            {
                'id': 3,
                'name': 'Job 3',
                'unified_job_template_id': None,
                'organization_id': None,
                'started': datetime(2024, 1, 20),
                'finished': datetime(2024, 1, 20, 0, 15),
                'status': 'successful',
                'elapsed': decimal.Decimal('900.0'),
                'launched_by_id': None,
                'launched_by_username': None,
                'project_id': None,
                'project_name': None,
                'created': datetime(2024, 1, 20),
                'modified': datetime(2024, 1, 20),
                'labels': [],
                'host_summaries': [],
                'num_hosts': 0,
            },
        ]
        for i, job in enumerate(result['results']):
            for key, value in expected_jobs[i].items():
                assert job[key] == value, f"Mismatch for job {i + 1} field '{key}': {job[key]} != {value}"

    def test_dashboard_jobs_batch_empty(self):
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.__iter__.return_value = iter([])
        self.mock_db.cursor.return_value = mock_cursor

        collector = dashboard_jobs(since=self.since, until=self.until, db=self.mock_db, after_id=100, batch_size=10000)
        result = collector.gather()

        assert result == {'count': 0, 'results': []}

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_jobs_batch_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.get_job_labels_for_ids_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.get_job_host_summaries_for_ids_query')
    def test_dashboard_jobs_batch_uses_id_scoped_queries(self, mock_summaries_query, mock_labels_query, mock_batch_query):
        mock_batch_query.return_value = ('SELECT 1', [])
        mock_labels_query.return_value = ('SELECT 1', [])
        mock_summaries_query.return_value = ('SELECT 1', [])

        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [
            ('id',),
            ('name',),
            ('unified_job_template_id',),
            ('organization_id',),
            ('started',),
            ('finished',),
            ('status',),
            ('elapsed',),
            ('launched_by_id',),
            ('launched_by_username',),
            ('project_id',),
            ('project_name',),
            ('created',),
            ('modified',),
        ]
        mock_cursor.__iter__.return_value = iter(
            [
                (
                    1,
                    'Job 1',
                    1,
                    1,
                    datetime(2024, 1, 10),
                    datetime(2024, 1, 10, 0, 5),
                    'successful',
                    decimal.Decimal('300.0'),
                    1,
                    'user',
                    2,
                    'proj',
                    datetime(2024, 1, 10),
                    datetime(2024, 1, 10),
                ),
            ]
        )
        self.mock_db.cursor.return_value = mock_cursor

        collector = dashboard_jobs(since=self.since, until=self.until, db=self.mock_db, after_id=0, batch_size=5000)
        collector.gather()

        mock_batch_query.assert_called_once_with(self.since, self.until, 0, 5000, date_field='modified')
        mock_labels_query.assert_called_once_with([1])
        mock_summaries_query.assert_called_once_with([1])

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_jobs_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.get_job_labels_for_ids_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.get_job_host_summaries_for_ids_query')
    def test_dashboard_jobs_non_batched_uses_sql_joins(self, mock_summaries_query, mock_labels_query, mock_jobs_query):
        """Non-batched path must use id-scoped SQL JOINs for enrichment."""
        mock_jobs_query.return_value = ('SELECT 1', [])
        mock_labels_query.return_value = ('SELECT 1', [])
        mock_summaries_query.return_value = ('SELECT 1', [])

        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.description = [
            ('id',),
            ('name',),
            ('unified_job_template_id',),
            ('organization_id',),
            ('started',),
            ('finished',),
            ('status',),
            ('elapsed',),
            ('launched_by_id',),
            ('launched_by_username',),
            ('project_id',),
            ('project_name',),
            ('created',),
            ('modified',),
        ]
        # Provide one row so the early-return guard doesn't fire
        mock_cursor.__iter__.return_value = iter(
            [
                (
                    1,
                    'Job 1',
                    1,
                    1,
                    datetime(2024, 1, 10),
                    datetime(2024, 1, 10, 0, 5),
                    'successful',
                    decimal.Decimal('300.0'),
                    None,
                    None,
                    None,
                    None,
                    datetime(2024, 1, 10),
                    datetime(2024, 1, 10),
                ),
            ]
        )
        self.mock_db.cursor.return_value = mock_cursor

        collector = dashboard_jobs(since=self.since, until=self.until, db=self.mock_db)
        collector.gather()

        mock_jobs_query.assert_called_once_with(self.since, self.until, date_field='modified')
        # Enrichment must go through id-scoped queries, not full-window helpers.
        mock_labels_query.assert_called_once_with([1])
        mock_summaries_query.assert_called_once_with([1])

    def test_dashboard_jobs_raises_on_only_after_id(self):
        """Passing after_id without batch_size must raise rather than silently run a full-window query."""
        import pytest

        with pytest.raises(ValueError, match='after_id and batch_size'):
            dashboard_jobs(since=self.since, until=self.until, db=self.mock_db, after_id=100).gather()

    def test_dashboard_jobs_raises_on_only_batch_size(self):
        """Passing batch_size without after_id must raise rather than silently ignore the batch intent."""
        import pytest

        with pytest.raises(ValueError, match='after_id and batch_size'):
            dashboard_jobs(since=self.since, until=self.until, db=self.mock_db, batch_size=5000).gather()

    def test_dashboard_jobs_raises_on_zero_batch_size(self):
        """batch_size=0 must raise — a zero-row page would loop forever."""
        import pytest

        with pytest.raises(ValueError, match='batch_size must be greater than 0'):
            dashboard_jobs(since=self.since, until=self.until, db=self.mock_db, after_id=0, batch_size=0).gather()


class TestCollectDashboardJobs:
    def setup_method(self):
        self.since = datetime.fromisoformat('2024-01-01T00:00:00Z')
        self.until = datetime.fromisoformat('2024-02-01T00:00:00Z')
        self.mock_db = MagicMock()

    def _bounds_cursor(self, min_id, max_id):
        """Return a cursor whose fetchone() returns (min_id, max_id)."""
        c = MagicMock()
        c.__enter__.return_value = c
        c.fetchone.return_value = (min_id, max_id)
        return c

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_min_max_job_id_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.dashboard_jobs')
    def test_collect_dashboard_jobs_empty_window(self, mock_dj, mock_bounds_query):
        """Empty window (min_id=None) returns empty result without calling dashboard_jobs."""
        mock_bounds_query.return_value = ('SELECT 1', [])
        bounds_cursor = MagicMock()
        bounds_cursor.__enter__.return_value = bounds_cursor
        bounds_cursor.fetchone.return_value = (None, None)
        self.mock_db.cursor.return_value = bounds_cursor

        result = collect_dashboard_jobs(db=self.mock_db, since=self.since, until=self.until, page_size=100, max_records=0)

        assert result == {'count': 0, 'results': []}
        mock_dj.assert_not_called()

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_min_max_job_id_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.dashboard_jobs')
    def test_collect_dashboard_jobs_single_page(self, mock_dj, mock_bounds_query):
        """A window smaller than page_size is fetched in a single call."""
        mock_bounds_query.return_value = ('SELECT 1', [])
        bounds_cursor = self._bounds_cursor(1, 3)
        self.mock_db.cursor.return_value = bounds_cursor

        page_results = [
            {'id': 1, 'name': 'Job 1'},
            {'id': 2, 'name': 'Job 2'},
            {'id': 3, 'name': 'Job 3'},
        ]
        mock_instance = MagicMock()
        mock_instance.gather.return_value = {'count': 3, 'results': page_results}
        mock_dj.return_value = mock_instance

        result = collect_dashboard_jobs(db=self.mock_db, since=self.since, until=self.until, page_size=1000, max_records=0)

        assert result['count'] == 3
        assert result['results'] == page_results
        mock_dj.assert_called_once_with(
            db=self.mock_db,
            since=self.since,
            until=self.until,
            after_id=0,  # min_id(1) - 1
            batch_size=1000,
            date_field='modified',
        )

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_min_max_job_id_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.dashboard_jobs')
    def test_collect_dashboard_jobs_multi_page(self, mock_dj, mock_bounds_query):
        """Multiple pages are accumulated correctly."""
        mock_bounds_query.return_value = ('SELECT 1', [])
        bounds_cursor = self._bounds_cursor(1, 4)
        self.mock_db.cursor.return_value = bounds_cursor

        page1_results = [{'id': 1, 'name': 'Job 1'}, {'id': 2, 'name': 'Job 2'}]
        page2_results = [{'id': 3, 'name': 'Job 3'}, {'id': 4, 'name': 'Job 4'}]

        mock_instance1 = MagicMock()
        mock_instance1.gather.return_value = {'count': 2, 'results': page1_results}
        mock_instance2 = MagicMock()
        mock_instance2.gather.return_value = {'count': 2, 'results': page2_results}
        mock_instance3 = MagicMock()
        mock_instance3.gather.return_value = {'count': 0, 'results': []}
        mock_dj.side_effect = [mock_instance1, mock_instance2, mock_instance3]

        result = collect_dashboard_jobs(db=self.mock_db, since=self.since, until=self.until, page_size=2, max_records=0)

        assert result['count'] == 4
        assert result['results'] == page1_results + page2_results
        assert mock_dj.call_count == 3
        calls = mock_dj.call_args_list
        assert calls[0] == call(db=self.mock_db, since=self.since, until=self.until, after_id=0, batch_size=2, date_field='modified')
        assert calls[1] == call(db=self.mock_db, since=self.since, until=self.until, after_id=2, batch_size=2, date_field='modified')
        assert calls[2] == call(db=self.mock_db, since=self.since, until=self.until, after_id=4, batch_size=2, date_field='modified')

    @patch('metrics_utility.library.collectors.dashboard.collectors.get_min_max_job_id_query')
    @patch('metrics_utility.library.collectors.dashboard.collectors.dashboard_jobs')
    def test_collect_dashboard_jobs_max_records_cap(self, mock_dj, mock_bounds_query):
        """max_records stops pagination once the limit is reached."""
        mock_bounds_query.return_value = ('SELECT 1', [])
        bounds_cursor = self._bounds_cursor(1, 100)
        self.mock_db.cursor.return_value = bounds_cursor

        page1_results = [{'id': i, 'name': f'Job {i}'} for i in range(1, 6)]
        mock_instance = MagicMock()
        mock_instance.gather.return_value = {'count': 5, 'results': page1_results}
        mock_dj.return_value = mock_instance

        result = collect_dashboard_jobs(db=self.mock_db, since=self.since, until=self.until, page_size=10, max_records=5)

        assert result['count'] == 5
        assert len(result['results']) == 5
        # Only one page fetched before the cap kicks in.
        assert mock_dj.call_count == 1

    def test_collect_dashboard_jobs_raises_on_invalid_page_size(self):
        """page_size=0 must raise."""
        import pytest

        with pytest.raises(ValueError, match='page_size must be greater than 0'):
            collect_dashboard_jobs(db=self.mock_db, since=self.since, until=self.until, page_size=0)
