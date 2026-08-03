from datetime import datetime

from metrics_utility.library.collectors.dashboard.queries import (
    get_job_host_summaries_for_ids_query,
    get_job_host_summaries_query,
    get_job_labels_for_ids_query,
    get_job_labels_query,
    get_jobs_batch_query,
    get_jobs_query,
    get_min_max_job_id_query,
    get_where_clause,
)


class TestQueriesDashboard:
    since = datetime.fromisoformat('2024-01-01T00:00:00Z')
    until = datetime.fromisoformat('2024-02-01T23:59:59Z')

    def test_where_clause_defaults_to_modified(self):
        result, params = get_where_clause(self.since, self.until)
        assert 'WHERE' in result
        assert 'uj.launch_type NOT IN (%s, %s)' in result
        assert 'AND (uj.status= %s OR uj.status = %s)' in result
        assert 'AND uj.modified >= %s' in result
        assert 'AND uj.modified < %s' in result
        expected_params = ['sync', 'workflow', 'failed', 'successful', '2024-01-01T00:00:00+00:00', '2024-02-01T23:59:59+00:00']
        assert params == expected_params

    def test_where_clause_finished(self):
        result, params = get_where_clause(self.since, self.until, date_field='finished')
        assert 'AND uj.finished >= %s' in result
        assert 'AND uj.finished < %s' in result
        assert 'modified' not in result

    def test_where_clause_invalid_date_field(self):
        import pytest

        with pytest.raises(ValueError, match='date_field must be'):
            get_where_clause(self.since, self.until, date_field='created')

    def test_get_job_labels_query(self):
        result, _ = get_job_labels_query(self.since, self.until)
        assert 'SELECT' in result
        assert 'FROM main_unifiedjob_labels l' in result
        assert 'JOIN main_unifiedjob uj on uj.id = l.unifiedjob_id' in result
        assert 'l.unifiedjob_id' in result
        assert 'l.label_id' in result

    def test_get_job_host_summaries_query(self):
        result, _ = get_job_host_summaries_query(self.since, self.until)
        assert 'SELECT' in result
        assert 'FROM main_jobhostsummary hs' in result
        assert 'JOIN main_unifiedjob uj on uj.id = hs.job_id' in result
        assert 'hs.id' in result
        assert 'hs.host_id' in result
        assert 'hs.job_id' in result
        assert 'hs.host_name' in result

    def test_get_jobs_query(self):
        result, _ = get_jobs_query(self.since, self.until)
        assert 'SELECT' in result
        assert 'FROM main_unifiedjob uj' in result
        assert 'JOIN main_job mj on mj.unifiedjob_ptr_id = uj.id' in result
        assert 'LEFT JOIN main_unifiedjobtemplate ujt ON ujt.id = uj.unified_job_template_id' in result
        assert 'LEFT JOIN auth_user u on u.id = uj.created_by_id' in result
        assert 'LEFT JOIN main_unifiedjobtemplate ujp on ujp.id = mj.project_id' in result
        assert 'order by uj.modified' in result
        assert 'uj.id' in result
        assert 'COALESCE(ujt.name, uj.name) as name' in result
        assert 'uj.unified_job_template_id' in result
        assert 'uj.organization_id' in result
        assert 'uj.started' in result
        assert 'uj.finished' in result
        assert 'uj.status' in result
        assert 'uj.elapsed' in result
        assert 'CASE' in result
        assert 'uj.launch_type' in result
        assert 'u.id' in result
        assert 'u.username' in result
        assert 'mj.project_id' in result
        assert 'ujp.name as project_name' in result
        assert 'uj.created' in result
        assert 'uj.modified' in result
        assert result.count('CASE') == 2

    def test_get_jobs_query_finished(self):
        result, _ = get_jobs_query(self.since, self.until, date_field='finished')
        assert 'AND uj.finished >= %s' in result
        assert 'AND uj.finished < %s' in result
        assert 'order by uj.finished' in result

    def test_get_min_max_job_id_query(self):
        result, params = get_min_max_job_id_query(self.since, self.until)
        assert 'MIN(uj.id) AS min_id' in result
        assert 'MAX(uj.id) AS max_id' in result
        assert 'FROM main_unifiedjob uj' in result
        # Must join main_job so bounds match the INNER JOIN in get_jobs_batch_query
        assert 'JOIN main_job mj ON mj.unifiedjob_ptr_id = uj.id' in result
        # Same base filter as get_where_clause
        assert 'uj.launch_type NOT IN (%s, %s)' in result
        assert 'uj.status' in result
        assert 'uj.modified' in result
        expected_params = ['sync', 'workflow', 'failed', 'successful', '2024-01-01T00:00:00+00:00', '2024-02-01T23:59:59+00:00']
        assert params == expected_params

    def test_get_jobs_batch_query(self):
        result, params = get_jobs_batch_query(self.since, self.until, after_id=500, batch_size=10000)
        # Cursor pagination clauses
        assert 'AND uj.id > %s' in result
        assert 'ORDER BY uj.id' in result
        assert 'LIMIT %s' in result
        assert 500 in params
        assert 10000 in params
        # Same column selection as full query
        assert 'COALESCE(ujt.name, uj.name) as name' in result
        assert 'uj.unified_job_template_id' in result
        assert 'uj.status' in result
        assert 'mj.project_id' in result
        # Same base filter
        assert 'uj.launch_type NOT IN (%s, %s)' in result

    def test_get_jobs_batch_query_zero_after_id(self):
        result, params = get_jobs_batch_query(self.since, self.until, after_id=0, batch_size=500)
        assert 0 in params
        assert 500 in params
        assert 'AND uj.id > %s' in result

    def test_get_job_labels_for_ids_query(self):
        job_ids = [1, 2, 3]
        result, params = get_job_labels_for_ids_query(job_ids)
        assert 'main_unifiedjob_labels' in result
        assert 'ANY(%s)' in result
        assert 'unifiedjob_id' in result
        assert 'label_id' in result
        assert 'ORDER BY' in result
        assert params == [job_ids]

    def test_get_job_labels_for_ids_query_empty_list(self):
        result, params = get_job_labels_for_ids_query([])
        assert 'ANY(%s)' in result
        assert params == [[]]

    def test_get_job_host_summaries_for_ids_query(self):
        job_ids = [10, 20, 30]
        result, params = get_job_host_summaries_for_ids_query(job_ids)
        assert 'main_jobhostsummary' in result
        assert 'ANY(%s)' in result
        assert 'host_name' in result
        assert 'host_id' in result
        assert 'job_id' in result
        assert 'ORDER BY' in result
        assert params == [job_ids]

    def test_get_job_host_summaries_for_ids_query_empty_list(self):
        result, params = get_job_host_summaries_for_ids_query([])
        assert 'ANY(%s)' in result
        assert params == [[]]
