from metrics_utility.library.collectors.dashboard.queries import get_job_host_summaries_query, get_job_labels_query, get_jobs_query, get_where_clause
from metrics_utility.test.util import utcdt


SINCE = utcdt('2024-01-01')
UNTIL = utcdt('2024-02-01T23:59:59')


def test_where_clause():
    result, params = get_where_clause(SINCE, UNTIL)
    assert 'WHERE' in result
    assert 'uj.launch_type != %s' in result
    assert 'AND (uj.status= %s OR uj.status = %s)' in result
    assert 'AND uj.modified >= %s' in result
    assert 'AND uj.modified < %s' in result
    expected_params = ['sync', 'failed', 'successful', '2024-01-01T00:00:00+00:00', '2024-02-01T23:59:59+00:00']
    assert params == expected_params


def test_get_job_labels_query():
    result, _ = get_job_labels_query(SINCE, UNTIL)
    assert 'SELECT' in result
    assert 'FROM main_unifiedjob_labels l' in result
    assert 'JOIN main_unifiedjob uj on uj.id = l.unifiedjob_id' in result
    assert 'l.unifiedjob_id' in result
    assert 'l.label_id' in result


def test_get_job_host_summaries_query():
    result, _ = get_job_host_summaries_query(SINCE, UNTIL)
    assert 'SELECT' in result
    assert 'FROM main_jobhostsummary hs' in result
    assert 'JOIN main_unifiedjob uj on uj.id = hs.job_id' in result
    assert 'hs.id' in result
    assert 'hs.host_id' in result
    assert 'hs.job_id' in result
    assert 'hs.host_name' in result


def test_get_jobs_query():
    result, _ = get_jobs_query(SINCE, UNTIL)
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
