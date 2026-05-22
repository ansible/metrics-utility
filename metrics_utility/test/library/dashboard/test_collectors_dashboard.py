import decimal

from datetime import datetime
from unittest.mock import patch

from metrics_utility.library.collectors.dashboard.collectors import _dashboard_job_host_summaries, _dashboard_job_labels, dashboard_jobs
from metrics_utility.test.util import mock_cursor_db, utcdt


SINCE = utcdt('2024-01-01')
UNTIL = utcdt('2024-02-01')

EXPECTED_JOB_LABELS = {1: [10, 11], 2: [20]}
EXPECTED_JOB_HOST_SUMMARIES = {
    1: [{'id': 1, 'host_name': 'host1', 'host_id': 100}, {'id': 2, 'host_name': 'host2', 'host_id': 200}],
    2: [{'id': 3, 'host_name': 'host3', 'host_id': None}],
}


def test_dashboard_job_labels():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('unifiedjob_id',), ('label_id',)]
    rows = [(1, 10), (1, 11), (2, 20)]
    mock_cursor.__iter__.return_value = iter(rows)
    result = _dashboard_job_labels(SINCE, UNTIL, mock_db)
    assert result == EXPECTED_JOB_LABELS


def test_dashboard_job_labels_no_results():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('unifiedjob_id',), ('label_id',)]
    rows = []
    mock_cursor.__iter__.return_value = iter(rows)
    result = _dashboard_job_labels(SINCE, UNTIL, mock_db)
    assert result == {}


def test_dashboard_job_host_summaries():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('id',), ('host_name',), ('host_id',), ('job_id',)]
    rows = [(1, 'host1', 100, 1), (2, 'host2', 200, 1), (3, 'host3', None, 2)]
    mock_cursor.__iter__.return_value = iter(rows)
    result = _dashboard_job_host_summaries(SINCE, UNTIL, mock_db)
    assert result == EXPECTED_JOB_HOST_SUMMARIES


def test_dashboard_job_host_summaries_no_results():
    mock_db, mock_cursor = mock_cursor_db()
    mock_cursor.description = [('id',), ('host_name',), ('host_id',), ('job_id',)]
    rows = []
    mock_cursor.__iter__.return_value = iter(rows)
    result = _dashboard_job_host_summaries(SINCE, UNTIL, mock_db)
    assert result == {}


@patch('metrics_utility.library.collectors.dashboard.collectors._dashboard_job_labels')
@patch('metrics_utility.library.collectors.dashboard.collectors._dashboard_job_host_summaries')
def test_dashboard_jobs(mock_dashboard_job_host_summaries, mock_dashboard_job_labels):
    mock_dashboard_job_labels.return_value = EXPECTED_JOB_LABELS
    mock_dashboard_job_host_summaries.return_value = EXPECTED_JOB_HOST_SUMMARIES
    mock_db, mock_cursor = mock_cursor_db()
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
    rows = [
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
    mock_cursor.__iter__.return_value = iter(rows)
    collector = dashboard_jobs(since=SINCE, until=UNTIL, db=mock_db)
    result = collector.gather()
    assert isinstance(result, dict)
    assert result['count'] == 3
    assert len(result['results']) == 3

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
            'labels': EXPECTED_JOB_LABELS[1],
            'host_summaries': EXPECTED_JOB_HOST_SUMMARIES[1],
            'num_hosts': len(EXPECTED_JOB_HOST_SUMMARIES[1]),
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
            'labels': EXPECTED_JOB_LABELS[2],
            'host_summaries': EXPECTED_JOB_HOST_SUMMARIES[2],
            'num_hosts': len(EXPECTED_JOB_HOST_SUMMARIES[2]),
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
