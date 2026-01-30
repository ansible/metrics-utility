import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup


jobs = [
    # controller A, version v1, template T1
    {
        'id': 1,
        'started': '2024-01-01 00:00:00.000000+00',
        'finished': '2024-01-01 00:00:03.000000+00',  # +3s
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'created': '2024-01-01 00:00:00.000000+00',
        'model': 'job',
        'launch_type': 'manual',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 3s, wait 0s
    {
        'id': 2,
        'started': '2024-01-01 00:00:10.000000+00',
        'finished': '2024-01-01 00:00:15.000000+00',  # +5s
        'failed': 1,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'created': '2024-01-01 00:00:08.000000+00',  # wait 2s
        'model': 'job',
        'launch_type': 'scheduled',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
    },  # duration 5s (failed), wait 2s
    # controller A, version v1, template T2
    {
        'id': 3,
        'started': '2024-01-01 00:01:40.000000+00',
        'finished': '2024-01-01 00:01:47.000000+00',  # +7s
        'failed': 0,
        'job_template_name': 'T2',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'created': '2024-01-01 00:01:36.000000+00',  # wait 4s
        'model': 'workflowjob',
        'launch_type': 'workflow',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 7s, wait 4s
    # controller B, version v2, template T1
    {
        'id': 4,
        'started': '2024-01-01 00:03:20.000000+00',
        'finished': '2024-01-01 00:03:22.000000+00',  # +2s
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-B',
        'ansible_version': 'v2',
        'created': '2024-01-01 00:03:19.000000+00',  # wait 1s
        'model': 'job',
        'launch_type': 'callback',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 2s, wait 1s
    # invalid rows (should be filtered out)
    {
        'id': 5,
        'started': '2024-01-01 00:06:40.000000+00',
        'finished': None,
        'failed': 0,
        'job_template_name': 'T3',
        'controller_node': 'ctrl-C',
        'ansible_version': 'v3',
        'model': 'adhoccommand',
        'launch_type': 'manual',
    },
    {
        'id': 6,
        'started': None,
        'finished': '2024-01-01 00:08:20.000000+00',
        'failed': 1,
        'job_template_name': 'T3',
        'controller_node': 'ctrl-C',
        'ansible_version': 'v3',
        'model': 'adhoccommand',
        'launch_type': 'scheduled',
    },
]


def test_jobs_anonymized_rollups_base_aggregation():
    # Build a DataFrame mimicking unified_jobs collector output columns we use
    # Times are ISO-like strings with explicit UTC offset (+00)

    df = pd.DataFrame(jobs)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    df = jobs_anonymized_rollup.prepare(df)
    result = jobs_anonymized_rollup.base(df)
    result = result['json']

    import pprint

    pprint.pprint(result)

    # Result is a dict with 'by_job_type' list
    assert isinstance(result, dict)
    assert 'by_job_type' in result

    # Extract the by_job_type list
    by_job_type = result['by_job_type']
    assert isinstance(by_job_type, list)

    # There should be 3 job types: 'job', 'workflowjob', 'adhoccommand'
    assert len(by_job_type) == 3

    # Identify records by job_type
    rec_job = next(r for r in by_job_type if r['job_type'] == 'job')
    rec_workflowjob = next(r for r in by_job_type if r['job_type'] == 'workflowjob')
    rec_adhoccommand = next(r for r in by_job_type if r['job_type'] == 'adhoccommand')

    # 'job' type counts (ids 1, 2, 4 - 3 jobs total)
    assert rec_job['jobs_total'] == 3
    assert rec_job['jobs_failed_total'] == 1
    assert rec_job['jobs_succeeded_total'] == 2
    assert rec_job['jobs_never_started_total'] == 0
    assert rec_job['templates_total'] == 1  # All from template T1

    # 'job' type durations (seconds): 3.0, 5.0, 2.0
    assert rec_job['job_duration_maximum_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_job['job_duration_minimum_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_job['job_duration_total_seconds'] == pytest.approx(10.0, rel=1e-6)

    # 'job' type waiting times (seconds): 0.0, 2.0, 1.0
    assert rec_job['job_waiting_time_maximum_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_job['job_waiting_time_minimum_seconds'] == pytest.approx(0.0, rel=1e-6)
    assert rec_job['job_waiting_time_total_seconds'] == pytest.approx(3.0, rel=1e-6)

    # 'workflowjob' type counts (id 3 - 1 job)
    assert rec_workflowjob['jobs_total'] == 1
    assert rec_workflowjob['jobs_failed_total'] == 0
    assert rec_workflowjob['jobs_succeeded_total'] == 1
    assert rec_workflowjob['jobs_never_started_total'] == 0
    assert rec_workflowjob['templates_total'] == 1  # From template T2

    # 'workflowjob' type duration (seconds): 7.0
    assert rec_workflowjob['job_duration_maximum_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_workflowjob['job_duration_minimum_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_workflowjob['job_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)

    # 'workflowjob' type waiting (seconds): 4.0
    assert rec_workflowjob['job_waiting_time_maximum_seconds'] == pytest.approx(4.0, rel=1e-6)
    assert rec_workflowjob['job_waiting_time_minimum_seconds'] == pytest.approx(4.0, rel=1e-6)
    assert rec_workflowjob['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # 'adhoccommand' type counts (id 6 - 1 job that never started)
    assert rec_adhoccommand['jobs_total'] == 1
    assert rec_adhoccommand['jobs_failed_total'] == 1
    assert rec_adhoccommand['jobs_succeeded_total'] == 0
    assert rec_adhoccommand['jobs_never_started_total'] == 1
    assert rec_adhoccommand['templates_total'] == 1  # From template T3

    # 'adhoccommand' type should have NaN for all duration metrics and 0 for totals
    assert pd.isna(rec_adhoccommand['job_duration_maximum_seconds'])
    assert pd.isna(rec_adhoccommand['job_duration_minimum_seconds'])
    assert rec_adhoccommand['job_duration_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # 'adhoccommand' type should have NaN for all waiting time metrics and 0 for totals
    assert pd.isna(rec_adhoccommand['job_waiting_time_maximum_seconds'])
    assert pd.isna(rec_adhoccommand['job_waiting_time_minimum_seconds'])
    assert rec_adhoccommand['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)
