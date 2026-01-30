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

    # Result is a dict with 'by_template' list and 'jobs_total'
    assert isinstance(result, dict)
    assert 'by_template' in result
    assert 'jobs_total' in result

    # Extract the by_template list
    by_template = result['by_template']
    assert isinstance(by_template, list)

    # There should be 3 templates (T1, T2, and T3 with never-started jobs)
    assert len(by_template) == 3

    # Identify records by job_template_name
    rec_t1 = next(r for r in by_template if r['job_template_name'] == 'T1')
    rec_t2 = next(r for r in by_template if r['job_template_name'] == 'T2')
    rec_t3 = next(r for r in by_template if r['job_template_name'] == 'T3')

    # T1 counts
    assert rec_t1['jobs_executed_total'] == 3
    assert rec_t1['jobs_failed_total'] == 1
    assert rec_t1['jobs_succeeded_total'] == 2
    assert rec_t1['jobs_never_started_total'] == 0

    # T1 durations (seconds): 3.0, 5.0, 2.0
    assert rec_t1['job_duration_maximum_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_t1['job_duration_minimum_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_t1['job_duration_total_seconds'] == pytest.approx(10.0, rel=1e-6)

    # T1 waiting times (seconds): 0.0, 2.0, 1.0
    assert rec_t1['job_waiting_time_maximum_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_t1['job_waiting_time_minimum_seconds'] == pytest.approx(0.0, rel=1e-6)
    assert rec_t1['job_waiting_time_total_seconds'] == pytest.approx(3.0, rel=1e-6)

    # T1 job_type (all jobs in T1 have model='job')
    assert rec_t1['job_type'] == 'job'

    # T2 counts
    assert rec_t2['jobs_executed_total'] == 1
    assert rec_t2['jobs_failed_total'] == 0
    assert rec_t2['jobs_succeeded_total'] == 1
    assert rec_t2['jobs_never_started_total'] == 0

    # T2 duration (seconds): 7.0
    assert rec_t2['job_duration_maximum_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_t2['job_duration_minimum_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_t2['job_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)

    # T2 waiting (seconds): 4.0
    assert rec_t2['job_waiting_time_maximum_seconds'] == pytest.approx(4.0, rel=1e-6)
    assert rec_t2['job_waiting_time_minimum_seconds'] == pytest.approx(4.0, rel=1e-6)
    assert rec_t2['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # T2 job_type
    assert rec_t2['job_type'] == 'workflowjob'

    # T3 counts (jobs that never started - should have NaN values for durations)
    assert rec_t3['jobs_executed_total'] == 1
    assert rec_t3['jobs_failed_total'] == 1
    assert rec_t3['jobs_succeeded_total'] == 0
    assert rec_t3['jobs_never_started_total'] == 1

    # T3 should have NaN for all duration metrics and 0 for totals
    assert pd.isna(rec_t3['job_duration_maximum_seconds'])
    assert pd.isna(rec_t3['job_duration_minimum_seconds'])
    assert rec_t3['job_duration_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # T3 should have NaN for all waiting time metrics and 0 for totals
    assert pd.isna(rec_t3['job_waiting_time_maximum_seconds'])
    assert pd.isna(rec_t3['job_waiting_time_minimum_seconds'])
    assert rec_t3['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # T3 job_type (all jobs in T3 have model='adhoccommand')
    assert rec_t3['job_type'] == 'adhoccommand'
