import pandas as pd
import pytest

from metrics_utility.rollups.jobs_anonymized_rollups import Jobs_Anonymized_Rollups


data = [
    # controller A, version v1, template T1
    {
        'started': 1_000_000,
        'finished': 1_003_000,
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'job_created': 999_000,  # waiting 1.0s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 3000 ms -> 3.0 s
    {
        'started': 1_010_000,
        'finished': 1_015_000,
        'failed': 1,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'job_created': 1_008_000,  # waiting 2.0s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
    },  # duration 5000 ms -> 5.0 s (failed)
    # controller A, version v1, template T2
    {
        'started': 2_000_000,
        'finished': 2_007_000,
        'failed': 0,
        'job_template_name': 'T2',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'job_created': 1_996_000,  # waiting 4.0s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 7000 ms -> 7.0 s
    # controller B, version v2, template T1
    {
        'started': 3_000_000,
        'finished': 3_002_000,
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-B',
        'ansible_version': 'v2',
        'job_created': 2_999_500,  # waiting 0.5s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 2000 ms -> 2.0 s
    # Row with missing finished should be filtered out
    {'started': 4_000_000, 'finished': None, 'failed': 0, 'job_template_name': 'T3', 'controller_node': 'ctrl-C', 'ansible_version': 'v3'},
    # Row with missing started should be filtered out
    {'started': None, 'finished': 5_000_000, 'failed': 0, 'job_template_name': 'T3', 'controller_node': 'ctrl-C', 'ansible_version': 'v3'},
]


def test_jobs_anonymized_rollups_base_aggregation():
    # Build a DataFrame mimicking unified_jobs collector output columns we use
    # Times are in milliseconds epoch to match the code dividing by 1000

    df = pd.DataFrame(data)

    result = Jobs_Anonymized_Rollups.base(df)

    # New version returns list of per-template aggregates
    assert isinstance(result, list)

    # There should be 2 templates (T1 and T2); rows with missing timestamps are filtered
    assert len(result) == 2

    # Identify records by count of jobs (T1 has 3, T2 has 1)
    rec_t1 = next(r for r in result if r['number_of_jobs_executed'] == 3)
    rec_t2 = next(r for r in result if r['number_of_jobs_executed'] == 1)

    # T1 counts
    assert rec_t1['number_of_jobs_failed'] == 1
    assert rec_t1['number_of_jobs_succeeded'] == 2

    # T1 durations (seconds): 3.0, 5.0, 2.0
    assert pytest.approx(rec_t1['job_duration_average_in_seconds'], rel=1e-6) == 10 / 3
    assert pytest.approx(rec_t1['job_duration_maximum_in_seconds'], rel=1e-6) == 5.0
    assert pytest.approx(rec_t1['job_duration_minimum_in_seconds'], rel=1e-6) == 2.0
    assert pytest.approx(rec_t1['job_duration_total_in_seconds'], rel=1e-6) == 10.0

    # T1 waiting times (seconds): 1.0, 2.0, 0.5
    assert pytest.approx(rec_t1['job_waiting_time_average_in_seconds'], rel=1e-6) == (1.0 + 2.0 + 0.5) / 3
    assert pytest.approx(rec_t1['job_waiting_time_maximum_in_seconds'], rel=1e-6) == 2.0
    assert pytest.approx(rec_t1['job_waiting_time_minimum_in_seconds'], rel=1e-6) == 0.5
    assert pytest.approx(rec_t1['job_waiting_time_total_in_seconds'], rel=1e-6) == 3.5

    # T2 counts
    assert rec_t2['number_of_jobs_failed'] == 0
    assert rec_t2['number_of_jobs_succeeded'] == 1

    # T2 duration (seconds): 7.0
    assert pytest.approx(rec_t2['job_duration_average_in_seconds'], rel=1e-6) == 7.0
    assert pytest.approx(rec_t2['job_duration_maximum_in_seconds'], rel=1e-6) == 7.0
    assert pytest.approx(rec_t2['job_duration_minimum_in_seconds'], rel=1e-6) == 7.0
    assert pytest.approx(rec_t2['job_duration_total_in_seconds'], rel=1e-6) == 7.0

    # T2 waiting (seconds): 4.0
    assert pytest.approx(rec_t2['job_waiting_time_average_in_seconds'], rel=1e-6) == 4.0
    assert pytest.approx(rec_t2['job_waiting_time_maximum_in_seconds'], rel=1e-6) == 4.0
    assert pytest.approx(rec_t2['job_waiting_time_minimum_in_seconds'], rel=1e-6) == 4.0
    assert pytest.approx(rec_t2['job_waiting_time_total_in_seconds'], rel=1e-6) == 4.0
