from datetime import datetime

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.jobs_anonymized_rollups import Jobs_Anonymized_Rollups


data = [
    # controller A, version v1, template T1
    {
        'id': 1,
        'started': datetime(2024, 1, 1, 0, 0, 0),
        'finished': datetime(2024, 1, 1, 0, 0, 3),  # +3s
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 3s, wait 1s
    {
        'id': 2,
        'started': datetime(2024, 1, 1, 0, 0, 10),
        'finished': datetime(2024, 1, 1, 0, 0, 15),  # +5s
        'failed': 1,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'job_created': datetime(2024, 1, 1, 0, 0, 8),  # wait 2s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
    },  # duration 5s (failed), wait 2s
    # controller A, version v1, template T2
    {
        'id': 3,
        'started': datetime(2024, 1, 1, 0, 1, 40),
        'finished': datetime(2024, 1, 1, 0, 1, 47),  # +7s
        'failed': 0,
        'job_template_name': 'T2',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
        'job_created': datetime(2024, 1, 1, 0, 1, 36),  # wait 4s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 7s, wait 4s
    # controller B, version v2, template T1
    {
        'id': 4,
        'started': datetime(2024, 1, 1, 0, 3, 20),
        'finished': datetime(2024, 1, 1, 0, 3, 22),  # +2s
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-B',
        'ansible_version': 'v2',
        'job_created': datetime(2024, 1, 1, 0, 3, 19),  # wait 1s
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
    },  # duration 2s, wait 1s
    # invalid rows (should be filtered out)
    {
        'id': 5,
        'started': datetime(2024, 1, 1, 0, 6, 40),
        'finished': None,
        'failed': 0,
        'job_template_name': 'T3',
        'controller_node': 'ctrl-C',
        'ansible_version': 'v3',
    },
    {
        'id': 6,
        'started': None,
        'finished': datetime(2024, 1, 1, 0, 8, 20),
        'failed': 0,
        'job_template_name': 'T3',
        'controller_node': 'ctrl-C',
        'ansible_version': 'v3',
    },
]


def test_jobs_anonymized_rollups_base_aggregation():
    # Build a DataFrame mimicking unified_jobs collector output columns we use
    # Times are in milliseconds epoch to match the code dividing by 1000

    df = pd.DataFrame(data)

    result = Jobs_Anonymized_Rollups.base(df)

    import pprint

    pprint.pprint(result)

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

    # T1 waiting times (seconds): 0.0, 2.0, 1.0
    assert pytest.approx(rec_t1['job_waiting_time_average_in_seconds'], rel=1e-6) == 1.0
    assert pytest.approx(rec_t1['job_waiting_time_maximum_in_seconds'], rel=1e-6) == 2.0
    assert pytest.approx(rec_t1['job_waiting_time_minimum_in_seconds'], rel=1e-6) == 0.0
    assert pytest.approx(rec_t1['job_waiting_time_total_in_seconds'], rel=1e-6) == 3.0

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
