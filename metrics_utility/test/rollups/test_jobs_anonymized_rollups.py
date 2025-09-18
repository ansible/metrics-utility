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
    },  # duration 3000 ms -> 3.0 s
    {
        'started': 1_010_000,
        'finished': 1_015_000,
        'failed': 1,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
    },  # duration 5000 ms -> 5.0 s (failed)
    # controller A, version v1, template T2
    {
        'started': 2_000_000,
        'finished': 2_007_000,
        'failed': 0,
        'job_template_name': 'T2',
        'controller_node': 'ctrl-A',
        'ansible_version': 'v1',
    },  # duration 7000 ms -> 7.0 s
    # controller B, version v2, template T1
    {
        'started': 3_000_000,
        'finished': 3_002_000,
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-B',
        'ansible_version': 'v2',
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

    from pprint import pprint

    print('\n')
    pprint(result)

    # counts
    assert result['number_of_jobs_executed'] == 4
    assert result['number_of_jobs_failed'] == 1
    assert result['number_of_jobs_succeeded'] == 3

    # durations by template (seconds)
    # T1 durations: 3.0, 5.0, 2.0 -> avg= (3+5+2)/3= 10/3 ≈ 3.333..., min=2.0, max=5.0, sum=10.0
    # T2 durations: 7.0
    avg_by_template = result['job_duration_average_in_seconds_by_template']
    max_by_template = result['job_duration_maximum_seconds_by_template']
    min_by_template = result['job_duration_minimum_seconds_by_template']
    sum_by_template = result['job_total_seconds_by_template']

    assert pytest.approx(avg_by_template['T1'], rel=1e-6) == 10 / 3
    assert pytest.approx(avg_by_template['T2'], rel=1e-6) == 7.0

    assert pytest.approx(max_by_template['T1'], rel=1e-6) == 5.0
    assert pytest.approx(max_by_template['T2'], rel=1e-6) == 7.0

    assert pytest.approx(min_by_template['T1'], rel=1e-6) == 2.0
    assert pytest.approx(min_by_template['T2'], rel=1e-6) == 7.0

    assert pytest.approx(sum_by_template['T1'], rel=1e-6) == 10.0
    assert pytest.approx(sum_by_template['T2'], rel=1e-6) == 7.0

    # active customers and by controller version
    assert result['active_number_of_customers'] == 2  # ctrl-A, ctrl-B

    by_version = result['active_number_of_clusters_by_controller_version']
    # v1 has ctrl-A once, v2 has ctrl-B once
    assert by_version == {'v1': 1, 'v2': 1}

    # number of templates executed by company (controller node)
    # ctrl-A used templates {T1, T2} = 2, ctrl-B used {T1} = 1
    templates_by_company = result['number_of_templates_executed_by_company']
    assert templates_by_company == {'ctrl-A': 2, 'ctrl-B': 1}
