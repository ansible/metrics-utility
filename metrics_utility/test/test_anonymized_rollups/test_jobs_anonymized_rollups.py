import json

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup


jobs = [
    # controller A, ansible 2.9.0, template T1
    {
        'id': 1,
        'started': '2024-01-01 00:00:00.000000+00',
        'finished': '2024-01-01 00:00:03.000000+00',  # +3s
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': '2.9.0',
        'organization_name': 'Org1',
        'created': '2024-01-01 00:00:00.000000+00',
        'model': 'job',
        'launch_type': 'manual',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 5,
        'inventory_name': 'inventory1',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.9.10'},
            'community.general': {'version': '1.0.0'},
        }),
    },  # duration 3s, wait 0s
    {
        'id': 2,
        'started': '2024-01-01 00:00:10.000000+00',
        'finished': '2024-01-01 00:00:15.000000+00',  # +5s
        'failed': 1,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-A',
        'ansible_version': '2.10.0',
        'organization_name': 'Org1',
        'created': '2024-01-01 00:00:08.000000+00',  # wait 2s
        'model': 'job',
        'launch_type': 'scheduled',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
        'forks': 10,
        'inventory_name': 'inventory1',
        'scm_type': 'svn',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.9.10'},  # Same version as job 1
            'community.general': {'version': '2.0.0'},  # Different version - same collection
            'ansible.windows': {'version': '1.0.0'},
        }),
    },  # duration 5s (failed), wait 2s
    # controller A, ansible 2.11.0, template T2
    {
        'id': 3,
        'started': '2024-01-01 00:01:40.000000+00',
        'finished': '2024-01-01 00:01:47.000000+00',  # +7s
        'failed': 0,
        'job_template_name': 'T2',
        'controller_node': 'ctrl-A',
        'ansible_version': '2.11.0',
        'organization_name': 'Org2',
        'created': '2024-01-01 00:01:36.000000+00',  # wait 4s
        'model': 'workflowjob',
        'launch_type': 'workflow',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 20,
        'inventory_name': 'inventory2',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.9.10'},  # Same version as jobs 1 and 2
            'community.general': {'version': '2.0.0'},  # Same version as job 2
            'community.aws': {'version': '1.5.0'},
        }),
    },  # duration 7s, wait 4s
    # controller B, ansible 2.12.0, template T1
    {
        'id': 4,
        'started': '2024-01-01 00:03:20.000000+00',
        'finished': '2024-01-01 00:03:22.000000+00',  # +2s
        'failed': 0,
        'job_template_name': 'T1',
        'controller_node': 'ctrl-B',
        'ansible_version': '2.12.0',
        'organization_name': 'Org1',
        'created': '2024-01-01 00:03:19.000000+00',  # wait 1s
        'model': 'job',
        'launch_type': 'callback',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 15,
        'inventory_name': 'inventory1',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.9.10'},  # Same version as other jobs
            'community.general': {'version': '1.0.0'},  # Same version as job 1
        }),
    },  # duration 2s, wait 1s
    # invalid rows (should be filtered out)
    {
        'id': 5,
        'started': '2024-01-01 00:06:40.000000+00',
        'finished': None,
        'failed': 0,
        'job_template_name': 'T3',
        'controller_node': 'ctrl-C',
        'ansible_version': '2.13.0',
        'organization_name': 'Org3',
        'model': 'adhoccommand',
        'launch_type': 'manual',
        'forks': 0,
        'inventory_name': 'inventory3',
        'scm_type': 'manual',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.9.10'},
        }),
    },
    {
        'id': 6,
        'started': None,
        'finished': '2024-01-01 00:08:20.000000+00',
        'failed': 1,
        'job_template_name': 'T3',
        'controller_node': 'ctrl-C',
        'ansible_version': '2.14.0',
        'organization_name': 'Org3',
        'model': 'adhoccommand',
        'launch_type': 'scheduled',
        'forks': 0,
        'inventory_name': 'inventory3',
        'scm_type': 'unknown',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.9.10'},
            'community.general': {'version': '3.0.0'},  # Another version of community.general
        }),
    },
]


def test_jobs_anonymized_rollups_base_aggregation():
    # Build a DataFrame mimicking unified_jobs collector output columns we use
    # Times are ISO-like strings with explicit UTC offset (+00)

    df = pd.DataFrame(jobs)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    prepared_data = jobs_anonymized_rollup.prepare(df)
    result = jobs_anonymized_rollup.base(prepared_data)
    result = result['json']

    import pprint

    pprint.pprint(result)

    # Result is a dict with 'by_job_type' list and top-level fields
    assert isinstance(result, dict)
    assert 'by_job_type' in result
    assert 'organizations_total' in result
    assert 'ansible_version' in result

    # Check top-level fields
    assert result['organizations_total'] == 3  # Org1, Org2, and Org3 (job 5 filtered out, but job 6 with Org3 remains)
    assert result['ansible_version'] == '2.9.0'  # First ansible_version in dataframe

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


def test_jobs_anonymized_rollups_ansible_version():
    """Test that ansible_version and organizations_total are correctly aggregated at top level."""
    df = pd.DataFrame(jobs)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    prepared_data = jobs_anonymized_rollup.prepare(df)
    result = jobs_anonymized_rollup.base(prepared_data)
    result = result['json']

    # Verify top-level fields are present
    assert 'ansible_version' in result
    assert 'organizations_total' in result
    assert result['ansible_version'] is not None
    assert result['organizations_total'] is not None

    # Verify ansible_version uses 'first' value from dataframe (first job: id 1 with '2.9.0')
    assert result['ansible_version'] == '2.9.0'

    # Verify organizations_total counts unique organizations (Org1, Org2, and Org3 - job 5 filtered out, but job 6 with Org3 remains)
    assert result['organizations_total'] == 3


def test_jobs_anonymized_rollups_ansible_version_multiple_per_type():
    """Test ansible_version aggregation when multiple versions exist."""
    test_jobs = [
        {
            'id': 1,
            'started': '2024-01-01 00:00:00.000000+00',
            'finished': '2024-01-01 00:00:03.000000+00',
            'failed': 0,
            'job_template_name': 'T1',
            'controller_node': 'ctrl-A',
            'ansible_version': '2.9.0',
            'organization_name': 'Org1',
            'created': '2024-01-01 00:00:00.000000+00',
            'model': 'job',
            'launch_type': 'manual',
            'forks': 5,
            'inventory_name': 'inventory1',
            'scm_type': 'git',
        },
        {
            'id': 2,
            'started': '2024-01-01 00:00:10.000000+00',
            'finished': '2024-01-01 00:00:15.000000+00',
            'failed': 0,
            'job_template_name': 'T2',
            'controller_node': 'ctrl-B',
            'ansible_version': '2.10.0',
            'organization_name': 'Org2',
            'created': '2024-01-01 00:00:08.000000+00',
            'model': 'job',
            'launch_type': 'scheduled',
            'forks': 10,
            'inventory_name': 'inventory2',
            'scm_type': 'svn',
        },
        {
            'id': 3,
            'started': '2024-01-01 00:01:00.000000+00',
            'finished': '2024-01-01 00:01:05.000000+00',
            'failed': 0,
            'job_template_name': 'T3',
            'controller_node': 'ctrl-C',
            'ansible_version': '2.11.0',
            'organization_name': 'Org3',
            'created': '2024-01-01 00:00:58.000000+00',
            'model': 'job',
            'launch_type': 'callback',
            'forks': 15,
            'inventory_name': 'inventory3',
            'scm_type': 'git',
        },
    ]

    df = pd.DataFrame(test_jobs)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    prepared_data = jobs_anonymized_rollup.prepare(df)
    result = jobs_anonymized_rollup.base(prepared_data)
    result = result['json']

    by_job_type = result['by_job_type']
    rec_job = next(r for r in by_job_type if r['job_type'] == 'job')

    # Should use 'first' value from dataframe (first job: id 1 with '2.9.0')
    assert result['ansible_version'] == '2.9.0'
    assert result['organizations_total'] == 3  # Org1, Org2, Org3
    assert rec_job['jobs_total'] == 3  # All three jobs are included


def test_jobs_anonymized_rollups_installed_collections():
    """Test that installed collections are correctly extracted and counted."""
    df = pd.DataFrame(jobs)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    prepared_data = jobs_anonymized_rollup.prepare(df)
    result = jobs_anonymized_rollup.base(prepared_data)
    result = result['json']

    # Verify installed_collections field exists
    assert 'installed_collections' in result
    installed_collections = result['installed_collections']
    assert isinstance(installed_collections, list)

    # Expected collections from jobs 1-4 and 6 (job 5 is filtered out because finished is None):
    # Job 1: ansible.builtin 2.9.10, community.general 1.0.0
    # Job 2: ansible.builtin 2.9.10, community.general 2.0.0, ansible.windows 1.0.0
    # Job 3: ansible.builtin 2.9.10, community.general 2.0.0, community.aws 1.5.0
    # Job 4: ansible.builtin 2.9.10, community.general 1.0.0
    # Job 6: ansible.builtin 2.9.10, community.general 3.0.0

    # Expected counts:
    # ansible.builtin 2.9.10: 5 jobs (1, 2, 3, 4, 6)
    # community.general 1.0.0: 2 jobs (1, 4)
    # community.general 2.0.0: 2 jobs (2, 3)
    # community.general 3.0.0: 1 job (6)
    # ansible.windows 1.0.0: 1 job (2)
    # community.aws 1.5.0: 1 job (3)

    # Convert to dict for easier lookup
    collections_dict = {
        (c['collection_name'], c['collection_version']): c['job_count']
        for c in installed_collections
    }

    # Verify ansible.builtin 2.9.10 appears in 5 jobs
    assert collections_dict.get(('ansible.builtin', '2.9.10')) == 5, (
        f"Expected ansible.builtin 2.9.10 in 5 jobs, got {collections_dict.get(('ansible.builtin', '2.9.10'))}"
    )

    # Verify community.general appears with different versions
    assert collections_dict.get(('community.general', '1.0.0')) == 2, (
        f"Expected community.general 1.0.0 in 2 jobs, got {collections_dict.get(('community.general', '1.0.0'))}"
    )
    assert collections_dict.get(('community.general', '2.0.0')) == 2, (
        f"Expected community.general 2.0.0 in 2 jobs, got {collections_dict.get(('community.general', '2.0.0'))}"
    )
    assert collections_dict.get(('community.general', '3.0.0')) == 1, (
        f"Expected community.general 3.0.0 in 1 job, got {collections_dict.get(('community.general', '3.0.0'))}"
    )

    # Verify other collections
    assert collections_dict.get(('ansible.windows', '1.0.0')) == 1, (
        f"Expected ansible.windows 1.0.0 in 1 job, got {collections_dict.get(('ansible.windows', '1.0.0'))}"
    )
    assert collections_dict.get(('community.aws', '1.5.0')) == 1, (
        f"Expected community.aws 1.5.0 in 1 job, got {collections_dict.get(('community.aws', '1.5.0'))}"
    )

    # Verify total number of unique collection-version pairs
    # Should have 6 unique pairs: ansible.builtin 2.9.10, community.general (3 versions), ansible.windows 1.0.0, community.aws 1.5.0
    assert len(installed_collections) == 6, (
        f"Expected 6 unique collection-version pairs, got {len(installed_collections)}"
    )

    # Verify all entries have required fields
    for collection in installed_collections:
        assert 'collection_name' in collection
        assert 'collection_version' in collection
        assert 'job_count' in collection
        assert isinstance(collection['job_count'], int)
        assert collection['job_count'] > 0
