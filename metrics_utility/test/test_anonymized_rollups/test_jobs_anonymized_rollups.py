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
        'forks': 5,
        'inventory_name': 'inventory1',
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},
                'community.general': {'version': '1.0.0'},
            }
        ),
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
        'forks': 10,
        'inventory_name': 'inventory1',
        'scm_type': 'svn',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},  # Same version as job 1
                'community.general': {'version': '2.0.0'},  # Different version - same collection
                'ansible.windows': {'version': '1.0.0'},
            }
        ),
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
        'forks': 20,
        'inventory_name': 'inventory2',
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},  # Same version as jobs 1 and 2
                'community.general': {'version': '2.0.0'},  # Same version as job 2
                'community.aws': {'version': '1.5.0'},
            }
        ),
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
        'forks': 15,
        'inventory_name': 'inventory1',
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},  # Same version as other jobs
                'community.general': {'version': '1.0.0'},  # Same version as job 1
            }
        ),
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
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},
            }
        ),
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
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},
                'community.general': {'version': '3.0.0'},  # Another version of community.general
            }
        ),
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

    # Check top-level fields
    assert result['organizations_total'] == 3  # Org1, Org2, and Org3 (job 5 filtered out, but job 6 with Org3 remains)
    # Check scm_types: jobs 1,2,3,4,6 have scm_types: git, svn, git, git, unknown (job 5 filtered out)
    assert 'scm_types' in result, 'Should have scm_types field in result'
    assert result['scm_types'] == ['git', 'svn', 'unknown'], f"Expected ['git', 'svn', 'unknown'] for scm_types, got {result['scm_types']}"

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
    assert rec_job['jobs_never_started_total'] == 0
    assert rec_job['templates_total'] == 1  # All from template T1

    # 'job' type durations (seconds): 3.0, 5.0, 2.0
    assert rec_job['job_duration_maximum_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_job['job_duration_minimum_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_job['jobs_duration_total_seconds'] == pytest.approx(10.0, rel=1e-6)

    # 'job' type should have is_automation = True
    assert rec_job['is_automation'], 'job type should have is_automation = True'

    # 'job' type waiting times (seconds): 0.0, 2.0, 1.0
    assert rec_job['job_waiting_time_maximum_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_job['job_waiting_time_minimum_seconds'] == pytest.approx(0.0, rel=1e-6)
    assert rec_job['job_waiting_time_total_seconds'] == pytest.approx(3.0, rel=1e-6)

    # 'workflowjob' type counts (id 3 - 1 job)
    assert rec_workflowjob['jobs_total'] == 1
    assert rec_workflowjob['jobs_failed_total'] == 0
    assert rec_workflowjob['jobs_never_started_total'] == 0
    assert rec_workflowjob['templates_total'] == 1  # From template T2

    # 'workflowjob' type duration (seconds): 7.0
    assert rec_workflowjob['job_duration_maximum_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_workflowjob['job_duration_minimum_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_workflowjob['jobs_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)

    # 'workflowjob' type should have is_automation = False
    assert not rec_workflowjob['is_automation'], 'workflowjob type should have is_automation = False'

    # 'workflowjob' type waiting (seconds): 4.0
    assert rec_workflowjob['job_waiting_time_maximum_seconds'] == pytest.approx(4.0, rel=1e-6)
    assert rec_workflowjob['job_waiting_time_minimum_seconds'] == pytest.approx(4.0, rel=1e-6)
    assert rec_workflowjob['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # 'adhoccommand' type counts (id 6 - 1 job that never started)
    assert rec_adhoccommand['jobs_total'] == 1
    assert rec_adhoccommand['jobs_failed_total'] == 1
    assert rec_adhoccommand['jobs_never_started_total'] == 1
    assert rec_adhoccommand['templates_total'] == 1  # From template T3

    # 'adhoccommand' type should have NaN for all duration metrics and 0 for totals
    assert pd.isna(rec_adhoccommand['job_duration_maximum_seconds'])
    assert pd.isna(rec_adhoccommand['job_duration_minimum_seconds'])
    assert rec_adhoccommand['jobs_duration_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # 'adhoccommand' type should have is_automation = False
    assert not rec_adhoccommand['is_automation'], 'adhoccommand type should have is_automation = False'

    # 'adhoccommand' type should have NaN for all waiting time metrics and 0 for totals
    assert pd.isna(rec_adhoccommand['job_waiting_time_maximum_seconds'])
    assert pd.isna(rec_adhoccommand['job_waiting_time_minimum_seconds'])
    assert rec_adhoccommand['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # Validate controller_versions in by_job_type
    # 'job' type has jobs 1, 2, 4 with versions: 2.9.0, 2.10.0, 2.12.0
    assert 'controller_versions' in rec_job, 'Should have controller_versions field in by_job_type'
    assert rec_job['controller_versions'] == ['2.10.0', '2.12.0', '2.9.0'], (
        f"Expected ['2.10.0', '2.12.0', '2.9.0'] for job type, got {rec_job['controller_versions']}"
    )
    # 'workflowjob' type has job 3 with version: 2.11.0
    assert 'controller_versions' in rec_workflowjob, 'Should have controller_versions field in by_job_type'
    assert rec_workflowjob['controller_versions'] == ['2.11.0'], (
        f"Expected ['2.11.0'] for workflowjob type, got {rec_workflowjob['controller_versions']}"
    )
    # 'adhoccommand' type has job 6 with version: 2.14.0
    assert 'controller_versions' in rec_adhoccommand, 'Should have controller_versions field in by_job_type'
    assert rec_adhoccommand['controller_versions'] == ['2.14.0'], (
        f"Expected ['2.14.0'] for adhoccommand type, got {rec_adhoccommand['controller_versions']}"
    )

    # ========== Validate by_launch_type aggregations ==========
    # Result should have 'by_launch_type' list
    assert 'by_launch_type' in result

    # Extract the by_launch_type list
    by_launch_type = result['by_launch_type']
    assert isinstance(by_launch_type, list)

    # Expected launch types from test data (jobs 1-4 and 6, job 5 is filtered out):
    # Job 1: manual
    # Job 2: scheduled
    # Job 3: workflow
    # Job 4: callback
    # Job 6: scheduled
    # So we should have: manual, scheduled, workflow, callback (4 launch types)
    assert len(by_launch_type) == 4

    # Identify records by launch_type
    rec_manual = next((r for r in by_launch_type if r['launch_type'] == 'manual'), None)
    rec_scheduled = next((r for r in by_launch_type if r['launch_type'] == 'scheduled'), None)
    rec_workflow = next((r for r in by_launch_type if r['launch_type'] == 'workflow'), None)
    rec_callback = next((r for r in by_launch_type if r['launch_type'] == 'callback'), None)

    assert rec_manual is not None, 'Should have manual launch_type'
    assert rec_scheduled is not None, 'Should have scheduled launch_type'
    assert rec_workflow is not None, 'Should have workflow launch_type'
    assert rec_callback is not None, 'Should have callback launch_type'

    # 'manual' launch_type (job 1)
    assert rec_manual['jobs_total'] == 1
    assert rec_manual['jobs_failed_total'] == 0
    assert rec_manual['jobs_never_started_total'] == 0
    assert rec_manual['job_type_total'] == 1  # Only 'job' type
    assert rec_manual['templates_total'] == 1  # Template T1
    assert rec_manual['jobs_duration_total_seconds'] == pytest.approx(3.0, rel=1e-6)
    assert rec_manual['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # 'scheduled' launch_type (jobs 2 and 6)
    assert rec_scheduled['jobs_total'] == 2
    assert rec_scheduled['jobs_failed_total'] == 2  # Job 2 failed, job 6 failed (both have failed=1)
    assert rec_scheduled['jobs_never_started_total'] == 1  # Job 6 never started
    assert rec_scheduled['job_type_total'] == 2  # 'job' (job 2) and 'adhoccommand' (job 6)
    assert rec_scheduled['templates_total'] == 2  # Template T1 (job 2) and T3 (job 6)
    # Job 2: duration 5s, wait 2s; Job 6: never started (0s)
    assert rec_scheduled['jobs_duration_total_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_scheduled['job_waiting_time_total_seconds'] == pytest.approx(2.0, rel=1e-6)

    # 'workflow' launch_type (job 3)
    assert rec_workflow['jobs_total'] == 1
    assert rec_workflow['jobs_failed_total'] == 0
    assert rec_workflow['jobs_never_started_total'] == 0
    assert rec_workflow['job_type_total'] == 1  # Only 'workflowjob' type
    assert rec_workflow['templates_total'] == 1  # Template T2
    assert rec_workflow['jobs_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_workflow['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # 'callback' launch_type (job 4)
    assert rec_callback['jobs_total'] == 1
    assert rec_callback['jobs_failed_total'] == 0
    assert rec_callback['jobs_never_started_total'] == 0
    assert rec_callback['job_type_total'] == 1  # Only 'job' type
    assert rec_callback['templates_total'] == 1  # Template T1
    assert rec_callback['jobs_duration_total_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_callback['job_waiting_time_total_seconds'] == pytest.approx(1.0, rel=1e-6)

    # Verify that launch_type_*_total fields are NOT present (since we're grouping by launch_type)
    assert 'launch_type_manual_total' not in rec_manual
    assert 'launch_type_scheduled_total' not in rec_scheduled
    assert 'launch_type_workflow_total' not in rec_workflow
    assert 'launch_type_callback_total' not in rec_callback

    # Verify that job_type_total is present (instead of launch_type counts)
    assert 'job_type_total' in rec_manual
    assert 'job_type_total' in rec_scheduled
    assert 'job_type_total' in rec_workflow
    assert 'job_type_total' in rec_callback

    # Validate controller_versions in by_launch_type
    # 'manual' launch_type has job 1 with version: 2.9.0
    assert 'controller_versions' in rec_manual, 'Should have controller_versions field in by_launch_type'
    assert rec_manual['controller_versions'] == ['2.9.0'], f"Expected ['2.9.0'] for manual launch_type, got {rec_manual['controller_versions']}"
    # 'scheduled' launch_type has jobs 2, 6 with versions: 2.10.0, 2.14.0
    assert 'controller_versions' in rec_scheduled, 'Should have controller_versions field in by_launch_type'
    assert rec_scheduled['controller_versions'] == ['2.10.0', '2.14.0'], (
        f"Expected ['2.10.0', '2.14.0'] for scheduled launch_type, got {rec_scheduled['controller_versions']}"
    )
    # 'workflow' launch_type has job 3 with version: 2.11.0
    assert 'controller_versions' in rec_workflow, 'Should have controller_versions field in by_launch_type'
    assert rec_workflow['controller_versions'] == ['2.11.0'], (
        f"Expected ['2.11.0'] for workflow launch_type, got {rec_workflow['controller_versions']}"
    )
    # 'callback' launch_type has job 4 with version: 2.12.0
    assert 'controller_versions' in rec_callback, 'Should have controller_versions field in by_launch_type'
    assert rec_callback['controller_versions'] == ['2.12.0'], (
        f"Expected ['2.12.0'] for callback launch_type, got {rec_callback['controller_versions']}"
    )

    # Verify totals match between by_job_type and by_launch_type
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in by_job_type)
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in by_launch_type)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == 5, (
        f'Total jobs should match: by_job_type={total_jobs_by_job_type}, by_launch_type={total_jobs_by_launch_type}'
    )

    # ========== Validate by_controller_version aggregations ==========
    # Result should have 'by_controller_version' list
    assert 'by_controller_version' in result

    # Extract the by_controller_version list
    by_controller_version = result['by_controller_version']
    assert isinstance(by_controller_version, list)

    # Expected controller versions from test data (jobs 1-4 and 6, job 5 is filtered out):
    # Job 1: 2.9.0
    # Job 2: 2.10.0
    # Job 3: 2.11.0
    # Job 4: 2.12.0
    # Job 6: 2.14.0
    # So we should have 5 controller versions
    assert len(by_controller_version) == 5

    # Identify records by controller_version
    rec_2_9_0 = next((r for r in by_controller_version if r['controller_version'] == '2.9.0'), None)
    rec_2_10_0 = next((r for r in by_controller_version if r['controller_version'] == '2.10.0'), None)
    rec_2_11_0 = next((r for r in by_controller_version if r['controller_version'] == '2.11.0'), None)
    rec_2_12_0 = next((r for r in by_controller_version if r['controller_version'] == '2.12.0'), None)
    rec_2_14_0 = next((r for r in by_controller_version if r['controller_version'] == '2.14.0'), None)

    assert rec_2_9_0 is not None, 'Should have controller_version 2.9.0'
    assert rec_2_10_0 is not None, 'Should have controller_version 2.10.0'
    assert rec_2_11_0 is not None, 'Should have controller_version 2.11.0'
    assert rec_2_12_0 is not None, 'Should have controller_version 2.12.0'
    assert rec_2_14_0 is not None, 'Should have controller_version 2.14.0'

    # '2.9.0' controller_version (job 1)
    assert rec_2_9_0['jobs_total'] == 1
    assert rec_2_9_0['jobs_failed_total'] == 0
    assert rec_2_9_0['jobs_never_started_total'] == 0
    assert rec_2_9_0['job_type_total'] == 1  # Only 'job' type
    assert rec_2_9_0['templates_total'] == 1  # Template T1
    assert rec_2_9_0['jobs_duration_total_seconds'] == pytest.approx(3.0, rel=1e-6)
    assert rec_2_9_0['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # '2.10.0' controller_version (job 2)
    assert rec_2_10_0['jobs_total'] == 1
    assert rec_2_10_0['jobs_failed_total'] == 1
    assert rec_2_10_0['jobs_never_started_total'] == 0
    assert rec_2_10_0['job_type_total'] == 1  # Only 'job' type
    assert rec_2_10_0['templates_total'] == 1  # Template T1
    assert rec_2_10_0['jobs_duration_total_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_2_10_0['job_waiting_time_total_seconds'] == pytest.approx(2.0, rel=1e-6)

    # '2.11.0' controller_version (job 3)
    assert rec_2_11_0['jobs_total'] == 1
    assert rec_2_11_0['jobs_failed_total'] == 0
    assert rec_2_11_0['jobs_never_started_total'] == 0
    assert rec_2_11_0['job_type_total'] == 1  # Only 'workflowjob' type
    assert rec_2_11_0['templates_total'] == 1  # Template T2
    assert rec_2_11_0['jobs_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_2_11_0['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # '2.12.0' controller_version (job 4)
    assert rec_2_12_0['jobs_total'] == 1
    assert rec_2_12_0['jobs_failed_total'] == 0
    assert rec_2_12_0['jobs_never_started_total'] == 0
    assert rec_2_12_0['job_type_total'] == 1  # Only 'job' type
    assert rec_2_12_0['templates_total'] == 1  # Template T1
    assert rec_2_12_0['jobs_duration_total_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_2_12_0['job_waiting_time_total_seconds'] == pytest.approx(1.0, rel=1e-6)

    # '2.14.0' controller_version (job 6)
    assert rec_2_14_0['jobs_total'] == 1
    assert rec_2_14_0['jobs_failed_total'] == 1
    assert rec_2_14_0['jobs_never_started_total'] == 1  # Job 6 never started
    assert rec_2_14_0['job_type_total'] == 1  # Only 'adhoccommand' type
    assert rec_2_14_0['templates_total'] == 1  # Template T3
    assert rec_2_14_0['jobs_duration_total_seconds'] == pytest.approx(0.0, rel=1e-6)
    assert rec_2_14_0['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # Verify that job_type_total is present (counts distinct job types per controller_version)
    assert 'job_type_total' in rec_2_9_0
    assert 'job_type_total' in rec_2_10_0
    assert 'job_type_total' in rec_2_11_0
    assert 'job_type_total' in rec_2_12_0
    assert 'job_type_total' in rec_2_14_0

    # Verify that launch_type_*_total fields are NOT present (removed from all groupings)
    assert 'launch_type_manual_total' not in rec_2_9_0
    assert 'launch_type_scheduled_total' not in rec_2_10_0
    assert 'launch_type_workflow_total' not in rec_2_11_0
    assert 'launch_type_callback_total' not in rec_2_12_0
    assert 'launch_type_scheduled_total' not in rec_2_14_0

    # Verify totals match across all groupings
    total_jobs_by_controller_version = sum(j.get('jobs_total', 0) for j in by_controller_version)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_controller_version == 5, (
        f'Total jobs should match: by_job_type={total_jobs_by_job_type}, '
        f'by_launch_type={total_jobs_by_launch_type}, by_controller_version={total_jobs_by_controller_version}'
    )


def test_jobs_anonymized_rollups_ansible_version():
    """Test that organizations_total is correctly aggregated at top level."""
    df = pd.DataFrame(jobs)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    prepared_data = jobs_anonymized_rollup.prepare(df)
    result = jobs_anonymized_rollup.base(prepared_data)
    result = result['json']

    # Verify top-level fields are present
    assert 'organizations_total' in result
    assert result['organizations_total'] is not None

    # Verify organizations_total counts unique organizations (Org1, Org2, and Org3 - job 5 filtered out, but job 6 with Org3 remains)
    assert result['organizations_total'] == 3


def test_jobs_anonymized_rollups_ansible_version_multiple_per_type():
    """Test organizations_total aggregation when multiple organizations exist."""
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

    assert result['organizations_total'] == 3  # Org1, Org2, Org3
    assert rec_job['jobs_total'] == 3  # All three jobs are included
    # Check scm_types: jobs 1,2,3 have scm_types: git, svn, git
    assert 'scm_types' in result, 'Should have scm_types field in result'
    assert result['scm_types'] == ['git', 'svn'], f"Expected ['git', 'svn'] for scm_types, got {result['scm_types']}"


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
    collections_dict = {(c['collection_name'], c['collection_version']): c['job_count'] for c in installed_collections}

    # Verify ansible.builtin 2.9.10 appears in 5 jobs
    assert collections_dict.get(('ansible.builtin', '2.9.10')) == 5, (
        f'Expected ansible.builtin 2.9.10 in 5 jobs, got {collections_dict.get(("ansible.builtin", "2.9.10"))}'
    )

    # Verify community.general appears with different versions
    assert collections_dict.get(('community.general', '1.0.0')) == 2, (
        f'Expected community.general 1.0.0 in 2 jobs, got {collections_dict.get(("community.general", "1.0.0"))}'
    )
    assert collections_dict.get(('community.general', '2.0.0')) == 2, (
        f'Expected community.general 2.0.0 in 2 jobs, got {collections_dict.get(("community.general", "2.0.0"))}'
    )
    assert collections_dict.get(('community.general', '3.0.0')) == 1, (
        f'Expected community.general 3.0.0 in 1 job, got {collections_dict.get(("community.general", "3.0.0"))}'
    )

    # Verify other collections
    assert collections_dict.get(('ansible.windows', '1.0.0')) == 1, (
        f'Expected ansible.windows 1.0.0 in 1 job, got {collections_dict.get(("ansible.windows", "1.0.0"))}'
    )
    assert collections_dict.get(('community.aws', '1.5.0')) == 1, (
        f'Expected community.aws 1.5.0 in 1 job, got {collections_dict.get(("community.aws", "1.5.0"))}'
    )

    # Verify total number of unique collection-version pairs
    # Should have 6 unique pairs: ansible.builtin 2.9.10, community.general (3 versions), ansible.windows 1.0.0, community.aws 1.5.0
    assert len(installed_collections) == 6, f'Expected 6 unique collection-version pairs, got {len(installed_collections)}'

    # Verify all entries have required fields
    for collection in installed_collections:
        assert 'collection_name' in collection
        assert 'collection_version' in collection
        assert 'job_count' in collection
        assert isinstance(collection['job_count'], int)
        assert collection['job_count'] > 0


def _extract_controller_versions_from_jobs(jobs_by_job_type):
    """Extract and merge controller versions from jobs_by_job_type."""
    expected_versions_set = set()
    for job in jobs_by_job_type:
        controller_versions = job.get('controller_versions', [])
        if isinstance(controller_versions, list):
            expected_versions_set.update(controller_versions)
    return sorted(list(expected_versions_set))


def _validate_controller_versions(result, expected_versions):
    """Validate controller versions at top level."""
    assert 'rollup_period_controller_versions' in result, 'Should have controller_versions at top level'
    assert result['rollup_period_controller_versions'] == expected_versions, (
        f'Expected controller_versions {expected_versions} at top level, got {result["rollup_period_controller_versions"]}'
    )
    assert len(result['rollup_period_controller_versions']) == 5, (
        f'Expected 5 unique controller versions, got {len(result["rollup_period_controller_versions"])}'
    )
    for version in ['2.9.0', '2.10.0', '2.11.0', '2.12.0', '2.14.0']:
        assert version in result['rollup_period_controller_versions']


def _validate_job_statistics(statistics, jobs_by_job_type):
    """Validate job statistics match sum from jobs_by_job_type."""
    if not jobs_by_job_type:
        return

    expected_jobs_successful = sum(j.get('jobs_successful_total', 0) for j in jobs_by_job_type)
    expected_jobs_failed = sum(j.get('jobs_failed_total', 0) for j in jobs_by_job_type)
    expected_duration_all = sum(j.get('jobs_duration_total_seconds', 0) or 0 for j in jobs_by_job_type)
    expected_duration_successful = sum(j.get('jobs_successful_duration_total_seconds', 0) or 0 for j in jobs_by_job_type)
    expected_duration_failed = sum(j.get('jobs_failed_duration_total_seconds', 0) or 0 for j in jobs_by_job_type)

    if statistics['rollup_period_jobs_successful'] is not None:
        assert statistics['rollup_period_jobs_successful'] == expected_jobs_successful, (
            f'jobs_successful should match sum from jobs_by_job_type: expected={expected_jobs_successful}, '
            f'got={statistics["rollup_period_jobs_successful"]}'
        )
    if statistics['rollup_period_jobs_failed'] is not None:
        assert statistics['rollup_period_jobs_failed'] == expected_jobs_failed, (
            f'jobs_failed should match sum from jobs_by_job_type: expected={expected_jobs_failed}, got={statistics["rollup_period_jobs_failed"]}'
        )
    if statistics['rollup_period_jobs_duration_all_statuses_seconds'] is not None:
        assert abs(statistics['rollup_period_jobs_duration_all_statuses_seconds'] - expected_duration_all) < 0.001, (
            f'jobs_duration_all_statuses_seconds should match sum from jobs_by_job_type: expected={expected_duration_all}, '
            f'got={statistics["rollup_period_jobs_duration_all_statuses_seconds"]}'
        )
    if statistics['rollup_period_jobs_successful_duration_total_seconds'] is not None:
        assert abs(statistics['rollup_period_jobs_successful_duration_total_seconds'] - expected_duration_successful) < 0.001, (
            f'jobs_successful_duration_total_seconds should match sum from jobs_by_job_type: expected={expected_duration_successful}, '
            f'got={statistics["rollup_period_jobs_successful_duration_total_seconds"]}'
        )
    if statistics['rollup_period_jobs_failed_duration_total_seconds'] is not None:
        assert abs(statistics['rollup_period_jobs_failed_duration_total_seconds'] - expected_duration_failed) < 0.001, (
            f'jobs_failed_duration_total_seconds should match sum from jobs_by_job_type: expected={expected_duration_failed}, '
            f'got={statistics["rollup_period_jobs_failed_duration_total_seconds"]}'
        )


def test_jobs_anonymized_rollups_statistics_controller_versions():
    """Test that controller_versions in statistics is correctly merged from jobs_by_job_type."""
    import os
    import shutil

    from datetime import datetime

    from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
    from metrics_utility.test.test_anonymized_rollups.test_credentials_anonymized_rollup import credentials
    from metrics_utility.test.test_anonymized_rollups.test_events_modules_anonymized_rollups import events
    from metrics_utility.test.test_anonymized_rollups.test_execution_environments_anonymized_rollups import execution_environments
    from metrics_utility.test.test_anonymized_rollups.test_jobhostsummary_anonymized_rollups import jobhostsummary

    # Cleanup
    out_dir = './out'
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)
    base_path = './out'
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # Create CSV files
    def create_csv_file(data_list, csv_path):
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        if not data_list:
            return None
        df = pd.DataFrame(data_list)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        return csv_path

    jobs_csv = create_csv_file(jobs, f'{data_dir}/unified_jobs.csv')
    events_csv = create_csv_file(events, f'{data_dir}/main_jobevent.csv')
    ee_csv = create_csv_file(execution_environments, f'{data_dir}/execution_environments.csv')
    jhs_csv = create_csv_file(jobhostsummary, f'{data_dir}/job_host_summary.csv')
    cred_csv = create_csv_file(credentials, f'{data_dir}/credentials.csv')

    input_data = {
        'unified_jobs': [jobs_csv] if jobs_csv else [],
        'job_host_summary': [jhs_csv] if jhs_csv else [],
        'main_jobevent': [events_csv] if events_csv else [],
        'execution_environments': [ee_csv] if ee_csv else [],
        'credentials': [cred_csv] if cred_csv else [],
    }

    result = compute_anonymized_rollup_from_raw_data(
        input_data=input_data, salt='test_salt', since=since, until=until, base_path=base_path, save_rollups=False
    )

    # Validate result has controller_versions at top level
    assert 'statistics' in result, 'Should have statistics in result'
    statistics = result['statistics']
    assert 'rollup_period_jobs_successful' in statistics, 'Should have jobs_successful in statistics'
    assert 'rollup_period_jobs_failed' in statistics, 'Should have jobs_failed in statistics'
    assert 'rollup_period_jobs_duration_all_statuses_seconds' in statistics, 'Should have jobs_duration_all_statuses_seconds in statistics'
    assert 'rollup_period_jobs_successful_duration_total_seconds' in statistics, 'Should have jobs_successful_duration_total_seconds in statistics'
    assert 'rollup_period_jobs_failed_duration_total_seconds' in statistics, 'Should have jobs_failed_duration_total_seconds in statistics'

    # Get controller_versions from jobs_by_job_type
    jobs_by_job_type = result.get('jobs_by_job_type', [])
    expected_versions = _extract_controller_versions_from_jobs(jobs_by_job_type)

    # Validate controller_versions at top level matches merged values from jobs_by_job_type
    _validate_controller_versions(result, expected_versions)

    # Validate new job statistics match sum from jobs_by_job_type
    _validate_job_statistics(statistics, jobs_by_job_type)

    # Validate scm_types at top level
    assert 'rollup_period_scm_types' in result, 'Should have rollup_period_scm_types at top level'
    assert result['rollup_period_scm_types'] == ['git', 'svn', 'unknown'], (
        f"Expected ['git', 'svn', 'unknown'] for rollup_period_scm_types, got {result['rollup_period_scm_types']}"
    )
