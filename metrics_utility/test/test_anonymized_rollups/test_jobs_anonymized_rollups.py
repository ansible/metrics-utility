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
        'unified_job_template_id': 1,
        'controller_node': 'ctrl-A',
        'ansible_version': '2.9.0',
        'organization_name': 'Org1',
        'created': '2024-01-01 00:00:00.000000+00',
        'model': 'job',
        'launch_type': 'manual',
        'forks': 5,
        'inventory_name': 'inventory1',
        'inventory_id': 1,
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},
                'community.general': {'version': '1.0.0'},
            }
        ),
        'execution_environment_id': 1,  # EE for ansible.builtin+community.general 1.0.0
    },  # duration 3s, wait 0s
    {
        'id': 2,
        'started': '2024-01-01 00:00:10.000000+00',
        'finished': '2024-01-01 00:00:15.000000+00',  # +5s
        'failed': 1,
        'job_template_name': 'T1',
        'unified_job_template_id': 1,
        'controller_node': 'ctrl-A',
        'ansible_version': '2.10.0',
        'organization_name': 'Org1',
        'created': '2024-01-01 00:00:08.000000+00',  # wait 2s
        'model': 'job',
        'launch_type': 'scheduled',
        'forks': 10,
        'inventory_name': 'inventory1',
        'inventory_id': 1,
        'scm_type': 'svn',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},  # Same version as job 1
                'community.general': {'version': '2.0.0'},  # Different version - same collection
                'ansible.windows': {'version': '1.0.0'},
            }
        ),
        'execution_environment_id': 2,  # EE for ansible.builtin+community.general 2.0.0+ansible.windows
    },  # duration 5s (failed), wait 2s
    # controller A, ansible 2.11.0, template T2
    {
        'id': 3,
        'started': '2024-01-01 00:01:40.000000+00',
        'finished': '2024-01-01 00:01:47.000000+00',  # +7s
        'failed': 0,
        'job_template_name': 'T2',
        'unified_job_template_id': 2,
        'controller_node': 'ctrl-A',
        'ansible_version': '2.11.0',
        'organization_name': 'Org2',
        'created': '2024-01-01 00:01:36.000000+00',  # wait 4s
        'model': 'workflowjob',
        'launch_type': 'workflow',
        'forks': 20,
        'inventory_name': 'inventory2',
        'inventory_id': 2,
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},  # Same version as jobs 1 and 2
                'community.general': {'version': '2.0.0'},  # Same version as job 2
                'community.aws': {'version': '1.5.0'},
            }
        ),
        'execution_environment_id': 3,  # EE for ansible.builtin+community.general 2.0.0+community.aws
    },  # duration 7s, wait 4s
    # controller B, ansible 2.12.0, template T1
    {
        'id': 4,
        'started': '2024-01-01 00:03:20.000000+00',
        'finished': '2024-01-01 00:03:22.000000+00',  # +2s
        'failed': 0,
        'job_template_name': 'T1',
        'unified_job_template_id': 1,
        'controller_node': 'ctrl-B',
        'ansible_version': '2.12.0',
        'organization_name': 'Org1',
        'created': '2024-01-01 00:03:19.000000+00',  # wait 1s
        'model': 'job',
        'launch_type': 'callback',
        'forks': 15,
        'inventory_name': 'inventory1',
        'inventory_id': 1,
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},  # Same version as other jobs
                'community.general': {'version': '1.0.0'},  # Same version as job 1
            }
        ),
        'execution_environment_id': 1,  # Same EE as job 1 (same installed_collections)
    },  # duration 2s, wait 1s
    # invalid rows (should be filtered out)
    {
        'id': 5,
        'started': '2024-01-01 00:06:40.000000+00',
        'finished': None,
        'failed': 0,
        'job_template_name': 'T3',
        'unified_job_template_id': 3,
        'controller_node': 'ctrl-C',
        'ansible_version': '2.13.0',
        'organization_name': 'Org3',
        'model': 'adhoccommand',
        'launch_type': 'manual',
        'forks': 0,
        'inventory_name': 'inventory3',
        'inventory_id': 3,
        'scm_type': 'manual',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},
            }
        ),
        'execution_environment_id': 4,  # EE for ansible.builtin only
    },
    {
        'id': 6,
        'started': None,
        'finished': '2024-01-01 00:08:20.000000+00',
        'failed': 1,
        'job_template_name': 'T3',
        'unified_job_template_id': 3,
        'controller_node': 'ctrl-C',
        'ansible_version': '2.14.0',
        'organization_name': 'Org3',
        'model': 'adhoccommand',
        'launch_type': 'scheduled',
        'forks': 0,
        'inventory_name': 'inventory3',
        'inventory_id': 3,
        'scm_type': 'unknown',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.9.10'},
                'community.general': {'version': '3.0.0'},  # Another version of community.general
            }
        ),
        'execution_environment_id': 5,  # EE for ansible.builtin+community.general 3.0.0
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

    # Validate ansible_versions in by_job_type
    # 'job' type has jobs 1, 2, 4 with versions: 2.9.0, 2.10.0, 2.12.0
    assert 'ansible_versions' in rec_job, 'Should have ansible_versions field in by_job_type'
    assert rec_job['ansible_versions'] == ['2.10.0', '2.12.0', '2.9.0'], (
        f"Expected ['2.10.0', '2.12.0', '2.9.0'] for job type, got {rec_job['ansible_versions']}"
    )
    # 'workflowjob' type has job 3 with version: 2.11.0
    assert 'ansible_versions' in rec_workflowjob, 'Should have ansible_versions field in by_job_type'
    assert rec_workflowjob['ansible_versions'] == ['2.11.0'], f"Expected ['2.11.0'] for workflowjob type, got {rec_workflowjob['ansible_versions']}"
    # 'adhoccommand' type has job 6 with version: 2.14.0
    assert 'ansible_versions' in rec_adhoccommand, 'Should have ansible_versions field in by_job_type'
    assert rec_adhoccommand['ansible_versions'] == ['2.14.0'], (
        f"Expected ['2.14.0'] for adhoccommand type, got {rec_adhoccommand['ansible_versions']}"
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
    assert rec_manual['templates_total'] == 1  # Template T1
    assert rec_manual['jobs_duration_total_seconds'] == pytest.approx(3.0, rel=1e-6)
    assert rec_manual['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # 'scheduled' launch_type (jobs 2 and 6)
    assert rec_scheduled['jobs_total'] == 2
    assert rec_scheduled['jobs_failed_total'] == 2  # Job 2 failed, job 6 failed (both have failed=1)
    assert rec_scheduled['jobs_never_started_total'] == 1  # Job 6 never started
    assert rec_scheduled['templates_total'] == 2  # Template T1 (job 2) and T3 (job 6)
    # Job 2: duration 5s, wait 2s; Job 6: never started (0s)
    assert rec_scheduled['jobs_duration_total_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_scheduled['job_waiting_time_total_seconds'] == pytest.approx(2.0, rel=1e-6)

    # 'workflow' launch_type (job 3)
    assert rec_workflow['jobs_total'] == 1
    assert rec_workflow['jobs_failed_total'] == 0
    assert rec_workflow['jobs_never_started_total'] == 0
    assert rec_workflow['templates_total'] == 1  # Template T2
    assert rec_workflow['jobs_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_workflow['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # 'callback' launch_type (job 4)
    assert rec_callback['jobs_total'] == 1
    assert rec_callback['jobs_failed_total'] == 0
    assert rec_callback['jobs_never_started_total'] == 0
    assert rec_callback['templates_total'] == 1  # Template T1
    assert rec_callback['jobs_duration_total_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_callback['job_waiting_time_total_seconds'] == pytest.approx(1.0, rel=1e-6)

    # Verify that launch_type_*_total fields are NOT present (since we're grouping by launch_type)
    assert 'launch_type_manual_total' not in rec_manual
    assert 'launch_type_scheduled_total' not in rec_scheduled
    assert 'launch_type_workflow_total' not in rec_workflow
    assert 'launch_type_callback_total' not in rec_callback

    # Validate ansible_versions in by_launch_type
    # 'manual' launch_type has job 1 with version: 2.9.0
    assert 'ansible_versions' in rec_manual, 'Should have ansible_versions field in by_launch_type'
    assert rec_manual['ansible_versions'] == ['2.9.0'], f"Expected ['2.9.0'] for manual launch_type, got {rec_manual['ansible_versions']}"
    # 'scheduled' launch_type has jobs 2, 6 with versions: 2.10.0, 2.14.0
    assert 'ansible_versions' in rec_scheduled, 'Should have ansible_versions field in by_launch_type'
    assert rec_scheduled['ansible_versions'] == ['2.10.0', '2.14.0'], (
        f"Expected ['2.10.0', '2.14.0'] for scheduled launch_type, got {rec_scheduled['ansible_versions']}"
    )
    # 'workflow' launch_type has job 3 with version: 2.11.0
    assert 'ansible_versions' in rec_workflow, 'Should have ansible_versions field in by_launch_type'
    assert rec_workflow['ansible_versions'] == ['2.11.0'], f"Expected ['2.11.0'] for workflow launch_type, got {rec_workflow['ansible_versions']}"
    # 'callback' launch_type has job 4 with version: 2.12.0
    assert 'ansible_versions' in rec_callback, 'Should have ansible_versions field in by_launch_type'
    assert rec_callback['ansible_versions'] == ['2.12.0'], f"Expected ['2.12.0'] for callback launch_type, got {rec_callback['ansible_versions']}"

    # Verify totals match between by_job_type and by_launch_type
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in by_job_type)
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in by_launch_type)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == 5, (
        f'Total jobs should match: by_job_type={total_jobs_by_job_type}, by_launch_type={total_jobs_by_launch_type}'
    )

    # ========== Validate by_ansible_version aggregations ==========
    # Result should have 'by_ansible_version' list
    assert 'by_ansible_version' in result

    # Extract the by_ansible_version list
    by_ansible_version = result['by_ansible_version']
    assert isinstance(by_ansible_version, list)

    # Expected controller versions from test data (jobs 1-4 and 6, job 5 is filtered out):
    # Job 1: 2.9.0
    # Job 2: 2.10.0
    # Job 3: 2.11.0
    # Job 4: 2.12.0
    # Job 6: 2.14.0
    # So we should have 5 controller versions
    assert len(by_ansible_version) == 5

    # Identify records by ansible_version
    rec_2_9_0 = next((r for r in by_ansible_version if r['ansible_version'] == '2.9.0'), None)
    rec_2_10_0 = next((r for r in by_ansible_version if r['ansible_version'] == '2.10.0'), None)
    rec_2_11_0 = next((r for r in by_ansible_version if r['ansible_version'] == '2.11.0'), None)
    rec_2_12_0 = next((r for r in by_ansible_version if r['ansible_version'] == '2.12.0'), None)
    rec_2_14_0 = next((r for r in by_ansible_version if r['ansible_version'] == '2.14.0'), None)

    assert rec_2_9_0 is not None, 'Should have ansible_version 2.9.0'
    assert rec_2_10_0 is not None, 'Should have ansible_version 2.10.0'
    assert rec_2_11_0 is not None, 'Should have ansible_version 2.11.0'
    assert rec_2_12_0 is not None, 'Should have ansible_version 2.12.0'
    assert rec_2_14_0 is not None, 'Should have ansible_version 2.14.0'

    # '2.9.0' ansible_version (job 1)
    assert rec_2_9_0['jobs_total'] == 1
    assert rec_2_9_0['jobs_failed_total'] == 0
    assert rec_2_9_0['jobs_never_started_total'] == 0
    assert rec_2_9_0['templates_total'] == 1  # Template T1
    assert rec_2_9_0['jobs_duration_total_seconds'] == pytest.approx(3.0, rel=1e-6)
    assert rec_2_9_0['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # '2.10.0' ansible_version (job 2)
    assert rec_2_10_0['jobs_total'] == 1
    assert rec_2_10_0['jobs_failed_total'] == 1
    assert rec_2_10_0['jobs_never_started_total'] == 0
    assert rec_2_10_0['templates_total'] == 1  # Template T1
    assert rec_2_10_0['jobs_duration_total_seconds'] == pytest.approx(5.0, rel=1e-6)
    assert rec_2_10_0['job_waiting_time_total_seconds'] == pytest.approx(2.0, rel=1e-6)

    # '2.11.0' ansible_version (job 3)
    assert rec_2_11_0['jobs_total'] == 1
    assert rec_2_11_0['jobs_failed_total'] == 0
    assert rec_2_11_0['jobs_never_started_total'] == 0
    assert rec_2_11_0['templates_total'] == 1  # Template T2
    assert rec_2_11_0['jobs_duration_total_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert rec_2_11_0['job_waiting_time_total_seconds'] == pytest.approx(4.0, rel=1e-6)

    # '2.12.0' ansible_version (job 4)
    assert rec_2_12_0['jobs_total'] == 1
    assert rec_2_12_0['jobs_failed_total'] == 0
    assert rec_2_12_0['jobs_never_started_total'] == 0
    assert rec_2_12_0['templates_total'] == 1  # Template T1
    assert rec_2_12_0['jobs_duration_total_seconds'] == pytest.approx(2.0, rel=1e-6)
    assert rec_2_12_0['job_waiting_time_total_seconds'] == pytest.approx(1.0, rel=1e-6)

    # '2.14.0' ansible_version (job 6)
    assert rec_2_14_0['jobs_total'] == 1
    assert rec_2_14_0['jobs_failed_total'] == 1
    assert rec_2_14_0['jobs_never_started_total'] == 1  # Job 6 never started
    assert rec_2_14_0['templates_total'] == 1  # Template T3
    assert rec_2_14_0['jobs_duration_total_seconds'] == pytest.approx(0.0, rel=1e-6)
    assert rec_2_14_0['job_waiting_time_total_seconds'] == pytest.approx(0.0, rel=1e-6)

    # Verify that launch_type_*_total fields are NOT present (removed from all groupings)
    assert 'launch_type_manual_total' not in rec_2_9_0
    assert 'launch_type_scheduled_total' not in rec_2_10_0
    assert 'launch_type_workflow_total' not in rec_2_11_0
    assert 'launch_type_callback_total' not in rec_2_12_0
    assert 'launch_type_scheduled_total' not in rec_2_14_0

    # Verify totals match across all groupings
    total_jobs_by_ansible_version = sum(j.get('jobs_total', 0) for j in by_ansible_version)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_ansible_version == 5, (
        f'Total jobs should match: by_job_type={total_jobs_by_job_type}, '
        f'by_launch_type={total_jobs_by_launch_type}, by_ansible_version={total_jobs_by_ansible_version}'
    )

    # ========== Validate jobs_by_controller_version aggregation ==========
    # This is a single-item array summarising ALL valid jobs combined
    assert 'jobs_by_controller_version' in result, 'Should have jobs_by_controller_version in result'
    ctrl_summary_list = result['jobs_by_controller_version']
    assert isinstance(ctrl_summary_list, list), 'jobs_by_controller_version should be a list'
    assert len(ctrl_summary_list) == 1, 'jobs_by_controller_version should contain exactly 1 item'

    ctrl_summary = ctrl_summary_list[0]
    # Counts: jobs 1,2,3,4,6 are valid (job 5 filtered - no finished)
    assert ctrl_summary['jobs_total'] == 5
    assert ctrl_summary['jobs_failed_total'] == 2  # jobs 2 and 6
    assert ctrl_summary['jobs_successful_total'] == 3  # jobs 1, 3, 4
    assert ctrl_summary['jobs_never_started_total'] == 1  # job 6 has started=None
    assert ctrl_summary['templates_total'] == 3  # T1 (id=1), T2 (id=2), T3 (id=3)
    assert ctrl_summary['inventories_total'] == 3  # inventory_id 1, 2, 3
    # Durations: 3s + 5s + 7s + 2s = 17s (job 6 is NaN, skipped in sum)
    assert ctrl_summary['jobs_duration_total_seconds'] == pytest.approx(17.0, rel=1e-6)
    assert ctrl_summary['job_duration_maximum_seconds'] == pytest.approx(7.0, rel=1e-6)  # job 3
    assert ctrl_summary['job_duration_minimum_seconds'] == pytest.approx(2.0, rel=1e-6)  # job 4
    # Waiting times: 0s + 2s + 4s + 1s = 7s (job 6 is NaN, skipped)
    assert ctrl_summary['job_waiting_time_total_seconds'] == pytest.approx(7.0, rel=1e-6)
    assert ctrl_summary['job_waiting_time_maximum_seconds'] == pytest.approx(4.0, rel=1e-6)  # job 3
    assert ctrl_summary['job_waiting_time_minimum_seconds'] == pytest.approx(0.0, rel=1e-6)  # job 1
    # All unique ansible versions across all valid jobs
    assert ctrl_summary['ansible_versions'] == ['2.10.0', '2.11.0', '2.12.0', '2.14.0', '2.9.0']
    # controller_version is NOT set at the base() stage - it is injected in flatten_json_report
    assert 'controller_version' not in ctrl_summary, (
        'controller_version should not be present in base() output; it is injected in flatten_json_report'
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
            'unified_job_template_id': 1,
            'controller_node': 'ctrl-A',
            'ansible_version': '2.9.0',
            'organization_name': 'Org1',
            'created': '2024-01-01 00:00:00.000000+00',
            'model': 'job',
            'launch_type': 'manual',
            'forks': 5,
            'inventory_name': 'inventory1',
            'inventory_id': 1,
            'scm_type': 'git',
        },
        {
            'id': 2,
            'started': '2024-01-01 00:00:10.000000+00',
            'finished': '2024-01-01 00:00:15.000000+00',
            'failed': 0,
            'job_template_name': 'T2',
            'unified_job_template_id': 2,
            'controller_node': 'ctrl-B',
            'ansible_version': '2.10.0',
            'organization_name': 'Org2',
            'created': '2024-01-01 00:00:08.000000+00',
            'model': 'job',
            'launch_type': 'scheduled',
            'forks': 10,
            'inventory_name': 'inventory2',
            'inventory_id': 2,
            'scm_type': 'svn',
        },
        {
            'id': 3,
            'started': '2024-01-01 00:01:00.000000+00',
            'finished': '2024-01-01 00:01:05.000000+00',
            'failed': 0,
            'job_template_name': 'T3',
            'unified_job_template_id': 3,
            'controller_node': 'ctrl-C',
            'ansible_version': '2.11.0',
            'organization_name': 'Org3',
            'created': '2024-01-01 00:00:58.000000+00',
            'model': 'job',
            'launch_type': 'callback',
            'forks': 15,
            'inventory_name': 'inventory3',
            'inventory_id': 3,
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
    collections_dict = {(c['collection_name'], c['collection_version']): c for c in installed_collections}

    # Expected failed/successful counts per collection (jobs 1-4 and 6 are included, 5 is filtered):
    # Job 1: failed=0, Job 2: failed=1, Job 3: failed=0, Job 4: failed=0, Job 6: failed=1
    # ansible.builtin 2.9.10: jobs 1(s),2(f),3(s),4(s),6(f) → 2 failed, 3 successful
    # community.general 1.0.0: jobs 1(s),4(s) → 0 failed, 2 successful
    # community.general 2.0.0: jobs 2(f),3(s) → 1 failed, 1 successful
    # community.general 3.0.0: job 6(f) → 1 failed, 0 successful
    # ansible.windows 1.0.0: job 2(f) → 1 failed, 0 successful
    # community.aws 1.5.0: job 3(s) → 0 failed, 1 successful

    # Verify ansible.builtin 2.9.10 appears in 5 jobs (1s, 2f, 3s, 4s, 6f-never-started)
    # durations: 3+5+7+2=17s total; job6 NaN skipped; successful: 3+7+2=12s; failed: 5s
    # waiting:   0+2+4+1=7s total; job6 NaN skipped; min=0s(job1), max=4s(job3)
    # templates: {1,2,3}; inventories: {1,2,3}; ansible_versions: all 5 versions
    ab = collections_dict.get(('ansible.builtin', '2.9.10'))
    assert ab['job_count'] == 5, f'Expected ansible.builtin 2.9.10 in 5 jobs, got {ab}'
    assert ab['jobs_failed_total'] == 2
    assert ab['jobs_successful_total'] == 3
    assert ab['jobs_never_started_total'] == 1, 'job6 has started=None → never started'
    assert ab['jobs_duration_total_seconds'] == pytest.approx(17.0)
    assert ab['jobs_successful_duration_total_seconds'] == pytest.approx(12.0)
    assert ab['jobs_failed_duration_total_seconds'] == pytest.approx(5.0)
    assert ab['job_duration_maximum_seconds'] == pytest.approx(7.0)
    assert ab['job_duration_minimum_seconds'] == pytest.approx(2.0)
    assert ab['job_waiting_time_total_seconds'] == pytest.approx(7.0)
    assert ab['job_waiting_time_maximum_seconds'] == pytest.approx(4.0)
    assert ab['job_waiting_time_minimum_seconds'] == pytest.approx(0.0)
    assert ab['templates_total'] == 3, 'templates 1,2,3 → 3 unique'
    assert ab['inventories_total'] == 3, 'inventories 1,2,3 → 3 unique'
    assert ab['ansible_versions'] == ['2.10.0', '2.11.0', '2.12.0', '2.14.0', '2.9.0']

    # Verify community.general appears with different versions
    # community.general 1.0.0 → jobs 1(s,3s,0s,T1,I1,v2.9.0) and 4(s,2s,1s,T1,I1,v2.12.0)
    cg1 = collections_dict.get(('community.general', '1.0.0'))
    assert cg1['job_count'] == 2, f'Expected community.general 1.0.0 in 2 jobs, got {cg1}'
    assert cg1['jobs_failed_total'] == 0
    assert cg1['jobs_successful_total'] == 2
    assert cg1['jobs_never_started_total'] == 0
    assert cg1['jobs_duration_total_seconds'] == pytest.approx(5.0)  # 3+2
    assert cg1['jobs_successful_duration_total_seconds'] == pytest.approx(5.0)
    assert cg1['jobs_failed_duration_total_seconds'] == pytest.approx(0.0)
    assert cg1['job_duration_maximum_seconds'] == pytest.approx(3.0)
    assert cg1['job_duration_minimum_seconds'] == pytest.approx(2.0)
    assert cg1['job_waiting_time_total_seconds'] == pytest.approx(1.0)  # 0+1
    assert cg1['job_waiting_time_maximum_seconds'] == pytest.approx(1.0)
    assert cg1['job_waiting_time_minimum_seconds'] == pytest.approx(0.0)
    assert cg1['templates_total'] == 1
    assert cg1['inventories_total'] == 1
    assert cg1['ansible_versions'] == ['2.12.0', '2.9.0']

    # community.general 2.0.0 → jobs 2(f,5s,2s,T1,I1,v2.10.0) and 3(s,7s,4s,T2,I2,v2.11.0)
    cg2 = collections_dict.get(('community.general', '2.0.0'))
    assert cg2['job_count'] == 2, f'Expected community.general 2.0.0 in 2 jobs, got {cg2}'
    assert cg2['jobs_failed_total'] == 1
    assert cg2['jobs_successful_total'] == 1
    assert cg2['jobs_never_started_total'] == 0
    assert cg2['jobs_duration_total_seconds'] == pytest.approx(12.0)  # 5+7
    assert cg2['jobs_successful_duration_total_seconds'] == pytest.approx(7.0)
    assert cg2['jobs_failed_duration_total_seconds'] == pytest.approx(5.0)
    assert cg2['job_duration_maximum_seconds'] == pytest.approx(7.0)
    assert cg2['job_duration_minimum_seconds'] == pytest.approx(5.0)
    assert cg2['job_waiting_time_total_seconds'] == pytest.approx(6.0)  # 2+4
    assert cg2['job_waiting_time_maximum_seconds'] == pytest.approx(4.0)
    assert cg2['job_waiting_time_minimum_seconds'] == pytest.approx(2.0)
    assert cg2['templates_total'] == 2
    assert cg2['inventories_total'] == 2
    assert cg2['ansible_versions'] == ['2.10.0', '2.11.0']

    # community.general 3.0.0 → job 6 only (failed, never started → NaN durations/waits)
    cg3 = collections_dict.get(('community.general', '3.0.0'))
    assert cg3['job_count'] == 1, f'Expected community.general 3.0.0 in 1 job, got {cg3}'
    assert cg3['jobs_failed_total'] == 1
    assert cg3['jobs_successful_total'] == 0
    assert cg3['jobs_never_started_total'] == 1
    assert cg3['jobs_duration_total_seconds'] == pytest.approx(0.0)  # NaN → skipped
    assert cg3['jobs_successful_duration_total_seconds'] == pytest.approx(0.0)
    assert cg3['jobs_failed_duration_total_seconds'] == pytest.approx(0.0)
    assert cg3['job_duration_maximum_seconds'] is None
    assert cg3['job_duration_minimum_seconds'] is None
    assert cg3['job_waiting_time_total_seconds'] == pytest.approx(0.0)
    assert cg3['job_waiting_time_maximum_seconds'] is None
    assert cg3['job_waiting_time_minimum_seconds'] is None
    assert cg3['templates_total'] == 1
    assert cg3['inventories_total'] == 1
    assert cg3['ansible_versions'] == ['2.14.0']

    # Verify other collections
    # ansible.windows 1.0.0 → job 2 only (failed, 5s duration, 2s wait, T1, I1, v2.10.0)
    aw = collections_dict.get(('ansible.windows', '1.0.0'))
    assert aw['job_count'] == 1, f'Expected ansible.windows 1.0.0 in 1 job, got {aw}'
    assert aw['jobs_failed_total'] == 1
    assert aw['jobs_successful_total'] == 0
    assert aw['jobs_never_started_total'] == 0
    assert aw['jobs_duration_total_seconds'] == pytest.approx(5.0)
    assert aw['jobs_successful_duration_total_seconds'] == pytest.approx(0.0)
    assert aw['jobs_failed_duration_total_seconds'] == pytest.approx(5.0)
    assert aw['job_duration_maximum_seconds'] == pytest.approx(5.0)
    assert aw['job_duration_minimum_seconds'] == pytest.approx(5.0)
    assert aw['job_waiting_time_total_seconds'] == pytest.approx(2.0)
    assert aw['job_waiting_time_maximum_seconds'] == pytest.approx(2.0)
    assert aw['job_waiting_time_minimum_seconds'] == pytest.approx(2.0)
    assert aw['templates_total'] == 1
    assert aw['inventories_total'] == 1
    assert aw['ansible_versions'] == ['2.10.0']

    # community.aws 1.5.0 → job 3 only (successful, 7s duration, 4s wait, T2, I2, v2.11.0)
    ca = collections_dict.get(('community.aws', '1.5.0'))
    assert ca['job_count'] == 1, f'Expected community.aws 1.5.0 in 1 job, got {ca}'
    assert ca['jobs_failed_total'] == 0
    assert ca['jobs_successful_total'] == 1
    assert ca['jobs_never_started_total'] == 0
    assert ca['jobs_duration_total_seconds'] == pytest.approx(7.0)
    assert ca['jobs_successful_duration_total_seconds'] == pytest.approx(7.0)
    assert ca['jobs_failed_duration_total_seconds'] == pytest.approx(0.0)
    assert ca['job_duration_maximum_seconds'] == pytest.approx(7.0)
    assert ca['job_duration_minimum_seconds'] == pytest.approx(7.0)
    assert ca['job_waiting_time_total_seconds'] == pytest.approx(4.0)
    assert ca['job_waiting_time_maximum_seconds'] == pytest.approx(4.0)
    assert ca['job_waiting_time_minimum_seconds'] == pytest.approx(4.0)
    assert ca['templates_total'] == 1
    assert ca['inventories_total'] == 1
    assert ca['ansible_versions'] == ['2.11.0']

    # Verify total number of unique collection-version pairs
    # Should have 6 unique pairs: ansible.builtin 2.9.10, community.general (3 versions), ansible.windows 1.0.0, community.aws 1.5.0
    assert len(installed_collections) == 6, f'Expected 6 unique collection-version pairs, got {len(installed_collections)}'

    # Verify all entries have required fields and invariants
    new_fields = [
        'jobs_never_started_total',
        'jobs_duration_total_seconds',
        'jobs_successful_duration_total_seconds',
        'jobs_failed_duration_total_seconds',
        'job_duration_maximum_seconds',
        'job_duration_minimum_seconds',
        'job_waiting_time_total_seconds',
        'job_waiting_time_maximum_seconds',
        'job_waiting_time_minimum_seconds',
        'templates',
        'templates_total',
        'inventories',
        'inventories_total',
        'ansible_versions',
    ]
    for collection in installed_collections:
        assert 'collection_name' in collection
        assert 'collection_version' in collection
        assert 'job_count' in collection
        assert 'jobs_failed_total' in collection
        assert 'jobs_successful_total' in collection
        assert isinstance(collection['job_count'], int)
        assert isinstance(collection['jobs_failed_total'], int)
        assert isinstance(collection['jobs_successful_total'], int)
        assert collection['job_count'] > 0
        assert collection['jobs_failed_total'] + collection['jobs_successful_total'] == collection['job_count']
        for field in new_fields:
            assert field in collection, f'Missing field {field!r} in {collection["collection_name"]} {collection["collection_version"]}'
        assert isinstance(collection['jobs_never_started_total'], int)
        assert isinstance(collection['templates_total'], int)
        assert isinstance(collection['inventories_total'], int)
        assert isinstance(collection['ansible_versions'], list)
        assert isinstance(collection['templates'], list)
        assert isinstance(collection['inventories'], list)
        assert collection['templates_total'] == len(collection['templates'])
        assert collection['inventories_total'] == len(collection['inventories'])
        # max/min durations: if there are no valid durations both must be None
        if collection['jobs_duration_total_seconds'] == 0 and collection['job_count'] == collection['jobs_never_started_total']:
            assert collection['job_duration_maximum_seconds'] is None
            assert collection['job_duration_minimum_seconds'] is None


# ===========================================================================
# Tests for untested / low-coverage code paths
# ===========================================================================


def test_prepare_empty_dataframe():
    """prepare() must return the canonical empty structure for an empty DataFrame (with columns)."""
    # Use a DataFrame that has the expected columns but zero rows – this is the realistic
    # scenario when the CSV collector produces an empty result set.
    df = pd.DataFrame(columns=list(jobs[0].keys()))
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    result = jobs_anonymized_rollup.prepare(df)

    assert result['by_job_type'] == []
    assert result['by_launch_type'] == []
    assert result['by_ansible_version'] == []
    assert result['by_controller_version'] == []
    assert result['organizations'] == []
    assert result['forks_total'] == 0
    assert result['scm_types'] == []
    assert result['installed_collections'] == []


def test_prepare_all_unfinished_jobs():
    """prepare() must return the canonical empty structure when all jobs have finished=None."""
    unfinished = [
        {
            'id': 1,
            'started': '2024-01-01 00:00:00.000000+00',
            'finished': None,
            'failed': 0,
            'job_template_name': 'T1',
            'unified_job_template_id': 1,
            'controller_node': 'ctrl-A',
            'ansible_version': '2.9.0',
            'organization_name': 'Org1',
            'created': '2024-01-01 00:00:00.000000+00',
            'model': 'job',
            'launch_type': 'manual',
            'forks': 5,
            'inventory_name': 'inventory1',
            'inventory_id': 1,
            'scm_type': 'git',
        }
    ]
    df = pd.DataFrame(unfinished)
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    result = jobs_anonymized_rollup.prepare(df)

    assert result['by_job_type'] == []
    assert result['by_launch_type'] == []
    assert result['by_ansible_version'] == []
    assert result['by_controller_version'] == []


def test_base_with_none_input():
    """base(None) must return the canonical None-placeholder structure."""
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    result = jobs_anonymized_rollup.base(None)['json']

    assert result['by_job_type'] == []
    assert result['by_launch_type'] == []
    assert result['by_ansible_version'] == []
    assert result['jobs_by_controller_version'] == []
    assert result['organizations_total'] is None
    assert result['forks_total'] is None
    assert result['jobs_total'] is None
    assert result['installed_collections'] == []
    assert result['scm_types'] == []


def test_base_with_empty_lists():
    """base() must return all-zero structure when every list is empty."""
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    empty_data = {
        'by_job_type': [],
        'by_launch_type': [],
        'by_ansible_version': [],
        'by_controller_version': [],
        'organizations': [],
        'forks_total': 0,
        'scm_types': [],
        'installed_collections': [],
    }
    result = jobs_anonymized_rollup.base(empty_data)['json']

    assert result['by_job_type'] == []
    assert result['by_launch_type'] == []
    assert result['by_ansible_version'] == []
    assert result['jobs_by_controller_version'] == []
    assert result['organizations_total'] == 0
    assert result['forks_total'] == 0
    assert result['jobs_total'] == 0
    assert result['installed_collections'] == []
    assert result['scm_types'] == []


def test_merge_with_none_data_all():
    """merge(None, data_new) must return data_new unchanged (first-iteration shortcut)."""
    jobs_anonymized_rollup = JobsAnonymizedRollup()
    df = pd.DataFrame(jobs)
    data_new = jobs_anonymized_rollup.prepare(df)

    result = jobs_anonymized_rollup.merge(None, data_new)
    assert result == data_new


def test_merge_two_batches():
    """merge() must correctly combine two disjoint batches of jobs."""
    jobs_batch1 = [jobs[0], jobs[1]]  # jobs 1 and 2 (both 'job' model)
    jobs_batch2 = [jobs[2], jobs[3]]  # jobs 3 (workflowjob) and 4 (job)

    rollup = JobsAnonymizedRollup()
    data1 = rollup.prepare(pd.DataFrame(jobs_batch1))
    data2 = rollup.prepare(pd.DataFrame(jobs_batch2))

    merged = rollup.merge(data1, data2)
    result = rollup.base(merged)['json']

    # All 4 jobs must be present
    assert result['jobs_total'] == 4

    by_job_type = result['by_job_type']
    rec_job = next((r for r in by_job_type if r['job_type'] == 'job'), None)
    rec_workflowjob = next((r for r in by_job_type if r['job_type'] == 'workflowjob'), None)

    assert rec_job is not None
    assert rec_workflowjob is not None
    # jobs 1, 2, 4 → 3 'job' records; job 3 → 1 'workflowjob'
    assert rec_job['jobs_total'] == 3
    assert rec_workflowjob['jobs_total'] == 1

    # Duration totals must be the sum across both batches
    assert rec_job['jobs_duration_total_seconds'] == pytest.approx(10.0)  # 3+5+2
    assert rec_workflowjob['jobs_duration_total_seconds'] == pytest.approx(7.0)

    # forks_total sums over both batches: jobs 1-4 → 5+10+20+15 = 50
    assert result['forks_total'] == 50

    # Organisations deduplicated across batches
    assert result['organizations_total'] == 2  # Org1 and Org2


def test_merge_stats_json_empty_stats_all():
    """_merge_stats_json must return stats_new when stats_all is empty."""
    rollup = JobsAnonymizedRollup()
    stats_new = [{'job_type': 'job', 'jobs_total': 5}]

    result = rollup._merge_stats_json([], stats_new, 'job_type')
    assert result == stats_new


def test_merge_stats_json_empty_stats_new():
    """_merge_stats_json must return stats_all when stats_new is empty."""
    rollup = JobsAnonymizedRollup()
    stats_all = [{'job_type': 'job', 'jobs_total': 5}]

    result = rollup._merge_stats_json(stats_all, [], 'job_type')
    assert result == stats_all


def test_merge_stats_json_both_empty():
    """_merge_stats_json must return [] when both inputs are empty."""
    rollup = JobsAnonymizedRollup()
    result = rollup._merge_stats_json([], [], 'job_type')
    assert result == []


def test_merge_stats_json_disjoint_keys():
    """_merge_stats_json must keep entries from both sides when keys do not overlap."""
    rollup = JobsAnonymizedRollup()
    stats_all = [
        {
            'job_type': 'job',
            'jobs_total': 3,
            'jobs_failed_total': 1,
            'jobs_successful_total': 2,
            'jobs_never_started_total': 0,
            'jobs_duration_total_seconds': 10.0,
            'jobs_successful_duration_total_seconds': 5.0,
            'jobs_failed_duration_total_seconds': 5.0,
            'job_duration_maximum_seconds': 5.0,
            'job_duration_minimum_seconds': 2.0,
            'job_waiting_time_maximum_seconds': 2.0,
            'job_waiting_time_minimum_seconds': 0.0,
            'job_waiting_time_total_seconds': 3.0,
            'templates_total': 1,
            'inventories_total': 1,
            'templates': ['1'],
            'inventories': ['1'],
            'ansible_versions': ['2.9.0'],
        }
    ]
    stats_new = [
        {
            'job_type': 'workflowjob',
            'jobs_total': 1,
            'jobs_failed_total': 0,
            'jobs_successful_total': 1,
            'jobs_never_started_total': 0,
            'jobs_duration_total_seconds': 7.0,
            'jobs_successful_duration_total_seconds': 7.0,
            'jobs_failed_duration_total_seconds': 0.0,
            'job_duration_maximum_seconds': 7.0,
            'job_duration_minimum_seconds': 7.0,
            'job_waiting_time_maximum_seconds': 4.0,
            'job_waiting_time_minimum_seconds': 4.0,
            'job_waiting_time_total_seconds': 4.0,
            'templates_total': 1,
            'inventories_total': 1,
            'templates': ['2'],
            'inventories': ['2'],
            'ansible_versions': ['2.11.0'],
        }
    ]

    result = rollup._merge_stats_json(stats_all, stats_new, 'job_type')
    assert len(result) == 2
    keys = {item['job_type'] for item in result}
    assert keys == {'job', 'workflowjob'}


def test_merge_stats_json_overlapping_keys():
    """_merge_stats_json must sum numeric fields for entries with the same key."""
    rollup = JobsAnonymizedRollup()
    base_item = {
        'job_type': 'job',
        'jobs_total': 3,
        'jobs_failed_total': 1,
        'jobs_successful_total': 2,
        'jobs_never_started_total': 0,
        'jobs_duration_total_seconds': 10.0,
        'jobs_successful_duration_total_seconds': 5.0,
        'jobs_failed_duration_total_seconds': 5.0,
        'job_duration_maximum_seconds': 5.0,
        'job_duration_minimum_seconds': 2.0,
        'job_waiting_time_maximum_seconds': 2.0,
        'job_waiting_time_minimum_seconds': 0.0,
        'job_waiting_time_total_seconds': 3.0,
        'templates_total': 1,
        'inventories_total': 1,
        'templates': ['1'],
        'inventories': ['1'],
        'ansible_versions': ['2.9.0'],
    }
    stats_all = [base_item]
    stats_new = [{**base_item, 'jobs_total': 2, 'jobs_failed_total': 0, 'jobs_successful_total': 2}]

    result = rollup._merge_stats_json(stats_all, stats_new, 'job_type')
    assert len(result) == 1
    merged = result[0]
    assert merged['jobs_total'] == 5
    assert merged['jobs_failed_total'] == 1
    assert merged['jobs_successful_total'] == 4


def test_merge_max_value_with_none():
    """_merge_max_value must handle None inputs correctly."""
    rollup = JobsAnonymizedRollup()

    assert rollup._merge_max_value(None, None) is None
    assert rollup._merge_max_value(None, 5.0) == pytest.approx(5.0)
    assert rollup._merge_max_value(5.0, None) == pytest.approx(5.0)
    assert rollup._merge_max_value(3.0, 7.0) == pytest.approx(7.0)
    assert rollup._merge_max_value(7.0, 3.0) == pytest.approx(7.0)


def test_merge_min_value_with_none():
    """_merge_min_value must handle None inputs correctly."""
    rollup = JobsAnonymizedRollup()

    assert rollup._merge_min_value(None, None) is None
    assert rollup._merge_min_value(None, 3.0) == pytest.approx(3.0)
    assert rollup._merge_min_value(3.0, None) == pytest.approx(3.0)
    assert rollup._merge_min_value(3.0, 7.0) == pytest.approx(3.0)
    assert rollup._merge_min_value(7.0, 3.0) == pytest.approx(3.0)


def test_merge_single_item_stats_empty_inputs():
    """_merge_single_item_stats must handle empty list inputs."""
    rollup = JobsAnonymizedRollup()

    assert rollup._merge_single_item_stats([], []) == []

    stats_new = [{'jobs_total': 5}]
    assert rollup._merge_single_item_stats([], stats_new) == stats_new

    stats_all = [{'jobs_total': 3}]
    assert rollup._merge_single_item_stats(stats_all, []) == stats_all


def test_merge_single_item_stats_combines():
    """_merge_single_item_stats must sum numeric fields when both inputs have one item."""
    rollup = JobsAnonymizedRollup()

    item = {
        'jobs_total': 3,
        'jobs_failed_total': 1,
        'jobs_successful_total': 2,
        'jobs_never_started_total': 0,
        'jobs_duration_total_seconds': 10.0,
        'jobs_successful_duration_total_seconds': 5.0,
        'jobs_failed_duration_total_seconds': 5.0,
        'job_duration_maximum_seconds': 5.0,
        'job_duration_minimum_seconds': 2.0,
        'job_waiting_time_maximum_seconds': 2.0,
        'job_waiting_time_minimum_seconds': 0.0,
        'job_waiting_time_total_seconds': 3.0,
        'templates_total': 1,
        'inventories_total': 1,
        'templates': ['1'],
        'inventories': ['1'],
        'ansible_versions': ['2.9.0'],
    }
    result = rollup._merge_single_item_stats([item], [item])
    assert len(result) == 1
    merged = result[0]
    assert merged['jobs_total'] == 6
    assert merged['jobs_failed_total'] == 2
    assert merged['jobs_successful_total'] == 4
    assert merged['jobs_duration_total_seconds'] == pytest.approx(20.0)


def test_parse_collections_data_variants():
    """_parse_collections_data must handle None, empty, invalid, and valid inputs."""
    rollup = JobsAnonymizedRollup()

    assert rollup._parse_collections_data(None) is None
    assert rollup._parse_collections_data('') is None
    assert rollup._parse_collections_data('not valid json {') is None

    parsed = rollup._parse_collections_data('{"ansible.builtin": {"version": "2.9.10"}}')
    assert parsed == {'ansible.builtin': {'version': '2.9.10'}}


def test_get_collection_cache_key_with_ee_id():
    """_get_collection_cache_key must use execution_environment_id when present."""
    import collections as _collections

    rollup = JobsAnonymizedRollup()
    MockRow = _collections.namedtuple('MockRow', ['installed_collections', 'execution_environment_id'])
    row = MockRow(installed_collections='{}', execution_environment_id=42)

    key = rollup._get_collection_cache_key(row, row.installed_collections)
    assert key == ('ee', 42)


def test_get_collection_cache_key_without_ee_id():
    """_get_collection_cache_key must fall back to raw hash when EE id is absent."""
    import collections as _collections

    rollup = JobsAnonymizedRollup()
    MockRow = _collections.namedtuple('MockRow', ['installed_collections'])
    row = MockRow(installed_collections='{"ansible.builtin": {"version": "1.0.0"}}')

    key = rollup._get_collection_cache_key(row, row.installed_collections)
    assert key[0] == 'raw'
    assert isinstance(key[1], int)


def test_get_collection_cache_key_with_nan_ee_id():
    """_get_collection_cache_key must fall back to raw hash when EE id is NaN."""
    import collections as _collections

    rollup = JobsAnonymizedRollup()
    MockRow = _collections.namedtuple('MockRow', ['installed_collections', 'execution_environment_id'])
    row = MockRow(installed_collections='{}', execution_environment_id=float('nan'))

    key = rollup._get_collection_cache_key(row, row.installed_collections)
    assert key[0] == 'raw'


def test_process_collections_from_jobs_no_column():
    """_process_collections_from_jobs must return [] when the column is absent."""
    rollup = JobsAnonymizedRollup()
    df = pd.DataFrame([{'id': 1, 'model': 'job', 'failed': 0}])
    assert rollup._process_collections_from_jobs(df) == []


def test_is_valid_id():
    """_is_valid_id must distinguish valid identifiers from None/NaN."""
    rollup = JobsAnonymizedRollup()

    assert rollup._is_valid_id(1) is True
    assert rollup._is_valid_id('abc') is True
    assert rollup._is_valid_id(0) is True
    assert rollup._is_valid_id(None) is False
    assert rollup._is_valid_id(float('nan')) is False


def test_compute_list_length():
    """_compute_list_length must return 0 for non-list inputs."""
    rollup = JobsAnonymizedRollup()

    assert rollup._compute_list_length([1, 2, 3]) == 3
    assert rollup._compute_list_length([]) == 0
    assert rollup._compute_list_length(None) == 0
    assert rollup._compute_list_length('not a list') == 0
    assert rollup._compute_list_length(42) == 0


def test_process_single_collection_missing_version():
    """_process_single_collection must skip collection_info dicts without a 'version' key."""
    rollup = JobsAnonymizedRollup()
    collections_stats = {}
    row_stats = {
        'job_duration_seconds': 5.0,
        'job_waiting_time_seconds': 1.0,
        'jobs_never_started': False,
        'unified_job_template_id': '1',
        'inventory_id': '1',
        'ansible_version': '2.9.0',
    }

    rollup._process_single_collection('ansible.builtin', {'name': 'no-version'}, collections_stats, False, row_stats)
    assert len(collections_stats) == 0


def test_process_single_collection_non_dict_info():
    """_process_single_collection must skip entries where collection_info is not a dict."""
    rollup = JobsAnonymizedRollup()
    collections_stats = {}

    rollup._process_single_collection('ansible.builtin', 'not-a-dict', collections_stats, False, {})
    assert len(collections_stats) == 0


def test_process_collections_dict_non_dict_input():
    """_process_collections_dict must be a no-op when collections_data is not a dict."""
    rollup = JobsAnonymizedRollup()
    collections_stats = {}

    rollup._process_collections_dict('not-a-dict', collections_stats, False, {})
    rollup._process_collections_dict(None, collections_stats, False, {})
    rollup._process_collections_dict([], collections_stats, False, {})
    assert len(collections_stats) == 0


def test_merge_list_fields():
    """_merge_list_fields must union two lists and sort the result."""
    rollup = JobsAnonymizedRollup()

    data_all = {'organizations': ['Org1', 'Org2']}
    data_new = {'organizations': ['Org2', 'Org3']}
    result = rollup._merge_list_fields(data_all, data_new, 'organizations')
    assert result == ['Org1', 'Org2', 'Org3']


def test_merge_list_fields_missing_key():
    """_merge_list_fields must handle a missing key in one dict."""
    rollup = JobsAnonymizedRollup()

    result = rollup._merge_list_fields({}, {'organizations': ['Org1']}, 'organizations')
    assert result == ['Org1']

    result = rollup._merge_list_fields({'organizations': ['Org1']}, {}, 'organizations')
    assert result == ['Org1']


def test_merge_single_collection_combines_stats():
    """_merge_single_collection must sum numeric fields, take max/min, and union lists."""
    rollup = JobsAnonymizedRollup()

    item_all = {
        'job_count': 3,
        'jobs_failed_total': 1,
        'jobs_successful_total': 2,
        'jobs_never_started_total': 0,
        'jobs_duration_total_seconds': 10.0,
        'jobs_successful_duration_total_seconds': 5.0,
        'jobs_failed_duration_total_seconds': 5.0,
        'job_duration_maximum_seconds': 5.0,
        'job_duration_minimum_seconds': 2.0,
        'job_waiting_time_total_seconds': 3.0,
        'job_waiting_time_maximum_seconds': 2.0,
        'job_waiting_time_minimum_seconds': 1.0,
        'templates': ['1', '2'],
        'inventories': ['1'],
        'ansible_versions': ['2.9.0'],
    }
    item_new = {
        'job_count': 2,
        'jobs_failed_total': 0,
        'jobs_successful_total': 2,
        'jobs_never_started_total': 1,
        'jobs_duration_total_seconds': 8.0,
        'jobs_successful_duration_total_seconds': 8.0,
        'jobs_failed_duration_total_seconds': 0.0,
        'job_duration_maximum_seconds': 7.0,
        'job_duration_minimum_seconds': 1.0,
        'job_waiting_time_total_seconds': 5.0,
        'job_waiting_time_maximum_seconds': 4.0,
        'job_waiting_time_minimum_seconds': 1.0,
        'templates': ['2', '3'],
        'inventories': ['2'],
        'ansible_versions': ['2.10.0', '2.11.0'],
    }

    result = rollup._merge_single_collection(item_all, item_new)

    assert result['job_count'] == 5
    assert result['jobs_failed_total'] == 1
    assert result['jobs_successful_total'] == 4
    assert result['jobs_never_started_total'] == 1
    assert result['jobs_duration_total_seconds'] == pytest.approx(18.0)
    assert result['jobs_successful_duration_total_seconds'] == pytest.approx(13.0)
    assert result['jobs_failed_duration_total_seconds'] == pytest.approx(5.0)
    assert result['job_duration_maximum_seconds'] == pytest.approx(7.0)
    assert result['job_duration_minimum_seconds'] == pytest.approx(1.0)
    assert result['job_waiting_time_total_seconds'] == pytest.approx(8.0)
    assert result['job_waiting_time_maximum_seconds'] == pytest.approx(4.0)
    assert result['job_waiting_time_minimum_seconds'] == pytest.approx(1.0)
    assert result['templates'] == ['1', '2', '3']
    assert result['templates_total'] == 3
    assert result['inventories'] == ['1', '2']
    assert result['inventories_total'] == 2
    assert result['ansible_versions'] == ['2.10.0', '2.11.0', '2.9.0']


def test_merge_collections_empty():
    """_merge_collections must return [] when both sides have empty installed_collections."""
    rollup = JobsAnonymizedRollup()
    result = rollup._merge_collections({'installed_collections': []}, {'installed_collections': []})
    assert result == []


def test_merge_collections_new_only():
    """_merge_collections must adopt data_new collections when data_all has none."""
    rollup = JobsAnonymizedRollup()

    data_new = {
        'installed_collections': [
            {
                'collection_name': 'ansible.builtin',
                'collection_version': '2.9.10',
                'job_count': 3,
                'jobs_failed_total': 1,
                'jobs_successful_total': 2,
                'jobs_never_started_total': 0,
                'jobs_duration_total_seconds': 10.0,
                'jobs_successful_duration_total_seconds': 5.0,
                'jobs_failed_duration_total_seconds': 5.0,
                'job_duration_maximum_seconds': 5.0,
                'job_duration_minimum_seconds': 2.0,
                'job_waiting_time_total_seconds': 3.0,
                'job_waiting_time_maximum_seconds': 2.0,
                'job_waiting_time_minimum_seconds': 1.0,
                'templates': ['1', '2'],
                'inventories': ['1'],
                'templates_total': 2,
                'inventories_total': 1,
                'ansible_versions': ['2.9.0'],
            }
        ]
    }
    result = rollup._merge_collections({'installed_collections': []}, data_new)
    assert len(result) == 1
    assert result[0]['collection_name'] == 'ansible.builtin'
    assert result[0]['collection_version'] == '2.9.10'
    assert result[0]['job_count'] == 3


def test_merge_full_pipeline_two_batches_installed_collections():
    """Full prepare→merge→base pipeline must correctly aggregate installed_collections across batches."""
    batch1 = [jobs[0]]  # job 1: ansible.builtin 2.9.10, community.general 1.0.0
    batch2 = [jobs[3]]  # job 4: ansible.builtin 2.9.10, community.general 1.0.0 (same EE)

    rollup = JobsAnonymizedRollup()
    data1 = rollup.prepare(pd.DataFrame(batch1))
    data2 = rollup.prepare(pd.DataFrame(batch2))

    merged = rollup.merge(data1, data2)
    result = rollup.base(merged)['json']

    installed_collections = result['installed_collections']
    coll_dict = {(c['collection_name'], c['collection_version']): c for c in installed_collections}

    ab = coll_dict.get(('ansible.builtin', '2.9.10'))
    assert ab is not None
    assert ab['job_count'] == 2  # both jobs use the same collection

    cg1 = coll_dict.get(('community.general', '1.0.0'))
    assert cg1 is not None
    assert cg1['job_count'] == 2


def _extract_ansible_versions_from_jobs(jobs_by_job_type):
    """Extract and merge controller versions from jobs_by_job_type."""
    expected_versions_set = set()
    for job in jobs_by_job_type:
        ansible_versions = job.get('ansible_versions', [])
        if isinstance(ansible_versions, list):
            expected_versions_set.update(ansible_versions)
    return sorted(expected_versions_set)


def _validate_ansible_versions(result, expected_versions):
    """Validate controller versions at top level."""
    assert 'rollup_period_ansible_versions' in result, 'Should have ansible_versions at top level'
    assert result['rollup_period_ansible_versions'] == expected_versions, (
        f'Expected ansible_versions {expected_versions} at top level, got {result["rollup_period_ansible_versions"]}'
    )
    assert len(result['rollup_period_ansible_versions']) == 5, (
        f'Expected 5 unique controller versions, got {len(result["rollup_period_ansible_versions"])}'
    )
    for version in ['2.9.0', '2.10.0', '2.11.0', '2.12.0', '2.14.0']:
        assert version in result['rollup_period_ansible_versions']


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


def test_jobs_anonymized_rollups_statistics_ansible_versions():
    """Test that ansible_versions in statistics is correctly merged from jobs_by_job_type."""
    import os
    import shutil

    from datetime import datetime

    from metrics_utility.test.test_anonymized_rollups.helpers import compute_anonymized_rollup_from_raw_data
    from metrics_utility.test.test_anonymized_rollups.test_credentials_anonymized_rollup import credentials
    from metrics_utility.test.test_anonymized_rollups.test_events_modules_anonymized_rollups import events
    from metrics_utility.test.test_anonymized_rollups.test_execution_environments_anonymized_rollups import execution_environments
    from metrics_utility.test.test_anonymized_rollups.test_jobhostsummary_anonymized_rollups import jobhostsummary

    # Cleanup
    out_dir = './out'
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    since = datetime(2025, 6, 13, 0, 0, 0)
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

    result = compute_anonymized_rollup_from_raw_data(input_data=input_data)

    # Validate result has ansible_versions at top level
    assert 'statistics' in result, 'Should have statistics in result'
    statistics = result['statistics']
    assert 'rollup_period_jobs_successful' in statistics, 'Should have jobs_successful in statistics'
    assert 'rollup_period_jobs_failed' in statistics, 'Should have jobs_failed in statistics'
    assert 'rollup_period_jobs_duration_all_statuses_seconds' in statistics, 'Should have jobs_duration_all_statuses_seconds in statistics'
    assert 'rollup_period_jobs_successful_duration_total_seconds' in statistics, 'Should have jobs_successful_duration_total_seconds in statistics'
    assert 'rollup_period_jobs_failed_duration_total_seconds' in statistics, 'Should have jobs_failed_duration_total_seconds in statistics'

    # Get ansible_versions from jobs_by_job_type
    jobs_by_job_type = result.get('jobs_by_job_type', [])
    expected_versions = _extract_ansible_versions_from_jobs(jobs_by_job_type)

    # Validate ansible_versions at top level matches merged values from jobs_by_job_type
    _validate_ansible_versions(result, expected_versions)

    # Validate new job statistics match sum from jobs_by_job_type
    _validate_job_statistics(statistics, jobs_by_job_type)

    # Validate scm_types at top level
    assert 'rollup_period_scm_types' in result, 'Should have rollup_period_scm_types at top level'
    assert result['rollup_period_scm_types'] == ['git', 'svn', 'unknown'], (
        f"Expected ['git', 'svn', 'unknown'] for rollup_period_scm_types, got {result['rollup_period_scm_types']}"
    )

    # Validate jobs_by_controller_version in the flattened output
    assert 'jobs_by_controller_version' in result, 'Should have jobs_by_controller_version in result'
    ctrl_summary_list = result['jobs_by_controller_version']
    assert isinstance(ctrl_summary_list, list), 'jobs_by_controller_version should be a list'
    assert len(ctrl_summary_list) == 1, 'jobs_by_controller_version should have exactly 1 item'
    ctrl_summary = ctrl_summary_list[0]
    assert ctrl_summary['jobs_total'] == 5, f'Expected 5 total jobs, got {ctrl_summary["jobs_total"]}'
    assert ctrl_summary['jobs_failed_total'] == 2
    assert ctrl_summary['jobs_successful_total'] == 3
    # No controller_version data was provided in input_data, so it is injected as None
    assert ctrl_summary.get('controller_version') is None, (
        f'controller_version should be None when no controller_version data is provided, got {ctrl_summary.get("controller_version")!r}'
    )
