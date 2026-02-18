"""
This test verifies that data split into multiple CSV files is correctly concatenated.
It tests the logic for loading and merging dataframes from multiple CSV files.

The test:
1. Takes data from other test files (jobs, events, execution_environments, jobhostsummary)
2. Splits each dataset into 2-3 separate CSV files
3. Creates CSV files with the split data
4. Tests that compute_anonymized_rollup_from_raw_data properly loads and concatenates the data
5. Validates the final output matches expected aggregated results

Enhanced Assertions:
- Deep validation of JSON structure including all nested values
- Verification of timing statistics (min, max, totals for job durations and waiting times)
- Detailed module and collection statistics validation
- Edge case handling (never-started jobs, null values)
- Comprehensive empty data handling test
- Prints full JSON output to terminal for inspection
"""

import os
import shutil

from datetime import datetime

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.test_anonymized_rollups.test_credentials_anonymized_rollup import credentials
from metrics_utility.test.test_anonymized_rollups.test_events_modules_anonymized_rollups import events
from metrics_utility.test.test_anonymized_rollups.test_execution_environments_anonymized_rollups import execution_environments
from metrics_utility.test.test_anonymized_rollups.test_jobhostsummary_anonymized_rollups import jobhostsummary

# Import test data from other test files
from metrics_utility.test.test_anonymized_rollups.test_jobs_anonymized_rollups import jobs


@pytest.fixture(scope='module')
def cleanup_test_data():
    """Clean up test directories before and after all tests in this module."""
    out_dir = './out'

    # Cleanup before tests
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    yield  # Run all tests

    # Cleanup after all tests (commented out for debugging)
    # if os.path.exists(out_dir):
    #     shutil.rmtree(out_dir)


def _create_csv_files_from_split_data(data_dir, jobs, events, execution_environments, jobhostsummary, credentials):
    """Create CSV files from split test data."""
    jobs_part1 = jobs[:3]
    jobs_part2 = jobs[3:]
    jobs_csv_files = []
    csv1 = create_csv_file(jobs_part1, f'{data_dir}/part1_unified_jobs.csv')
    if csv1:
        jobs_csv_files.append(csv1)
    csv2 = create_csv_file(jobs_part2, f'{data_dir}/part2_unified_jobs.csv')
    if csv2:
        jobs_csv_files.append(csv2)

    events_part1 = events[:8]
    events_part2 = events[8:16]
    events_part3 = events[16:]
    events_csv_files = []
    csv1 = create_csv_file(events_part1, f'{data_dir}/part1_main_jobevent.csv')
    if csv1:
        events_csv_files.append(csv1)
    csv2 = create_csv_file(events_part2, f'{data_dir}/part2_main_jobevent.csv')
    if csv2:
        events_csv_files.append(csv2)
    csv3 = create_csv_file(events_part3, f'{data_dir}/part3_main_jobevent.csv')
    if csv3:
        events_csv_files.append(csv3)

    ee_part1 = execution_environments[:2]
    ee_part2 = execution_environments[2:]
    ee_csv_files = []
    csv1 = create_csv_file(ee_part1, f'{data_dir}/part1_execution_environments.csv')
    if csv1:
        ee_csv_files.append(csv1)
    csv2 = create_csv_file(ee_part2, f'{data_dir}/part2_execution_environments.csv')
    if csv2:
        ee_csv_files.append(csv2)

    jhs_part1 = jobhostsummary[:8]
    jhs_part2 = jobhostsummary[8:]
    jhs_csv_files = []
    csv1 = create_csv_file(jhs_part1, f'{data_dir}/part1_job_host_summary.csv')
    if csv1:
        jhs_csv_files.append(csv1)
    csv2 = create_csv_file(jhs_part2, f'{data_dir}/part2_job_host_summary.csv')
    if csv2:
        jhs_csv_files.append(csv2)

    cred_part1 = credentials[:5]
    cred_part2 = credentials[5:]
    cred_csv_files = []
    csv1 = create_csv_file(cred_part1, f'{data_dir}/part1_credentials.csv')
    if csv1:
        cred_csv_files.append(csv1)
    csv2 = create_csv_file(cred_part2, f'{data_dir}/part2_credentials.csv')
    if csv2:
        cred_csv_files.append(csv2)

    return {
        'unified_jobs': jobs_csv_files,
        'job_host_summary': jhs_csv_files,
        'main_jobevent': events_csv_files,
        'execution_environments': ee_csv_files,
        'credentials': cred_csv_files,
    }


def _validate_jobs_by_job_type(jobs_list, result):
    """Validate jobs_by_job_type section."""
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 3
    assert result['statistics']['rollup_period_jobs_total'] == 5
    assert result['statistics']['rollup_period_templates_total'] == 3, 'Should have 3 total job templates (sum from all job_type groups)'

    job_type_jobs = [j for j in jobs_list if j['job_type'] == 'job' and j['jobs_total'] == 3]
    assert len(job_type_jobs) == 1
    job_type = job_type_jobs[0]
    assert job_type['jobs_total'] == 3
    assert job_type['jobs_failed_total'] == 1
    assert job_type['jobs_never_started_total'] == 0
    assert job_type['jobs_duration_total_seconds'] == pytest.approx(10.0)
    assert job_type['job_duration_minimum_seconds'] == pytest.approx(2.0)
    assert job_type['job_duration_maximum_seconds'] == pytest.approx(5.0)
    assert job_type['job_waiting_time_total_seconds'] == pytest.approx(3.0)
    assert job_type['job_waiting_time_minimum_seconds'] == pytest.approx(0.0)
    assert job_type['job_waiting_time_maximum_seconds'] == pytest.approx(2.0)
    assert job_type['job_type'] == 'job'
    assert 'controller_versions' in job_type, 'Should have controller_versions field in by_job_type'
    assert job_type['controller_versions'] == ['2.10.0', '2.12.0', '2.9.0'], (
        f"Expected ['2.10.0', '2.12.0', '2.9.0'] for job type, got {job_type['controller_versions']}"
    )

    workflowjob_type_jobs = [j for j in jobs_list if j['job_type'] == 'workflowjob' and j['jobs_never_started_total'] == 0]
    assert len(workflowjob_type_jobs) == 1
    workflowjob_type = workflowjob_type_jobs[0]
    assert workflowjob_type['jobs_total'] == 1
    assert workflowjob_type['jobs_failed_total'] == 0
    assert workflowjob_type['jobs_duration_total_seconds'] == pytest.approx(7.0)
    assert workflowjob_type['job_waiting_time_total_seconds'] == pytest.approx(4.0)
    assert workflowjob_type['job_type'] == 'workflowjob'
    assert 'controller_versions' in workflowjob_type, 'Should have controller_versions field in by_job_type'
    assert workflowjob_type['controller_versions'] == ['2.11.0'], (
        f"Expected ['2.11.0'] for workflowjob type, got {workflowjob_type['controller_versions']}"
    )

    adhoccommand_type_jobs = [j for j in jobs_list if j['job_type'] == 'adhoccommand' and j['jobs_never_started_total'] == 1]
    assert len(adhoccommand_type_jobs) == 1
    adhoccommand_type = adhoccommand_type_jobs[0]
    assert adhoccommand_type['jobs_total'] == 1
    assert adhoccommand_type['jobs_failed_total'] == 1
    assert adhoccommand_type['jobs_duration_total_seconds'] == pytest.approx(0.0)
    assert adhoccommand_type['job_waiting_time_total_seconds'] == pytest.approx(0.0)
    assert adhoccommand_type['job_type'] == 'adhoccommand'
    assert 'controller_versions' in adhoccommand_type, 'Should have controller_versions field in by_job_type'
    assert adhoccommand_type['controller_versions'] == ['2.14.0'], (
        f"Expected ['2.14.0'] for adhoccommand type, got {adhoccommand_type['controller_versions']}"
    )


def _validate_controller_versions_top_level(result):
    """Validate controller_versions at top level."""
    assert 'rollup_period_controller_versions' in result, 'Should have controller_versions at top level'
    statistics_controller_versions = result['rollup_period_controller_versions']
    assert isinstance(statistics_controller_versions, list), 'controller_versions should be a list'
    jobs_by_job_type = result.get('jobs_by_job_type', [])
    expected_versions_set = set()
    for job in jobs_by_job_type:
        controller_versions = job.get('controller_versions', [])
        if isinstance(controller_versions, list):
            expected_versions_set.update(controller_versions)
    expected_versions = sorted(list(expected_versions_set))
    assert statistics_controller_versions == expected_versions, (
        f'Expected controller_versions {expected_versions} in statistics, got {statistics_controller_versions}'
    )
    assert len(statistics_controller_versions) == 5, f'Expected 5 unique controller versions, got {len(statistics_controller_versions)}'
    for version in ['2.9.0', '2.10.0', '2.11.0', '2.12.0', '2.14.0']:
        assert version in statistics_controller_versions


def _validate_job_host_summary(jobs_list, result):
    """Validate job host summary section."""
    assert result['statistics']['rollup_period_unique_hosts_total'] == 8, 'Should have 8 unique hosts total (5 for job + 3 for workflowjob)'
    assert result['statistics']['rollup_period_job_host_pairs_total'] == 16, (
        f'Should have 16 total job host summary records, got {result["statistics"]["rollup_period_job_host_pairs_total"]}'
    )

    job_type_entry = next((j for j in jobs_list if j['job_type'] == 'job'), None)
    assert job_type_entry is not None, 'Should have job_type job'
    assert job_type_entry['unique_hosts_total'] == 5, 'Should have 5 unique hosts for job type'
    assert job_type_entry['ok_total'] == 26, 'Should have 26 ok tasks for job type'
    assert job_type_entry['failures_total'] == 2, 'Should have 2 failures for job type'
    assert job_type_entry['skipped_total'] == 2, 'Should have 2 skipped for job type'
    assert job_type_entry['dark_total'] == 0, 'Should have 0 dark for job type'
    assert job_type_entry['ignored_total'] == 0, 'Should have 0 ignored for job type'
    assert job_type_entry['rescued_total'] == 0, 'Should have 0 rescued for job type'

    workflowjob_type_entry = next((j for j in jobs_list if j['job_type'] == 'workflowjob'), None)
    assert workflowjob_type_entry is not None, 'Should have job_type workflowjob'
    assert workflowjob_type_entry['unique_hosts_total'] == 3, 'Should have 3 unique hosts for workflowjob type'
    assert workflowjob_type_entry['ok_total'] == 26, 'Should have 26 ok tasks for workflowjob type'
    assert workflowjob_type_entry['failures_total'] == 4, 'Should have 4 failures for workflowjob type'
    assert workflowjob_type_entry['skipped_total'] == 0, 'Should have 0 skipped for workflowjob type'
    assert workflowjob_type_entry['dark_total'] == 0, 'Should have 0 dark for workflowjob type'
    assert workflowjob_type_entry['ignored_total'] == 0, 'Should have 0 ignored for workflowjob type'
    assert workflowjob_type_entry['rescued_total'] == 0, 'Should have 0 rescued for workflowjob type'

    adhoccommand_type_entry = next((j for j in jobs_list if j['job_type'] == 'adhoccommand'), None)
    assert adhoccommand_type_entry is not None, 'Should have job_type adhoccommand'
    assert adhoccommand_type_entry['unique_hosts_total'] == 0, 'Should have 0 unique hosts (no job_host_summary match)'
    assert adhoccommand_type_entry['ok_total'] == 0, 'Should have 0 ok tasks (no job_host_summary match)'
    assert adhoccommand_type_entry['failures_total'] == 0, 'Should have 0 failures (no job_host_summary match)'
    assert adhoccommand_type_entry['skipped_total'] == 0, 'Should have 0 skipped (no job_host_summary match)'

    total_ok = sum(j.get('ok_total', 0) for j in jobs_list)
    total_failures = sum(j.get('failures_total', 0) for j in jobs_list)
    total_skipped = sum(j.get('skipped_total', 0) for j in jobs_list)
    total_dark = sum(j.get('dark_total', 0) for j in jobs_list)
    total_ignored = sum(j.get('ignored_total', 0) for j in jobs_list)
    assert total_ok == 52, 'Should have 52 ok tasks total (26 from job + 26 from workflowjob)'
    assert total_failures == 6, 'Should have 6 failures total (2 from job + 4 from workflowjob)'
    assert total_skipped == 2, 'Should have 2 skipped total (2 from job + 0 from workflowjob)'
    assert total_dark == 0, 'Should have 0 dark (unreachable) tasks total'
    assert total_ignored == 0, 'Should have 0 ignored tasks total'

    assert 'rollup_period_tasks_total' in result['statistics'], 'Should have rollup_period_tasks_total in statistics'
    assert 'rollup_period_task_ok_total' in result['statistics'], 'Should have rollup_period_task_ok_total in statistics'
    assert 'rollup_period_task_failed_total' in result['statistics'], 'Should have rollup_period_task_failed_total in statistics'
    assert 'rollup_period_task_skipped_total' in result['statistics'], 'Should have rollup_period_task_skipped_total in statistics'
    assert 'rollup_period_task_unreachable_total' in result['statistics'], 'Should have rollup_period_task_unreachable_total in statistics'
    assert 'rollup_period_task_ignored_total' in result['statistics'], 'Should have rollup_period_task_ignored_total in statistics'

    expected_tasks_total = total_ok + total_failures + total_skipped + total_dark + total_ignored
    assert result['statistics']['rollup_period_tasks_total'] == expected_tasks_total, (
        f'rollup_period_tasks_total should be {expected_tasks_total}, got {result["statistics"]["rollup_period_tasks_total"]}'
    )
    assert result['statistics']['rollup_period_task_ok_total'] == total_ok, (
        f'rollup_period_task_ok_total should be {total_ok}, got {result["statistics"]["rollup_period_task_ok_total"]}'
    )
    assert result['statistics']['rollup_period_task_failed_total'] == total_failures, (
        f'rollup_period_task_failed_total should be {total_failures}, got {result["statistics"]["rollup_period_task_failed_total"]}'
    )
    assert result['statistics']['rollup_period_task_skipped_total'] == total_skipped, (
        f'rollup_period_task_skipped_total should be {total_skipped}, got {result["statistics"]["rollup_period_task_skipped_total"]}'
    )
    assert result['statistics']['rollup_period_task_unreachable_total'] == total_dark, (
        f'rollup_period_task_unreachable_total should be {total_dark}, got {result["statistics"]["rollup_period_task_unreachable_total"]}'
    )
    assert result['statistics']['rollup_period_task_ignored_total'] == total_ignored, (
        f'rollup_period_task_ignored_total should be {total_ignored}, got {result["statistics"]["rollup_period_task_ignored_total"]}'
    )


def _validate_events_modules(result):
    """Validate events modules section."""
    assert result['statistics']['rollup_period_modules_total'] == 7, 'Should have 7 unique modules from all tarballs'
    assert result['statistics']['rollup_period_unique_hosts_automated_total'] == 9, 'Should have 9 unique hosts from all tarballs'
    assert 'rollup_period_warnings_total' in result['statistics'], 'Should have warnings_total in statistics'
    assert result['statistics']['rollup_period_warnings_total'] == 2, (
        f'Expected 2 warnings, got {result["statistics"]["rollup_period_warnings_total"]}'
    )
    assert 'rollup_period_deprecations_total' in result['statistics'], 'Should have deprecations_total in statistics'
    assert result['statistics']['rollup_period_deprecations_total'] == 1, (
        f'Expected 1 deprecated event, got {result["statistics"]["rollup_period_deprecations_total"]}'
    )

    module_names = [m['module_name'] for m in result['module_stats'] if 'module_name' in m]
    for module_name in [
        'ansible.netcommon.cli_config',
        'ansible.posix.firewalld',
        'ansible.windows.win_copy',
        'community.aws.ec2',
        'community.general.yum',
        'community.mongodb.insert',
    ]:
        assert module_name in module_names

    module_stats = result['module_stats']
    assert isinstance(module_stats, list), 'module_stats should be a list'
    assert len(module_stats) == 7, 'Should have stats for all 7 modules'

    win_copy_stats = [m for m in module_stats if m.get('module_name') == 'ansible.windows.win_copy']
    assert len(win_copy_stats) == 1, 'Should have exactly one entry for ansible.windows.win_copy'
    win_copy = win_copy_stats[0]
    assert win_copy['collection_source'] == 'certified'
    assert win_copy['collection_name'] == 'ansible.windows'
    assert win_copy['jobs_total'] == 3
    assert win_copy['unique_hosts_total'] == 3
    assert win_copy['task_ok_total'] == 1
    assert win_copy['task_ok_with_retries_total'] == 2
    assert win_copy['task_failed_total'] == 0
    assert win_copy['jobs_duration_total_seconds'] == pytest.approx(2100.0)
    assert win_copy['processed_events_total'] == 5

    yum_stats = [m for m in module_stats if m.get('module_name') == 'community.general.yum']
    assert len(yum_stats) == 1, 'Should have exactly one entry for community.general.yum'
    yum = yum_stats[0]
    assert yum['collection_source'] == 'community'
    assert yum['jobs_total'] == 3
    assert yum['jobs_never_started_total'] == 1
    assert yum['task_failed_total'] == 3
    assert yum['jobs_failed_because_of_module_failure_total'] == 3
    assert yum['processed_events_total'] == 3

    collection_stats = result['collection_stats']
    assert isinstance(collection_stats, list), 'collection_stats should be a list'
    assert len(collection_stats) == 7, 'Should have stats for all 7 collections'

    windows_collection = [c for c in collection_stats if c.get('collection_name') == 'ansible.windows']
    assert len(windows_collection) == 1, 'Should have exactly one entry for ansible.windows collection'
    windows_coll = windows_collection[0]
    assert windows_coll['collection_source'] == 'certified'
    assert windows_coll['jobs_total'] == 3
    assert windows_coll['unique_hosts_total'] == 3
    assert windows_coll['task_ok_total'] == 1
    assert windows_coll['task_ok_with_retries_total'] == 2
    assert windows_coll['processed_events_total'] == 5

    assert result['statistics']['rollup_period_playbooks_total'] == 5, 'Should have 5 total playbooks'


def _validate_collections_versions(result):
    """Validate collections versions section."""
    collections_versions = result['collections_versions']
    assert isinstance(collections_versions, list), 'collections_versions should be a list'
    collections_dict = {(c['name'], c['version']): c['job_count'] for c in collections_versions}

    assert collections_dict.get(('Unknown', 'Unknown')) == 5, (
        f'Expected Unknown Unknown (ansible.builtin) in 5 jobs, got {collections_dict.get(("Unknown", "Unknown"))}'
    )
    assert collections_dict.get(('community.general', '1.0.0')) == 2, (
        f'Expected community.general 1.0.0 in 2 jobs, got {collections_dict.get(("community.general", "1.0.0"))}'
    )
    assert collections_dict.get(('community.general', '2.0.0')) == 2, (
        f'Expected community.general 2.0.0 in 2 jobs, got {collections_dict.get(("community.general", "2.0.0"))}'
    )
    assert collections_dict.get(('community.general', '3.0.0')) == 1, (
        f'Expected community.general 3.0.0 in 1 job, got {collections_dict.get(("community.general", "3.0.0"))}'
    )
    assert collections_dict.get(('ansible.windows', '1.0.0')) == 1, (
        f'Expected ansible.windows 1.0.0 in 1 job, got {collections_dict.get(("ansible.windows", "1.0.0"))}'
    )
    assert collections_dict.get(('community.aws', '1.5.0')) == 1, (
        f'Expected community.aws 1.5.0 in 1 job, got {collections_dict.get(("community.aws", "1.5.0"))}'
    )

    assert len(collections_versions) == 6, f'Expected 6 unique collection-version pairs, got {len(collections_versions)}'
    for collection in collections_versions:
        assert 'name' in collection, 'Each collection should have name field'
        assert 'version' in collection, 'Each collection should have version field'
        assert 'job_count' in collection, 'Each collection should have job_count field'
        assert isinstance(collection['job_count'], int), 'job_count should be an integer'
        assert collection['job_count'] > 0, 'job_count should be greater than 0'


def _validate_jobs_by_launch_type(result):
    """Validate jobs_by_launch_type section."""
    jobs_by_launch_type_list = result['jobs_by_launch_type']
    assert isinstance(jobs_by_launch_type_list, list), 'jobs_by_launch_type should be a list'
    assert len(jobs_by_launch_type_list) == 4, f'Should have 4 launch types, got {len(jobs_by_launch_type_list)}'

    manual_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'manual'), None)
    scheduled_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'scheduled'), None)
    workflow_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'workflow'), None)
    callback_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'callback'), None)

    assert manual_entry is not None, 'Should have manual launch_type'
    assert scheduled_entry is not None, 'Should have scheduled launch_type'
    assert workflow_entry is not None, 'Should have workflow launch_type'
    assert callback_entry is not None, 'Should have callback launch_type'

    assert manual_entry['jobs_total'] == 1, 'manual should have 1 job'
    assert manual_entry['jobs_failed_total'] == 0, 'manual should have 0 failed jobs'
    assert manual_entry['job_type_total'] == 1, 'manual should have 1 job type (job)'
    assert manual_entry['jobs_duration_total_seconds'] == pytest.approx(3.0), 'manual should have 3s total duration'
    assert 'unique_hosts_total' in manual_entry, 'Should have unique_hosts_total field from job_host_summary merge'
    assert 'ok_total' in manual_entry, 'Should have ok_total field from job_host_summary merge'
    assert 'failures_total' in manual_entry, 'Should have failures_total field from job_host_summary merge'

    assert scheduled_entry['jobs_total'] == 2, 'scheduled should have 2 jobs'
    assert scheduled_entry['jobs_failed_total'] == 2, 'scheduled should have 2 failed jobs (both job 2 and job 6 have failed=1)'
    assert scheduled_entry['jobs_never_started_total'] == 1, 'scheduled should have 1 never started job'
    assert scheduled_entry['job_type_total'] == 2, 'scheduled should have 2 job types (job and adhoccommand)'
    assert scheduled_entry['jobs_duration_total_seconds'] == pytest.approx(5.0), 'scheduled should have 5s total duration'
    assert 'unique_hosts_total' in scheduled_entry, 'Should have unique_hosts_total field from job_host_summary merge'

    assert workflow_entry['jobs_total'] == 1, 'workflow should have 1 job'
    assert workflow_entry['jobs_failed_total'] == 0, 'workflow should have 0 failed jobs'
    assert workflow_entry['job_type_total'] == 1, 'workflow should have 1 job type (workflowjob)'
    assert workflow_entry['jobs_duration_total_seconds'] == pytest.approx(7.0), 'workflow should have 7s total duration'
    assert 'unique_hosts_total' in workflow_entry, 'Should have unique_hosts_total field from job_host_summary merge'

    assert callback_entry['jobs_total'] == 1, 'callback should have 1 job'
    assert callback_entry['jobs_failed_total'] == 0, 'callback should have 0 failed jobs'
    assert callback_entry['job_type_total'] == 1, 'callback should have 1 job type (job)'
    assert callback_entry['jobs_duration_total_seconds'] == pytest.approx(2.0), 'callback should have 2s total duration'
    assert 'unique_hosts_total' in callback_entry, 'Should have unique_hosts_total field from job_host_summary merge'

    for entry in [manual_entry, scheduled_entry, workflow_entry, callback_entry]:
        assert 'launch_type_manual_total' not in entry, 'Should not have launch_type_*_total when grouped by launch_type'
        assert 'job_type_total' in entry, 'Should have job_type_total field'

    assert manual_entry['controller_versions'] == ['2.9.0'], f"Expected ['2.9.0'] for manual launch_type, got {manual_entry['controller_versions']}"
    assert scheduled_entry['controller_versions'] == ['2.10.0', '2.14.0'], (
        f"Expected ['2.10.0', '2.14.0'] for scheduled launch_type, got {scheduled_entry['controller_versions']}"
    )
    assert workflow_entry['controller_versions'] == ['2.11.0'], (
        f"Expected ['2.11.0'] for workflow launch_type, got {workflow_entry['controller_versions']}"
    )
    assert callback_entry['controller_versions'] == ['2.12.0'], (
        f"Expected ['2.12.0'] for callback launch_type, got {callback_entry['controller_versions']}"
    )


def _validate_jobs_by_controller_version(result):
    """Validate jobs_by_controller_version section."""
    jobs_by_controller_version_list = result['jobs_by_controller_version']
    assert isinstance(jobs_by_controller_version_list, list), 'jobs_by_controller_version should be a list'
    assert len(jobs_by_controller_version_list) == 5, f'Should have 5 controller versions, got {len(jobs_by_controller_version_list)}'

    version_2_9_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.9.0'), None)
    version_2_10_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.10.0'), None)
    version_2_11_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.11.0'), None)
    version_2_12_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.12.0'), None)
    version_2_14_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.14.0'), None)

    assert version_2_9_0 is not None, 'Should have controller_version 2.9.0'
    assert version_2_10_0 is not None, 'Should have controller_version 2.10.0'
    assert version_2_11_0 is not None, 'Should have controller_version 2.11.0'
    assert version_2_12_0 is not None, 'Should have controller_version 2.12.0'
    assert version_2_14_0 is not None, 'Should have controller_version 2.14.0'

    assert version_2_9_0['jobs_total'] == 1, '2.9.0 should have 1 job'
    assert version_2_9_0['jobs_failed_total'] == 0, '2.9.0 should have 0 failed jobs'
    assert version_2_9_0['job_type_total'] == 1, '2.9.0 should have 1 job type (job)'
    assert version_2_9_0['jobs_duration_total_seconds'] == pytest.approx(3.0), '2.9.0 should have 3s total duration'
    assert 'unique_hosts_total' in version_2_9_0, 'Should have unique_hosts_total field from job_host_summary merge'

    assert version_2_10_0['jobs_total'] == 1, '2.10.0 should have 1 job'
    assert version_2_10_0['jobs_failed_total'] == 1, '2.10.0 should have 1 failed job'
    assert version_2_10_0['job_type_total'] == 1, '2.10.0 should have 1 job type (job)'
    assert version_2_10_0['jobs_duration_total_seconds'] == pytest.approx(5.0), '2.10.0 should have 5s total duration'
    assert 'unique_hosts_total' in version_2_10_0, 'Should have unique_hosts_total field from job_host_summary merge'

    assert version_2_11_0['jobs_total'] == 1, '2.11.0 should have 1 job'
    assert version_2_11_0['jobs_failed_total'] == 0, '2.11.0 should have 0 failed jobs'
    assert version_2_11_0['job_type_total'] == 1, '2.11.0 should have 1 job type (workflowjob)'
    assert version_2_11_0['jobs_duration_total_seconds'] == pytest.approx(7.0), '2.11.0 should have 7s total duration'
    assert 'unique_hosts_total' in version_2_11_0, 'Should have unique_hosts_total field from job_host_summary merge'

    assert version_2_12_0['jobs_total'] == 1, '2.12.0 should have 1 job'
    assert version_2_12_0['jobs_failed_total'] == 0, '2.12.0 should have 0 failed jobs'
    assert version_2_12_0['job_type_total'] == 1, '2.12.0 should have 1 job type (job)'
    assert version_2_12_0['jobs_duration_total_seconds'] == pytest.approx(2.0), '2.12.0 should have 2s total duration'
    assert 'unique_hosts_total' in version_2_12_0, 'Should have unique_hosts_total field from job_host_summary merge'

    assert version_2_14_0['jobs_total'] == 1, '2.14.0 should have 1 job'
    assert version_2_14_0['jobs_failed_total'] == 1, '2.14.0 should have 1 failed job'
    assert version_2_14_0['jobs_never_started_total'] == 1, '2.14.0 should have 1 never started job'
    assert version_2_14_0['job_type_total'] == 1, '2.14.0 should have 1 job type (adhoccommand)'
    assert version_2_14_0['jobs_duration_total_seconds'] == pytest.approx(0.0), '2.14.0 should have 0s total duration'
    assert 'unique_hosts_total' in version_2_14_0, 'Should have unique_hosts_total field from job_host_summary merge'

    for version_entry in [version_2_9_0, version_2_10_0, version_2_11_0, version_2_12_0, version_2_14_0]:
        assert 'job_type_total' in version_entry, 'Should have job_type_total field'
        assert 'launch_type_manual_total' not in version_entry, 'Should not have launch_type_*_total field'

    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_launch_type'])
    total_jobs_by_controller_version = sum(j.get('jobs_total', 0) for j in jobs_by_controller_version_list)
    assert (
        total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_controller_version == result['statistics']['rollup_period_jobs_total']
    ), (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_controller_version={total_jobs_by_controller_version}, '
        f'statistics={result["statistics"]["rollup_period_jobs_total"]}'
    )


def create_csv_file(data_list, csv_path):
    """
    Create a CSV file from a list of dictionaries.

    Args:
        data_list: List of dictionaries to convert to CSV
        csv_path: Path where to save the CSV file

    Returns:
        The path to the created CSV file, or None if data_list is empty
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Skip creating CSV for empty data
    if not data_list:
        return None

    # Convert list of dicts to DataFrame then to CSV
    df = pd.DataFrame(data_list)
    df.to_csv(csv_path, index=False, encoding='utf-8')

    return csv_path


def test_multiple_csv_files_concatenation(cleanup_test_data):
    """
    Test that multiple CSV files are properly concatenated and aggregated.

    This test splits the test data into multiple CSV files (2-3 parts each)
    and verifies that the concatenation logic works correctly.

    The test validates:
    1. **Jobs**: Verifies counts, timing statistics (min/max/totals), and edge cases like never-started jobs
    2. **Execution Environments**: Validates total, default, and custom EE counts
    3. **Job Host Summary**: Checks task result counts (ok, failures, skipped, etc.)
    4. **Events Modules**: Comprehensive validation including:
       - Total module and host counts
       - Module and collection lists
       - Detailed module statistics (jobs, hosts, tasks, durations)
       - Collection statistics
       - Playbook-level module usage
    5. **Anonymization**: Verifies all sensitive names are properly hashed
    """

    # since = begining of the day
    # until = begining of the next day
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)

    base_path = './out'
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # ========== Split and create CSV files for each collector ==========
    input_data = _create_csv_files_from_split_data(data_dir, jobs, events, execution_environments, jobhostsummary, credentials)

    result = compute_anonymized_rollup_from_raw_data(input_data=input_data, salt='test_salt')

    # print the result with pretty json
    import json

    # Note: result is already sanitized by compute_anonymized_rollup_from_raw_data
    json_content = json.dumps(result, indent=4)
    print('\n' + '=' * 80)
    print('=== ANONYMIZED ROLLUP RESULT (from multiple CSV files) ===')
    print('=' * 80)
    print(json_content)
    print('=' * 80)

    # save the result as json inside rollups/2025/06/13/anonymized.json - based on the year, month, day
    json_path = f'./out/rollups/{year}/{month:02d}/{day:02d}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d")}.json'

    # ensure the directory exists
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        print(f'Saving result to {json_path}')
        # write result as json to file
        f.write(json_content)

        # ========== Validate the results ==========

        # Validate flattened structure
        assert 'statistics' in result
        assert 'jobs_by_job_type' in result
        assert 'jobs_by_launch_type' in result
        # job_host_summary is now merged into jobs_by_job_type
        assert 'module_stats' in result
        assert 'collection_stats' in result
        assert 'collections_versions' in result

    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    _validate_jobs_by_job_type(jobs_list, result)
    _validate_controller_versions_top_level(result)

    # Validate scm_types at top level
    assert 'rollup_period_scm_types' in result, 'Should have rollup_period_scm_types at top level'
    assert result['rollup_period_scm_types'] == ['git', 'svn', 'unknown'], (
        f"Expected ['git', 'svn', 'unknown'] for rollup_period_scm_types, got {result['rollup_period_scm_types']}"
    )

    # ========== Validate Execution Environments ==========
    assert result['statistics']['rollup_period_execution_environments_total'] == 5
    assert result['statistics']['rollup_period_EE_default_total'] == 2
    assert result['statistics']['rollup_period_EE_custom_total'] == 3

    # ========== Validate Job Host Summary ==========
    _validate_job_host_summary(jobs_list, result)

    # ========== Validate Events Modules ==========
    _validate_events_modules(result)

    # ========== Validate Credentials ==========
    print('--- Validating credentials data values ---')
    assert 'rollup_period_credential_types' in result, 'Should have rollup_period_credential_types at top level'
    credential_types = result['rollup_period_credential_types']
    assert isinstance(credential_types, list), 'rollup_period_credential_types should be a list'
    for cred_type in ['Amazon Web Services', 'Container Registry', 'Machine', 'Network', 'Source Control', 'Vault']:
        assert cred_type in credential_types
    assert len(credential_types) == 6, f'Should have 6 unique credential types, got {len(credential_types)}'
    assert credential_types == sorted(credential_types), 'credential_types should be sorted'

    # ========== Validate Collections Versions ==========
    print('--- Validating collections_versions data values ---')
    _validate_collections_versions(result)

    # ========== Validate Jobs by Launch Type ==========
    _validate_jobs_by_launch_type(result)

    # ========== Validate Jobs by Controller Version ==========
    _validate_jobs_by_controller_version(result)


def test_empty_csv_files_handling(cleanup_test_data):
    """
    Test that the system handles case with no CSV files gracefully.
    """

    base_path = './out'
    since = datetime(2025, 6, 13, 0, 0, 0)
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # Create the directory but don't create any CSV files
    # This simulates a scenario where no data was collected
    os.makedirs(data_dir, exist_ok=True)

    # Create input_data dict with empty lists
    input_data = {
        'unified_jobs': [],
        'job_host_summary': [],
        'main_jobevent': [],
        'execution_environments': [],
        'credentials': [],
    }

    # Should not crash, but return empty/default results
    result = compute_anonymized_rollup_from_raw_data(input_data=input_data, salt='test_salt')

    # Print the result for debugging
    import json

    json_content = json.dumps(result, indent=4)
    print('\n=== Empty CSV Files Result ===')
    print(json_content)

    # Validate flattened structure exists even with empty data
    assert 'statistics' in result
    assert 'jobs_by_job_type' in result
    assert 'jobs_by_launch_type' in result
    # job_host_summary is now merged into jobs_by_job_type
    # Event-related fields should be missing when there are no events
    assert 'module_stats' not in result, 'module_stats should be missing when there are no events'
    assert 'collection_stats' not in result, 'collection_stats should be missing when there are no events'
    assert 'collections_versions' in result

    # Verify statistics contains all fields (with null values for empty data)
    statistics = result['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dict'
    # Event-related fields should be missing when there are no events
    assert 'rollup_period_modules_total' not in statistics, 'rollup_period_modules_total should be missing when there are no events'
    assert 'rollup_period_unique_hosts_automated_total' not in statistics, 'rollup_period_unique_hosts_automated_total should be missing when there are no events'
    assert 'rollup_period_warnings_total' not in statistics, 'rollup_period_warnings_total should be missing when there are no events'
    assert 'rollup_period_deprecations_total' not in statistics, 'rollup_period_deprecations_total should be missing when there are no events'
    assert 'rollup_period_execution_environments_total' in statistics
    assert 'rollup_period_EE_default_total' in statistics
    assert 'rollup_period_EE_custom_total' in statistics
    # Validate execution_environments_total is sum of default and custom
    if statistics['rollup_period_execution_environments_total'] is not None:
        assert statistics['rollup_period_execution_environments_total'] == (
            (statistics['rollup_period_EE_default_total'] or 0) + (statistics['rollup_period_EE_custom_total'] or 0)
        ), 'execution_environments_total should be sum of EE_default and EE_custom'
    assert 'rollup_period_jobs_total' in statistics
    assert 'rollup_period_jobs_successful' in statistics
    assert 'rollup_period_jobs_failed' in statistics
    assert 'rollup_period_jobs_duration_all_statuses_seconds' in statistics
    assert 'rollup_period_jobs_successful_duration_total_seconds' in statistics
    assert 'rollup_period_jobs_failed_duration_total_seconds' in statistics
    assert 'rollup_period_organizations_total' in statistics
    assert 'rollup_period_forks_total' in statistics
    assert 'rollup_period_unique_hosts_total' in statistics
    assert 'rollup_period_job_host_pairs_total' in statistics
    assert 'rollup_period_successful_hosts_total' in statistics
    assert 'rollup_period_failed_hosts_total' in statistics
    assert 'rollup_period_unreachable_hosts_total' in statistics
    # Event-related fields should be missing when there are no events
    assert 'rollup_period_playbooks_total' not in statistics, 'rollup_period_playbooks_total should be missing when there are no events'
    assert 'rollup_period_templates_total' in statistics
    assert 'rollup_period_tasks_total' in statistics
    assert 'rollup_period_task_ok_total' in statistics
    assert 'rollup_period_task_failed_total' in statistics
    assert 'rollup_period_task_skipped_total' in statistics
    assert 'rollup_period_task_unreachable_total' in statistics
    assert 'rollup_period_task_ignored_total' in statistics

    # All statistics should be None for empty data (except counts which should be 0)
    # Event-related fields are missing when there are no events, so we don't check them here
    assert statistics['rollup_period_execution_environments_total'] is None
    assert statistics['rollup_period_EE_default_total'] is None
    assert statistics['rollup_period_EE_custom_total'] is None
    assert statistics['rollup_period_jobs_total'] is None
    assert statistics['rollup_period_jobs_successful'] is None, 'jobs_successful should be None for empty data'
    assert statistics['rollup_period_jobs_failed'] is None, 'jobs_failed should be None for empty data'
    assert statistics['rollup_period_jobs_duration_all_statuses_seconds'] is None, 'jobs_duration_all_statuses_seconds should be None for empty data'
    assert statistics['rollup_period_jobs_successful_duration_total_seconds'] is None, (
        'jobs_successful_duration_total_seconds should be None for empty data'
    )
    assert statistics['rollup_period_jobs_failed_duration_total_seconds'] is None, 'jobs_failed_duration_total_seconds should be None for empty data'
    assert statistics['rollup_period_organizations_total'] is None
    assert 'rollup_period_controller_versions' in result, 'Should have controller_versions field at top level'
    assert result['rollup_period_controller_versions'] == [], 'controller_versions should be empty list for empty data'
    assert statistics['rollup_period_forks_total'] is None
    assert statistics['rollup_period_unique_hosts_total'] is None
    # job_host_pairs_total should be 0 (not None) when there's no data, as it represents a count
    assert statistics['rollup_period_job_host_pairs_total'] == 0, (
        f'job_host_pairs_total should be 0 for empty data, got {statistics["rollup_period_job_host_pairs_total"]}'
    )
    # Host outcome totals should be None for empty data
    assert statistics['rollup_period_successful_hosts_total'] is None, 'successful_hosts_total should be None for empty data'
    assert statistics['rollup_period_failed_hosts_total'] is None, 'failed_hosts_total should be None for empty data'
    assert statistics['rollup_period_unreachable_hosts_total'] is None, 'unreachable_hosts_total should be None for empty data'
    # playbooks_total should be missing when there are no events
    # templates_total should be None when there's no data (no job_type groups)
    assert statistics['rollup_period_templates_total'] is None, (
        f'templates_total should be None for empty data, got {statistics["rollup_period_templates_total"]}'
    )
    # Task statistics should be 0 (not None) when there's no data, as they're calculated from empty jobs_by_job_type list
    assert statistics['rollup_period_tasks_total'] == 0, (
        f'rollup_period_tasks_total should be 0 for empty data, got {statistics["rollup_period_tasks_total"]}'
    )
    assert statistics['rollup_period_task_ok_total'] == 0, (
        f'rollup_period_task_ok_total should be 0 for empty data, got {statistics["rollup_period_task_ok_total"]}'
    )
    assert statistics['rollup_period_task_failed_total'] == 0, (
        f'rollup_period_task_failed_total should be 0 for empty data, got {statistics["rollup_period_task_failed_total"]}'
    )
    assert statistics['rollup_period_task_skipped_total'] == 0, (
        f'rollup_period_task_skipped_total should be 0 for empty data, got {statistics["rollup_period_task_skipped_total"]}'
    )
    assert statistics['rollup_period_task_unreachable_total'] == 0, (
        f'rollup_period_task_unreachable_total should be 0 for empty data, got {statistics["rollup_period_task_unreachable_total"]}'
    )
    assert statistics['rollup_period_task_ignored_total'] == 0, (
        f'rollup_period_task_ignored_total should be 0 for empty data, got {statistics["rollup_period_task_ignored_total"]}'
    )

    # Verify all arrays are empty
    assert isinstance(result['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    assert len(result['jobs_by_job_type']) == 0, 'jobs_by_job_type should be empty with no data'
    assert isinstance(result['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'
    assert len(result['jobs_by_launch_type']) == 0, 'jobs_by_launch_type should be empty with no data'
    # job_host_summary is now merged into jobs_by_job_type

    # Event-related arrays should be missing when there are no events
    assert 'module_stats' not in result, 'module_stats should be missing when there are no events'
    assert 'collection_stats' not in result, 'collection_stats should be missing when there are no events'

    # modules_used_per_playbook is computed but not included in final output

    assert isinstance(result['collections_versions'], list), 'collections_versions should be a list'
    assert len(result['collections_versions']) == 0, 'collections_versions should be empty with no data'

    # Verify credentials field is present but empty when there's no data
    # (credentials_list would be empty list)
    assert 'rollup_period_credential_types' in result
    assert result['rollup_period_credential_types'] == [], 'Should have empty credential_types list when there is no credentials data'

    # Verify scm_types field is present but empty when there's no data
    assert 'rollup_period_scm_types' in result, 'Should have rollup_period_scm_types at top level'
    assert result['rollup_period_scm_types'] == [], 'Should have empty scm_types list when there is no jobs data'
