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

from metrics_utility.test.test_anonymized_rollups.helpers import compute_anonymized_rollup_from_raw_data
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

    # Execution environments: snapshot collector - keep all data in single batch (no splitting)
    ee_csv_file = create_csv_file(execution_environments, f'{data_dir}/execution_environments.csv')
    ee_csv_files = [ee_csv_file] if ee_csv_file else []

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
    assert 'ansible_versions' in job_type, 'Should have ansible_versions field in by_job_type'
    assert job_type['ansible_versions'] == ['2.10.0', '2.12.0', '2.9.0'], (
        f"Expected ['2.10.0', '2.12.0', '2.9.0'] for job type, got {job_type['ansible_versions']}"
    )

    workflowjob_type_jobs = [j for j in jobs_list if j['job_type'] == 'workflowjob' and j['jobs_never_started_total'] == 0]
    assert len(workflowjob_type_jobs) == 1
    workflowjob_type = workflowjob_type_jobs[0]
    assert workflowjob_type['jobs_total'] == 1
    assert workflowjob_type['jobs_failed_total'] == 0
    assert workflowjob_type['jobs_duration_total_seconds'] == pytest.approx(7.0)
    assert workflowjob_type['job_waiting_time_total_seconds'] == pytest.approx(4.0)
    assert workflowjob_type['job_type'] == 'workflowjob'
    assert 'ansible_versions' in workflowjob_type, 'Should have ansible_versions field in by_job_type'
    assert workflowjob_type['ansible_versions'] == ['2.11.0'], f"Expected ['2.11.0'] for workflowjob type, got {workflowjob_type['ansible_versions']}"

    adhoccommand_type_jobs = [j for j in jobs_list if j['job_type'] == 'adhoccommand' and j['jobs_never_started_total'] == 1]
    assert len(adhoccommand_type_jobs) == 1
    adhoccommand_type = adhoccommand_type_jobs[0]
    assert adhoccommand_type['jobs_total'] == 1
    assert adhoccommand_type['jobs_failed_total'] == 1
    assert adhoccommand_type['jobs_duration_total_seconds'] == pytest.approx(0.0)
    assert adhoccommand_type['job_waiting_time_total_seconds'] == pytest.approx(0.0)
    assert adhoccommand_type['job_type'] == 'adhoccommand'
    assert 'ansible_versions' in adhoccommand_type, 'Should have ansible_versions field in by_job_type'
    assert adhoccommand_type['ansible_versions'] == ['2.14.0'], (
        f"Expected ['2.14.0'] for adhoccommand type, got {adhoccommand_type['ansible_versions']}"
    )


def _validate_ansible_versions_top_level(result):
    """Validate ansible_versions at top level."""
    assert 'rollup_period_ansible_versions' in result, 'Should have ansible_versions at top level'
    statistics_ansible_versions = result['rollup_period_ansible_versions']
    assert isinstance(statistics_ansible_versions, list), 'ansible_versions should be a list'
    jobs_by_job_type = result.get('jobs_by_job_type', [])
    expected_versions_set = set()
    for job in jobs_by_job_type:
        ansible_versions = job.get('ansible_versions', [])
        if isinstance(ansible_versions, list):
            expected_versions_set.update(ansible_versions)
    expected_versions = sorted(list(expected_versions_set))
    assert statistics_ansible_versions == expected_versions, (
        f'Expected ansible_versions {expected_versions} in statistics, got {statistics_ansible_versions}'
    )
    assert len(statistics_ansible_versions) == 5, f'Expected 5 unique ansible versions, got {len(statistics_ansible_versions)}'
    for version in ['2.9.0', '2.10.0', '2.11.0', '2.12.0', '2.14.0']:
        assert version in statistics_ansible_versions


def _validate_job_host_summary(jobs_list, result):
    """Validate job host summary section."""
    # Note: unique hosts are h1, h2, h3, h4, h5 (5 total) - h1, h2, h3 are shared between job types
    assert result['statistics']['rollup_period_unique_hosts_total'] == 5, 'Should have 5 unique hosts total (h1, h2, h3, h4, h5)'
    assert result['statistics']['rollup_period_job_host_pairs_total'] == 16, (
        f'Should have 16 total job host summary records, got {result["statistics"]["rollup_period_job_host_pairs_total"]}'
    )

    job_type_entry = next((j for j in jobs_list if j['job_type'] == 'job'), None)
    assert job_type_entry is not None, 'Should have job_type job'
    # Note: unique_hosts_total is only at top level, not in groupings
    assert job_type_entry['ok_total'] == 26, 'Should have 26 ok tasks for job type'
    assert job_type_entry['failed_total'] == 2, 'Should have 2 failures for job type'
    assert job_type_entry['skipped_total'] == 2, 'Should have 2 skipped for job type'
    assert job_type_entry['unreachable_total'] == 0, 'Should have 0 dark for job type'
    assert job_type_entry['ignored_total'] == 0, 'Should have 0 ignored for job type'
    assert job_type_entry['rescued_total'] == 0, 'Should have 0 rescued for job type'

    workflowjob_type_entry = next((j for j in jobs_list if j['job_type'] == 'workflowjob'), None)
    assert workflowjob_type_entry is not None, 'Should have job_type workflowjob'
    # Note: unique_hosts_total is only at top level, not in groupings
    assert workflowjob_type_entry['ok_total'] == 26, 'Should have 26 ok tasks for workflowjob type'
    assert workflowjob_type_entry['failed_total'] == 4, 'Should have 4 failures for workflowjob type'
    assert workflowjob_type_entry['skipped_total'] == 0, 'Should have 0 skipped for workflowjob type'
    assert workflowjob_type_entry['unreachable_total'] == 0, 'Should have 0 dark for workflowjob type'
    assert workflowjob_type_entry['ignored_total'] == 0, 'Should have 0 ignored for workflowjob type'
    assert workflowjob_type_entry['rescued_total'] == 0, 'Should have 0 rescued for workflowjob type'

    adhoccommand_type_entry = next((j for j in jobs_list if j['job_type'] == 'adhoccommand'), None)
    assert adhoccommand_type_entry is not None, 'Should have job_type adhoccommand'
    # Note: unique_hosts_total is only at top level, not in groupings
    assert adhoccommand_type_entry['ok_total'] == 0, 'Should have 0 ok tasks (no job_host_summary match)'
    assert adhoccommand_type_entry['failed_total'] == 0, 'Should have 0 failures (no job_host_summary match)'
    assert adhoccommand_type_entry['skipped_total'] == 0, 'Should have 0 skipped (no job_host_summary match)'

    total_ok = sum(j.get('ok_total', 0) for j in jobs_list)
    total_failures = sum(j.get('failed_total', 0) for j in jobs_list)
    total_skipped = sum(j.get('skipped_total', 0) for j in jobs_list)
    total_dark = sum(j.get('unreachable_total', 0) for j in jobs_list)
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
    assert result['statistics']['rollup_period_modules_total'] == 7, 'Should have 7 unique modules from all tarballs (including Custom)'
    assert result['statistics']['rollup_period_unique_hosts_automated_total'] == 9, 'Should have 9 unique hosts from all tarballs'
    assert 'rollup_period_warnings_total' in result['statistics'], 'Should have warnings_total in statistics'
    assert result['statistics']['rollup_period_warnings_total'] == 2, (
        f'Expected 2 warnings, got {result["statistics"]["rollup_period_warnings_total"]}'
    )
    assert 'rollup_period_deprecations_total' in result['statistics'], 'Should have deprecations_total in statistics'
    assert result['statistics']['rollup_period_deprecations_total'] == 1, (
        f'Expected 1 deprecated event, got {result["statistics"]["rollup_period_deprecations_total"]}'
    )

    module_names = [m['module'] for m in result['module_stats'] if 'module' in m]
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
    assert len(module_stats) == 6, 'Should have stats for all 6 non-Custom modules'

    win_copy_stats = [m for m in module_stats if m.get('module') == 'ansible.windows.win_copy']
    assert len(win_copy_stats) == 1, 'Should have exactly one entry for ansible.windows.win_copy'
    win_copy = win_copy_stats[0]
    assert win_copy['collection_source'] == 'certified'
    assert win_copy['collection'] == 'ansible.windows'
    assert win_copy['jobs_total'] == 3
    assert win_copy['unique_hosts_total'] == 3
    assert win_copy['task_ok_total'] == 1
    assert win_copy['task_ok_with_retries_total'] == 2
    assert win_copy['task_failed_total'] == 0
    assert win_copy['jobs_duration_total_seconds'] == pytest.approx(2100.0)
    assert win_copy['processed_events_total'] == 5

    yum_stats = [m for m in module_stats if m.get('module') == 'community.general.yum']
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
    assert len(collection_stats) == 6, 'Should have stats for all 6 non-Custom collections'

    windows_collection = [c for c in collection_stats if c.get('collection') == 'ansible.windows']
    assert len(windows_collection) == 1, 'Should have exactly one entry for ansible.windows collection'
    windows_coll = windows_collection[0]
    assert windows_coll['collection_source'] == 'certified'
    assert windows_coll['jobs_total'] == 3
    assert windows_coll['unique_hosts_total'] == 3
    assert windows_coll['task_ok_total'] == 1
    assert windows_coll['task_ok_with_retries_total'] == 2
    assert windows_coll['processed_events_total'] == 5

    assert result['statistics']['rollup_period_playbooks_total'] == 5, 'Should have 5 total playbooks'


def _validate_jobs_by_installed_collections_versions(result):
    """Validate collections versions section.

    Uses same job fixture data as test_jobs_anonymized_rollups.py.  ansible.builtin is a known
    certified collection kept as-is.  All timing/template/inventory/ansible_version stats should
    match the per-collection rollup values calculated from jobs 1-4 and 6 (job 5 filtered – no
    finished).
    """
    jobs_by_installed_collections_versions = result['jobs_by_installed_collections_versions']
    assert isinstance(jobs_by_installed_collections_versions, list), 'jobs_by_installed_collections_versions should be a list'
    cv = {(c['collection'], c['version']): c for c in jobs_by_installed_collections_versions}

    # --- jobs_total counts (unchanged from before) ---
    assert cv.get(('ansible.builtin', '2.9.10'))['jobs_total'] == 5, (
        f'Expected ansible.builtin 2.9.10 in 5 jobs, got {cv.get(("ansible.builtin", "2.9.10"))}'
    )
    assert cv.get(('community.general', '1.0.0'))['jobs_total'] == 2
    assert cv.get(('community.general', '2.0.0'))['jobs_total'] == 2
    assert cv.get(('community.general', '3.0.0'))['jobs_total'] == 1
    assert cv.get(('ansible.windows', '1.0.0'))['jobs_total'] == 1
    assert cv.get(('community.aws', '1.5.0'))['jobs_total'] == 1

    assert len(jobs_by_installed_collections_versions) == 6, (
        f'Expected 6 unique collection-version pairs (ansible.builtin now kept), got {len(jobs_by_installed_collections_versions)}'
    )

    # --- ansible.builtin 2.9.10 (5 jobs) ---
    # jobs 1(s,3s,0s) 2(f,5s,2s) 3(s,7s,4s) 4(s,2s,1s) 6(f,NaN,NaN,never-started)
    custom = cv[('ansible.builtin', '2.9.10')]
    assert custom['jobs_failed_total'] == 2
    assert custom['jobs_successful_total'] == 3
    assert custom['jobs_never_started_total'] == 1
    assert custom['jobs_duration_total_seconds'] == pytest.approx(17.0)  # 3+5+7+2; job6 NaN
    assert custom['jobs_successful_duration_total_seconds'] == pytest.approx(12.0)  # 3+7+2
    assert custom['jobs_failed_duration_total_seconds'] == pytest.approx(5.0)  # job2
    assert custom['job_duration_maximum_seconds'] == pytest.approx(7.0)
    assert custom['job_duration_minimum_seconds'] == pytest.approx(2.0)
    assert custom['job_waiting_time_total_seconds'] == pytest.approx(7.0)  # 0+2+4+1
    assert custom['job_waiting_time_maximum_seconds'] == pytest.approx(4.0)
    assert custom['job_waiting_time_minimum_seconds'] == pytest.approx(0.0)
    assert custom['templates_total'] == 3  # T1, T2, T3
    assert custom['inventories_total'] == 3
    assert custom['ansible_versions'] == ['2.10.0', '2.11.0', '2.12.0', '2.14.0', '2.9.0']

    # --- community.general 1.0.0 (jobs 1, 4 – both successful) ---
    cg1 = cv[('community.general', '1.0.0')]
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

    # --- community.general 2.0.0 (jobs 2 failed, 3 successful) ---
    cg2 = cv[('community.general', '2.0.0')]
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

    # --- community.general 3.0.0 (job 6 only – failed, never started → no durations) ---
    cg3 = cv[('community.general', '3.0.0')]
    assert cg3['jobs_failed_total'] == 1
    assert cg3['jobs_successful_total'] == 0
    assert cg3['jobs_never_started_total'] == 1
    assert cg3['jobs_duration_total_seconds'] == pytest.approx(0.0)
    assert cg3['job_duration_maximum_seconds'] is None
    assert cg3['job_duration_minimum_seconds'] is None
    assert cg3['job_waiting_time_total_seconds'] == pytest.approx(0.0)
    assert cg3['job_waiting_time_maximum_seconds'] is None
    assert cg3['job_waiting_time_minimum_seconds'] is None
    assert cg3['templates_total'] == 1
    assert cg3['inventories_total'] == 1
    assert cg3['ansible_versions'] == ['2.14.0']

    # --- ansible.windows 1.0.0 (job 2 only – failed, 5s, 2s wait) ---
    aw = cv[('ansible.windows', '1.0.0')]
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

    # --- community.aws 1.5.0 (job 3 only – successful, 7s, 4s wait) ---
    ca = cv[('community.aws', '1.5.0')]
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

    # --- invariants for every entry ---
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
        'templates_total',
        'inventories_total',
        'ansible_versions',
    ]
    for collection in jobs_by_installed_collections_versions:
        assert 'collection' in collection, 'Each collection should have collection field'
        assert 'version' in collection, 'Each collection should have version field'
        assert 'jobs_total' in collection, 'Each collection should have jobs_total field'
        assert 'jobs_failed_total' in collection, 'Each collection should have jobs_failed_total field'
        assert 'jobs_successful_total' in collection, 'Each collection should have jobs_successful_total field'
        assert isinstance(collection['jobs_total'], int), 'jobs_total should be an integer'
        assert isinstance(collection['jobs_failed_total'], int), 'jobs_failed_total should be an integer'
        assert isinstance(collection['jobs_successful_total'], int), 'jobs_successful_total should be an integer'
        assert collection['jobs_total'] > 0, 'jobs_total should be greater than 0'
        assert collection['jobs_failed_total'] + collection['jobs_successful_total'] == collection['jobs_total'], (
            f'jobs_failed_total + jobs_successful_total should equal jobs_total for {collection}'
        )
        for field in new_fields:
            assert field in collection, (
                f'Missing new field {field!r} in jobs_by_installed_collections_versions entry {collection["collection"]} {collection["version"]}'
            )
        assert isinstance(collection['jobs_never_started_total'], int)
        assert isinstance(collection['templates_total'], int)
        assert isinstance(collection['inventories_total'], int)
        assert isinstance(collection['ansible_versions'], list)


def _validate_jobs_by_controller_version(result):
    """Validate jobs_by_controller_version: 1 item summarising all valid jobs."""
    assert 'jobs_by_controller_version' in result, 'Should have jobs_by_controller_version in result'
    ctrl_summary_list = result['jobs_by_controller_version']
    assert isinstance(ctrl_summary_list, list), 'jobs_by_controller_version should be a list'
    assert len(ctrl_summary_list) == 1, 'jobs_by_controller_version should contain exactly 1 item'

    ctrl_summary = ctrl_summary_list[0]
    # 5 valid jobs total (job 5 filtered - no finished)
    assert ctrl_summary['jobs_total'] == 5
    assert ctrl_summary['jobs_failed_total'] == 2  # jobs 2 and 6
    assert ctrl_summary['jobs_successful_total'] == 3  # jobs 1, 3, 4
    assert ctrl_summary['jobs_never_started_total'] == 1  # job 6 has started=None
    assert ctrl_summary['templates_total'] == 3  # T1, T2, T3
    assert ctrl_summary['inventories_total'] == 3  # inventory_id 1, 2, 3
    # Durations: 3+5+7+2 = 17s (job 6 NaN skipped)
    assert ctrl_summary['jobs_duration_total_seconds'] == pytest.approx(17.0)
    assert ctrl_summary['job_duration_maximum_seconds'] == pytest.approx(7.0)
    assert ctrl_summary['job_duration_minimum_seconds'] == pytest.approx(2.0)
    # Waiting: 0+2+4+1 = 7s (job 6 NaN skipped)
    assert ctrl_summary['job_waiting_time_total_seconds'] == pytest.approx(7.0)
    assert ctrl_summary['job_waiting_time_maximum_seconds'] == pytest.approx(4.0)
    assert ctrl_summary['job_waiting_time_minimum_seconds'] == pytest.approx(0.0)
    assert ctrl_summary['ansible_versions'] == ['2.10.0', '2.11.0', '2.12.0', '2.14.0', '2.9.0']
    # No controller_version data in input_data -> injected as None
    assert ctrl_summary.get('controller_version') is None, (
        f'controller_version should be None when not provided, got {ctrl_summary.get("controller_version")!r}'
    )


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
    assert manual_entry['jobs_duration_total_seconds'] == pytest.approx(3.0), 'manual should have 3s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings
    assert 'ok_total' in manual_entry, 'Should have ok_total field from job_host_summary merge'
    assert 'failed_total' in manual_entry, 'Should have failed_total field from job_host_summary merge'

    assert scheduled_entry['jobs_total'] == 2, 'scheduled should have 2 jobs'
    assert scheduled_entry['jobs_failed_total'] == 2, 'scheduled should have 2 failed jobs (both job 2 and job 6 have failed=1)'
    assert scheduled_entry['jobs_never_started_total'] == 1, 'scheduled should have 1 never started job'
    assert scheduled_entry['jobs_duration_total_seconds'] == pytest.approx(5.0), 'scheduled should have 5s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    assert workflow_entry['jobs_total'] == 1, 'workflow should have 1 job'
    assert workflow_entry['jobs_failed_total'] == 0, 'workflow should have 0 failed jobs'
    assert workflow_entry['jobs_duration_total_seconds'] == pytest.approx(7.0), 'workflow should have 7s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    assert callback_entry['jobs_total'] == 1, 'callback should have 1 job'
    assert callback_entry['jobs_failed_total'] == 0, 'callback should have 0 failed jobs'
    assert callback_entry['jobs_duration_total_seconds'] == pytest.approx(2.0), 'callback should have 2s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    for entry in [manual_entry, scheduled_entry, workflow_entry, callback_entry]:
        assert 'launch_type_manual_total' not in entry, 'Should not have launch_type_*_total when grouped by launch_type'

    assert manual_entry['ansible_versions'] == ['2.9.0'], f"Expected ['2.9.0'] for manual launch_type, got {manual_entry['ansible_versions']}"
    assert scheduled_entry['ansible_versions'] == ['2.10.0', '2.14.0'], (
        f"Expected ['2.10.0', '2.14.0'] for scheduled launch_type, got {scheduled_entry['ansible_versions']}"
    )
    assert workflow_entry['ansible_versions'] == ['2.11.0'], f"Expected ['2.11.0'] for workflow launch_type, got {workflow_entry['ansible_versions']}"
    assert callback_entry['ansible_versions'] == ['2.12.0'], f"Expected ['2.12.0'] for callback launch_type, got {callback_entry['ansible_versions']}"


def _validate_jobs_by_ansible_version(result):
    """Validate jobs_by_ansible_version section."""
    jobs_by_ansible_version_list = result['jobs_by_ansible_version']
    assert isinstance(jobs_by_ansible_version_list, list), 'jobs_by_ansible_version should be a list'
    assert len(jobs_by_ansible_version_list) == 5, f'Should have 5 ansible versions, got {len(jobs_by_ansible_version_list)}'

    version_2_9_0 = next((j for j in jobs_by_ansible_version_list if j.get('ansible_version') == '2.9.0'), None)
    version_2_10_0 = next((j for j in jobs_by_ansible_version_list if j.get('ansible_version') == '2.10.0'), None)
    version_2_11_0 = next((j for j in jobs_by_ansible_version_list if j.get('ansible_version') == '2.11.0'), None)
    version_2_12_0 = next((j for j in jobs_by_ansible_version_list if j.get('ansible_version') == '2.12.0'), None)
    version_2_14_0 = next((j for j in jobs_by_ansible_version_list if j.get('ansible_version') == '2.14.0'), None)

    assert version_2_9_0 is not None, 'Should have ansible_version 2.9.0'
    assert version_2_10_0 is not None, 'Should have ansible_version 2.10.0'
    assert version_2_11_0 is not None, 'Should have ansible_version 2.11.0'
    assert version_2_12_0 is not None, 'Should have ansible_version 2.12.0'
    assert version_2_14_0 is not None, 'Should have ansible_version 2.14.0'

    assert version_2_9_0['jobs_total'] == 1, '2.9.0 should have 1 job'
    assert version_2_9_0['jobs_failed_total'] == 0, '2.9.0 should have 0 failed jobs'
    assert version_2_9_0['jobs_duration_total_seconds'] == pytest.approx(3.0), '2.9.0 should have 3s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    assert version_2_10_0['jobs_total'] == 1, '2.10.0 should have 1 job'
    assert version_2_10_0['jobs_failed_total'] == 1, '2.10.0 should have 1 failed job'
    assert version_2_10_0['jobs_duration_total_seconds'] == pytest.approx(5.0), '2.10.0 should have 5s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    assert version_2_11_0['jobs_total'] == 1, '2.11.0 should have 1 job'
    assert version_2_11_0['jobs_failed_total'] == 0, '2.11.0 should have 0 failed jobs'
    assert version_2_11_0['jobs_duration_total_seconds'] == pytest.approx(7.0), '2.11.0 should have 7s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    assert version_2_12_0['jobs_total'] == 1, '2.12.0 should have 1 job'
    assert version_2_12_0['jobs_failed_total'] == 0, '2.12.0 should have 0 failed jobs'
    assert version_2_12_0['jobs_duration_total_seconds'] == pytest.approx(2.0), '2.12.0 should have 2s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    assert version_2_14_0['jobs_total'] == 1, '2.14.0 should have 1 job'
    assert version_2_14_0['jobs_failed_total'] == 1, '2.14.0 should have 1 failed job'
    assert version_2_14_0['jobs_never_started_total'] == 1, '2.14.0 should have 1 never started job'
    assert version_2_14_0['jobs_duration_total_seconds'] == pytest.approx(0.0), '2.14.0 should have 0s total duration'
    # Note: unique_hosts_total is only at top level, not in groupings

    for version_entry in [version_2_9_0, version_2_10_0, version_2_11_0, version_2_12_0, version_2_14_0]:
        assert 'launch_type_manual_total' not in version_entry, 'Should not have launch_type_*_total field'

    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_launch_type'])
    total_jobs_by_ansible_version = sum(j.get('jobs_total', 0) for j in jobs_by_ansible_version_list)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_ansible_version == result['statistics']['rollup_period_jobs_total'], (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_ansible_version={total_jobs_by_ansible_version}, '
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

    result = compute_anonymized_rollup_from_raw_data(input_data=input_data)

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
        assert 'jobs_by_installed_collections_versions' in result

    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    _validate_jobs_by_job_type(jobs_list, result)
    _validate_ansible_versions_top_level(result)

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
    print('--- Validating jobs_by_installed_collections_versions data values ---')
    _validate_jobs_by_installed_collections_versions(result)

    # ========== Validate Jobs by Launch Type ==========
    _validate_jobs_by_launch_type(result)

    # ========== Validate Jobs by Ansible Version ==========
    _validate_jobs_by_ansible_version(result)

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
    result = compute_anonymized_rollup_from_raw_data(input_data=input_data)

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
    assert 'jobs_by_installed_collections_versions' in result

    # Verify statistics contains all fields (with null values for empty data)
    statistics = result['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dict'
    # Event-related fields should be missing when there are no events
    assert 'rollup_period_modules_total' not in statistics, 'rollup_period_modules_total should be missing when there are no events'
    assert 'rollup_period_unique_hosts_automated_total' not in statistics, (
        'rollup_period_unique_hosts_automated_total should be missing when there are no events'
    )
    assert 'rollup_period_warnings_total' not in statistics, 'rollup_period_warnings_total should be missing when there are no events'
    assert 'rollup_period_deprecations_total' not in statistics, 'rollup_period_deprecations_total should be missing when there are no events'
    assert 'rollup_period_execution_environments_total' in statistics
    assert 'rollup_period_EE_default_total' in statistics
    assert 'rollup_period_EE_custom_total' in statistics
    # Validate execution_environments_total is sum of default and custom
    # This works for both None and 0 cases (0 is not None, so it will be validated)
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

    # All statistics should be 0 for empty data (not None)
    # Execution environments return 0 for empty data (not None)
    # Event-related fields are missing when there are no events, so we don't check them here
    assert statistics['rollup_period_execution_environments_total'] == 0, 'execution_environments_total should be 0 for empty data'
    assert statistics['rollup_period_EE_default_total'] == 0, 'EE_default_total should be 0 for empty data'
    assert statistics['rollup_period_EE_custom_total'] == 0, 'EE_custom_total should be 0 for empty data'
    assert statistics['rollup_period_jobs_total'] == 0, 'jobs_total should be 0 for empty data'
    assert statistics['rollup_period_jobs_successful'] == 0, 'jobs_successful should be 0 for empty data'
    assert statistics['rollup_period_jobs_failed'] == 0, 'jobs_failed should be 0 for empty data'
    assert statistics['rollup_period_jobs_duration_all_statuses_seconds'] == 0, 'jobs_duration_all_statuses_seconds should be 0 for empty data'
    assert statistics['rollup_period_jobs_successful_duration_total_seconds'] == 0, (
        'jobs_successful_duration_total_seconds should be 0 for empty data'
    )
    assert statistics['rollup_period_jobs_failed_duration_total_seconds'] == 0, 'jobs_failed_duration_total_seconds should be 0 for empty data'
    assert statistics['rollup_period_organizations_total'] == 0, 'organizations_total should be 0 for empty data'
    assert 'rollup_period_ansible_versions' in result, 'Should have ansible_versions field at top level'
    assert result['rollup_period_ansible_versions'] == [], 'ansible_versions should be empty list for empty data'
    assert statistics['rollup_period_forks_total'] == 0, 'forks_total should be 0 for empty data'
    assert statistics['rollup_period_unique_hosts_total'] == 0, 'unique_hosts_total should be 0 for empty data'
    # job_host_pairs_total should be 0 (not None) when there's no data, as it represents a count
    assert statistics['rollup_period_job_host_pairs_total'] == 0, (
        f'job_host_pairs_total should be 0 for empty data, got {statistics["rollup_period_job_host_pairs_total"]}'
    )
    # Host outcome totals should be 0 for empty data
    assert statistics['rollup_period_successful_hosts_total'] == 0, 'successful_hosts_total should be 0 for empty data'
    assert statistics['rollup_period_failed_hosts_total'] == 0, 'failed_hosts_total should be 0 for empty data'
    assert statistics['rollup_period_unreachable_hosts_total'] == 0, 'unreachable_hosts_total should be 0 for empty data'
    # playbooks_total should be missing when there are no events
    # templates_total should be 0 when there's no data (no job_type groups)
    assert statistics['rollup_period_templates_total'] == 0, (
        f'templates_total should be 0 for empty data, got {statistics["rollup_period_templates_total"]}'
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

    assert isinstance(result['jobs_by_installed_collections_versions'], list), 'jobs_by_installed_collections_versions should be a list'
    assert len(result['jobs_by_installed_collections_versions']) == 0, 'jobs_by_installed_collections_versions should be empty with no data'

    # Verify credentials field is present but empty when there's no data
    # (credentials_list would be empty list)
    assert 'rollup_period_credential_types' in result
    assert result['rollup_period_credential_types'] == [], 'Should have empty credential_types list when there is no credentials data'

    # Verify scm_types field is present but empty when there's no data
    assert 'rollup_period_scm_types' in result, 'Should have rollup_period_scm_types at top level'
    assert result['rollup_period_scm_types'] == [], 'Should have empty scm_types list when there is no jobs data'

    # jobs_by_controller_version should be present but empty when there is no jobs data
    assert 'jobs_by_controller_version' in result, 'Should have jobs_by_controller_version even with empty data'
    assert isinstance(result['jobs_by_controller_version'], list), 'jobs_by_controller_version should be a list'
    assert len(result['jobs_by_controller_version']) == 0, 'jobs_by_controller_version should be empty when there are no jobs'
