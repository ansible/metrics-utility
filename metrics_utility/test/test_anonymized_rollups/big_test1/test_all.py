"""
Test that combines all jobs (job1-job8) and validates anonymized rollups.

This test:
1. Combines all jobs, events, and job host summaries from job1 through job8
2. Splits data into multiple CSV files to test batch processing
3. Calls anonymized rollup computation and validates the results

The test validates:
- Total job counts across all job types, launch types, and ansible versions
- Host summary aggregations (ok, failures, dark, skipped, etc.)
- Module statistics
- Collection statistics
- Playbook statistics
- All data is properly anonymized
"""

import os
import shutil

from datetime import datetime

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data

# Import all job data from job1 through job8
from metrics_utility.test.test_anonymized_rollups.big_test1.credentials import credentials
from metrics_utility.test.test_anonymized_rollups.big_test1.execution_environments import execution_environments
from metrics_utility.test.test_anonymized_rollups.big_test1.job1 import events as events1
from metrics_utility.test.test_anonymized_rollups.big_test1.job1 import jobhostsummary as jhs1
from metrics_utility.test.test_anonymized_rollups.big_test1.job1 import jobs as jobs1
from metrics_utility.test.test_anonymized_rollups.big_test1.job2 import events as events2
from metrics_utility.test.test_anonymized_rollups.big_test1.job2 import jobhostsummary as jhs2
from metrics_utility.test.test_anonymized_rollups.big_test1.job2 import jobs as jobs2
from metrics_utility.test.test_anonymized_rollups.big_test1.job3 import events as events3
from metrics_utility.test.test_anonymized_rollups.big_test1.job3 import jobhostsummary as jhs3
from metrics_utility.test.test_anonymized_rollups.big_test1.job3 import jobs as jobs3
from metrics_utility.test.test_anonymized_rollups.big_test1.job4 import events as events4
from metrics_utility.test.test_anonymized_rollups.big_test1.job4 import jobhostsummary as jhs4
from metrics_utility.test.test_anonymized_rollups.big_test1.job4 import jobs as jobs4
from metrics_utility.test.test_anonymized_rollups.big_test1.job5 import events as events5
from metrics_utility.test.test_anonymized_rollups.big_test1.job5 import jobhostsummary as jhs5
from metrics_utility.test.test_anonymized_rollups.big_test1.job5 import jobs as jobs5
from metrics_utility.test.test_anonymized_rollups.big_test1.job6 import events as events6
from metrics_utility.test.test_anonymized_rollups.big_test1.job6 import jobhostsummary as jhs6
from metrics_utility.test.test_anonymized_rollups.big_test1.job6 import jobs as jobs6
from metrics_utility.test.test_anonymized_rollups.big_test1.job7 import events as events7
from metrics_utility.test.test_anonymized_rollups.big_test1.job7 import jobhostsummary as jhs7
from metrics_utility.test.test_anonymized_rollups.big_test1.job7 import jobs as jobs7
from metrics_utility.test.test_anonymized_rollups.big_test1.job8 import events as events8
from metrics_utility.test.test_anonymized_rollups.big_test1.job8 import jobhostsummary as jhs8
from metrics_utility.test.test_anonymized_rollups.big_test1.job8 import jobs as jobs8


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


def test_all_jobs_combined(cleanup_test_data):
    """
    Test that combines all jobs (job1-job8) and validates anonymized rollups.

    Expected data summary:
    - Total jobs: 8
    - Job types: 'job' (7 jobs), 'workflowjob' (1 job)
    - Launch types: 'manual' (5 jobs), 'scheduled' (1 job), 'workflow' (1 job), 'callback' (1 job)
    - Ansible versions: '2.15.0' (3 jobs), '2.16.0' (2 jobs), '2.17.0' (1 job), '2.18.0' (1 job), '2.19.0' (1 job)
    """
    # since = beginning of the day
    # until = beginning of the next day
    since = datetime(2024, 1, 15, 0, 0, 0)
    until = datetime(2024, 1, 16, 0, 0, 0)

    base_path = './out'
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # ========== Combine all job data ==========
    all_jobs = jobs1 + jobs2 + jobs3 + jobs4 + jobs5 + jobs6 + jobs7 + jobs8
    all_events = events1 + events2 + events3 + events4 + events5 + events6 + events7 + events8
    all_jobhostsummary = jhs1 + jhs2 + jhs3 + jhs4 + jhs5 + jhs6 + jhs7 + jhs8

    # ========== Split and create CSV files for each collector ==========

    # 1. Jobs data - split into 3 CSV files
    # 8 jobs total: part1: 3 jobs, part2: 3 jobs, part3: 2 jobs
    jobs_part1 = all_jobs[:3]
    jobs_part2 = all_jobs[3:6]
    jobs_part3 = all_jobs[6:]

    jobs_csv_files = []
    csv1 = create_csv_file(jobs_part1, f'{data_dir}/part1_unified_jobs.csv')
    if csv1:
        jobs_csv_files.append(csv1)
    csv2 = create_csv_file(jobs_part2, f'{data_dir}/part2_unified_jobs.csv')
    if csv2:
        jobs_csv_files.append(csv2)
    csv3 = create_csv_file(jobs_part3, f'{data_dir}/part3_unified_jobs.csv')
    if csv3:
        jobs_csv_files.append(csv3)

    # 2. Events data - split into 4 CSV files
    # Split events into roughly equal parts for batch processing testing
    total_events = len(all_events)
    events_per_part = total_events // 4
    events_part1 = all_events[:events_per_part]
    events_part2 = all_events[events_per_part : 2 * events_per_part]
    events_part3 = all_events[2 * events_per_part : 3 * events_per_part]
    events_part4 = all_events[3 * events_per_part :]

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
    csv4 = create_csv_file(events_part4, f'{data_dir}/part4_main_jobevent.csv')
    if csv4:
        events_csv_files.append(csv4)

    # 3. Job host summary - split into 3 CSV files
    # Split jobhostsummary into roughly equal parts
    total_jhs = len(all_jobhostsummary)
    jhs_per_part = total_jhs // 3
    jhs_part1 = all_jobhostsummary[:jhs_per_part]
    jhs_part2 = all_jobhostsummary[jhs_per_part : 2 * jhs_per_part]
    jhs_part3 = all_jobhostsummary[2 * jhs_per_part :]

    jhs_csv_files = []
    csv1 = create_csv_file(jhs_part1, f'{data_dir}/part1_job_host_summary.csv')
    if csv1:
        jhs_csv_files.append(csv1)
    csv2 = create_csv_file(jhs_part2, f'{data_dir}/part2_job_host_summary.csv')
    if csv2:
        jhs_csv_files.append(csv2)
    csv3 = create_csv_file(jhs_part3, f'{data_dir}/part3_job_host_summary.csv')
    if csv3:
        jhs_csv_files.append(csv3)

    # 4. Execution environments - split into 2 CSV files
    ee_part1 = execution_environments[:4]
    ee_part2 = execution_environments[4:]

    ee_csv_files = []
    csv1 = create_csv_file(ee_part1, f'{data_dir}/part1_execution_environments.csv')
    if csv1:
        ee_csv_files.append(csv1)
    csv2 = create_csv_file(ee_part2, f'{data_dir}/part2_execution_environments.csv')
    if csv2:
        ee_csv_files.append(csv2)

    # 5. Credentials - split into 2 CSV files
    cred_part1 = credentials[:8]  # First 8 entries
    cred_part2 = credentials[8:]  # Remaining 8 entries

    cred_csv_files = []
    csv1 = create_csv_file(cred_part1, f'{data_dir}/part1_credentials.csv')
    if csv1:
        cred_csv_files.append(csv1)
    csv2 = create_csv_file(cred_part2, f'{data_dir}/part2_credentials.csv')
    if csv2:
        cred_csv_files.append(csv2)

    # ========== Run the anonymized rollup computation ==========

    # Create input_data dict with lists of CSV file paths
    input_data = {
        'unified_jobs': jobs_csv_files,
        'job_host_summary': jhs_csv_files,
        'main_jobevent': events_csv_files,
        'execution_environments': ee_csv_files,
        'credentials': cred_csv_files,
    }

    result = compute_anonymized_rollup_from_raw_data(
        input_data=input_data, salt='test_salt', since=since, until=until, base_path=base_path, save_rollups=False
    )

    # Print the result with pretty json
    import json

    json_content = json.dumps(result, indent=4)
    print('\n' + '=' * 80)
    print('=== ANONYMIZED ROLLUP RESULT (all jobs combined) ===')
    print('=' * 80)
    print(json_content)
    print('=' * 80)

    # Save the result as json inside rollups/2024/01/15/anonymized.json
    json_path = f'./out/rollups/{year}/{month:02d}/{day:02d}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d")}.json'

    # Ensure the directory exists
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        print(f'Saving result to {json_path}')
        f.write(json_content)

    # ========== Validate the results ==========

    # Validate flattened structure
    assert 'statistics' in result
    assert 'jobs_by_job_type' in result
    assert 'jobs_by_launch_type' in result
    assert 'jobs_by_controller_version' in result
    assert 'module_stats' in result
    assert 'collection_name_stats' in result
    assert 'collections_versions' in result

    # ========== Validate Task Statistics (from job host summary data) ==========
    statistics = result['statistics']
    assert 'rollup_period_tasks_total' in statistics
    assert 'rollup_period_task_ok_total' in statistics
    assert 'rollup_period_task_failed_total' in statistics
    assert 'rollup_period_task_skipped_total' in statistics
    assert 'rollup_period_task_unreachable_total' in statistics
    assert 'rollup_period_task_ignored_total' in statistics

    # Expected totals calculated from all job host summary data:
    # Job 1: ok=11, failures=1, dark=0, skipped=0, ignored=0
    # Job 2: ok=11, failures=0, dark=1, skipped=0, ignored=0
    # Job 3: ok=14, failures=1, dark=1, skipped=0, ignored=0
    # Job 4: ok=10, failures=1, dark=1, skipped=0, ignored=0
    # Job 5: ok=12, failures=0, dark=0, skipped=0, ignored=0
    # Job 6: ok=14, failures=1, dark=1, skipped=0, ignored=0
    # Job 7: ok=14, failures=1, dark=1, skipped=0, ignored=0
    # Job 8: ok=12, failures=0, dark=0, skipped=0, ignored=0
    # Totals: ok=98, failures=5, dark=5, skipped=0, ignored=0, tasks_total=108
    assert statistics['rollup_period_task_ok_total'] == 98, f'Should have 98 ok tasks total, got {statistics["rollup_period_task_ok_total"]}'
    assert statistics['rollup_period_task_failed_total'] == 5, (
        f'Should have 5 failed tasks total, got {statistics["rollup_period_task_failed_total"]}'
    )
    assert statistics['rollup_period_task_unreachable_total'] == 5, (
        f'Should have 5 unreachable tasks total, got {statistics["rollup_period_task_unreachable_total"]}'
    )
    assert statistics['rollup_period_task_skipped_total'] == 0, (
        f'Should have 0 skipped tasks total, got {statistics["rollup_period_task_skipped_total"]}'
    )
    assert statistics['rollup_period_task_ignored_total'] == 0, (
        f'Should have 0 ignored tasks total, got {statistics["rollup_period_task_ignored_total"]}'
    )
    assert statistics['rollup_period_tasks_total'] == 108, f'Should have 108 total tasks, got {statistics["rollup_period_tasks_total"]}'

    # Verify that the sum matches
    calculated_total = (
        statistics['rollup_period_task_ok_total']
        + statistics['rollup_period_task_failed_total']
        + statistics['rollup_period_task_unreachable_total']
        + statistics['rollup_period_task_skipped_total']
        + statistics['rollup_period_task_ignored_total']
    )
    assert calculated_total == statistics['rollup_period_tasks_total'], (
        f'Sum of individual task counts ({calculated_total}) should equal rollup_period_tasks_total ({statistics["rollup_period_tasks_total"]})'
    )

    # ========== Validate Events Statistics ==========
    assert 'rollup_period_collected_events_total' in statistics, 'Should have rollup_period_collected_events_total in statistics'
    assert statistics['rollup_period_collected_events_total'] == 142, (
        f'Should have 142 collected events total, got {statistics["rollup_period_collected_events_total"]}'
    )
    assert 'rollup_period_warnings_total' in statistics, 'Should have rollup_period_warnings_total in statistics'
    assert statistics['rollup_period_warnings_total'] == 3, (
        f'Should have 3 warnings total, got {statistics["rollup_period_warnings_total"]}'
    )
    assert 'rollup_period_deprecations_total' in statistics, 'Should have rollup_period_deprecations_total in statistics'
    assert statistics['rollup_period_deprecations_total'] == 2, (
        f'Should have 2 deprecations total, got {statistics["rollup_period_deprecations_total"]}'
    )

    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    assert isinstance(jobs_list, list)
    assert result['statistics']['rollup_period_jobs_total'] == 8, f'Should have 8 total jobs, got {result["statistics"]["rollup_period_jobs_total"]}'

    # Validate job types: 'job' (7 jobs) and 'workflowjob' (1 job)
    job_type_jobs = [j for j in jobs_list if j['job_type'] == 'job']
    workflowjob_type_jobs = [j for j in jobs_list if j['job_type'] == 'workflowjob']

    assert len(job_type_jobs) == 1, 'Should have 1 job_type group for "job"'
    assert len(workflowjob_type_jobs) == 1, 'Should have 1 job_type group for "workflowjob"'

    job_type = job_type_jobs[0]
    assert job_type['jobs_total'] == 7, f'Should have 7 jobs of type "job", got {job_type["jobs_total"]}'
    assert job_type['job_type'] == 'job'

    workflowjob_type = workflowjob_type_jobs[0]
    assert workflowjob_type['jobs_total'] == 1, f'Should have 1 job of type "workflowjob", got {workflowjob_type["jobs_total"]}'
    assert workflowjob_type['job_type'] == 'workflowjob'

    # Validate controller_versions at top level
    assert 'rollup_period_controller_versions' in result, 'Should have controller_versions at top level'
    statistics_controller_versions = result['rollup_period_controller_versions']
    assert isinstance(statistics_controller_versions, list), 'controller_versions should be a list'
    # Expected: 2.15.0, 2.16.0, 2.17.0, 2.18.0, 2.19.0
    expected_versions = ['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0']
    assert len(statistics_controller_versions) == 5, f'Should have 5 unique controller versions, got {len(statistics_controller_versions)}'
    for version in expected_versions:
        assert version in statistics_controller_versions, f'Should have controller version {version}'

    # Validate scm_types at top level
    assert 'rollup_period_scm_types' in result, 'Should have rollup_period_scm_types at top level'
    assert result['rollup_period_scm_types'] == ['git', 'manual'], (
        f"Expected ['git', 'manual'] for rollup_period_scm_types, got {result['rollup_period_scm_types']}"
    )

    # ========== Validate Jobs by Launch Type ==========
    jobs_by_launch_type_list = result['jobs_by_launch_type']
    assert isinstance(jobs_by_launch_type_list, list), 'jobs_by_launch_type should be a list'

    # Expected launch types: 'manual' (5 jobs), 'scheduled' (1 job), 'workflow' (1 job), 'callback' (1 job)
    assert len(jobs_by_launch_type_list) == 4, f'Should have 4 launch types, got {len(jobs_by_launch_type_list)}'

    manual_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'manual'), None)
    scheduled_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'scheduled'), None)
    workflow_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'workflow'), None)
    callback_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'callback'), None)

    assert manual_entry is not None, 'Should have manual launch_type'
    assert scheduled_entry is not None, 'Should have scheduled launch_type'
    assert workflow_entry is not None, 'Should have workflow launch_type'
    assert callback_entry is not None, 'Should have callback launch_type'

    assert manual_entry['jobs_total'] == 5, f'manual should have 5 jobs, got {manual_entry["jobs_total"]}'
    assert scheduled_entry['jobs_total'] == 1, f'scheduled should have 1 job, got {scheduled_entry["jobs_total"]}'
    assert workflow_entry['jobs_total'] == 1, f'workflow should have 1 job, got {workflow_entry["jobs_total"]}'
    assert callback_entry['jobs_total'] == 1, f'callback should have 1 job, got {callback_entry["jobs_total"]}'

    # ========== Validate Jobs by Controller Version ==========
    jobs_by_controller_version_list = result['jobs_by_controller_version']
    assert isinstance(jobs_by_controller_version_list, list), 'jobs_by_controller_version should be a list'

    # Expected: 2.15.0 (3 jobs), 2.16.0 (2 jobs), 2.17.0 (1 job), 2.18.0 (1 job), 2.19.0 (1 job)
    assert len(jobs_by_controller_version_list) == 5, f'Should have 5 controller versions, got {len(jobs_by_controller_version_list)}'

    version_2_15_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.15.0'), None)
    version_2_16_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.16.0'), None)
    version_2_17_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.17.0'), None)
    version_2_18_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.18.0'), None)
    version_2_19_0 = next((j for j in jobs_by_controller_version_list if j.get('controller_version') == '2.19.0'), None)

    assert version_2_15_0 is not None, 'Should have controller_version 2.15.0'
    assert version_2_16_0 is not None, 'Should have controller_version 2.16.0'
    assert version_2_17_0 is not None, 'Should have controller_version 2.17.0'
    assert version_2_18_0 is not None, 'Should have controller_version 2.18.0'
    assert version_2_19_0 is not None, 'Should have controller_version 2.19.0'

    assert version_2_15_0['jobs_total'] == 3, f'2.15.0 should have 3 jobs, got {version_2_15_0["jobs_total"]}'
    assert version_2_16_0['jobs_total'] == 2, f'2.16.0 should have 2 jobs, got {version_2_16_0["jobs_total"]}'
    assert version_2_17_0['jobs_total'] == 1, f'2.17.0 should have 1 job, got {version_2_17_0["jobs_total"]}'
    assert version_2_18_0['jobs_total'] == 1, f'2.18.0 should have 1 job, got {version_2_18_0["jobs_total"]}'
    assert version_2_19_0['jobs_total'] == 1, f'2.19.0 should have 1 job, got {version_2_19_0["jobs_total"]}'

    # ========== Validate Job Host Summary (merged into jobs_by_job_type) ==========
    # Calculate expected totals from all jobs
    # Job1: 4 hosts (3 successful, 1 failed)
    # Job2: 4 hosts (4 successful)
    # Job3: 4 hosts (2 successful, 1 unreachable, 1 failed)
    # Job4: 4 hosts (2 successful, 1 unreachable, 1 failed)
    # Job5: 4 hosts (4 successful)
    # Job6: 4 hosts (2 successful, 1 unreachable, 1 failed)
    # Job7: 4 hosts (2 successful, 1 unreachable, 1 failed) - workflowjob
    # Job8: 4 hosts (4 successful)

    # Total job_host_pairs: 8 jobs × 4 hosts = 32
    assert result['statistics']['rollup_period_job_host_pairs_total'] == 32, (
        f'Should have 32 total job host pairs, got {result["statistics"]["rollup_period_job_host_pairs_total"]}'
    )

    # Validate merged host summary fields for 'job' type (jobs 1-6, 8 = 7 jobs)
    # Aggregate ok, failures, dark, skipped from all 7 job-type jobs
    # We'll validate that the totals are reasonable (non-zero for some fields)
    assert 'ok_total' in job_type, 'Should have ok_total field from job_host_summary merge'
    assert 'failures_total' in job_type, 'Should have failures_total field from job_host_summary merge'
    assert 'dark_total' in job_type, 'Should have dark_total field from job_host_summary merge'
    assert job_type['ok_total'] > 0, 'Should have some ok tasks'
    # Some jobs have failures, so failures_total should be > 0
    assert job_type['failures_total'] > 0, 'Should have some failures'
    # Some jobs have dark tasks, so dark_total should be > 0
    assert job_type['dark_total'] > 0, 'Should have some dark tasks'

    # Validate merged host summary fields for 'workflowjob' type (job 7)
    assert 'ok_total' in workflowjob_type, 'Should have ok_total field from job_host_summary merge'
    assert 'failures_total' in workflowjob_type, 'Should have failures_total field from job_host_summary merge'
    assert 'dark_total' in workflowjob_type, 'Should have dark_total field from job_host_summary merge'
    assert workflowjob_type['ok_total'] > 0, 'Should have some ok tasks'
    assert workflowjob_type['failures_total'] > 0, 'Should have some failures'
    assert workflowjob_type['dark_total'] > 0, 'Should have some dark tasks'

    # ========== Validate Module Stats ==========
    module_stats = result['module_stats']
    assert isinstance(module_stats, list), 'module_stats should be a list'
    # Expected: 6 unique modules (ansible.builtin.copy appears in both T1 and T2/T3, but counted once)
    # T1: ansible.builtin.copy, ansible.builtin.file, ansible.builtin.yum
    # T2/T3: ansible.builtin.copy (duplicate), community.general.git, community.general.archive, community.weird.git
    # Total unique: 6 modules
    assert len(module_stats) == 6, f'Should have 6 unique modules, got {len(module_stats)}'
    assert result['statistics']['rollup_period_modules_total'] == 6, (
        f'Should have 6 modules in statistics, got {result["statistics"]["rollup_period_modules_total"]}'
    )

    # Verify module stats structure (module names are anonymized, so we check structure)
    for module in module_stats:
        assert 'module_name' in module, 'Each module should have module_name field'
        assert 'collection_name' in module, 'Each module should have collection_name field'
        assert 'jobs_total' in module, 'Each module should have jobs_total field'
        assert 'unique_hosts_total' in module, 'Each module should have unique_hosts_total field'
        assert 'processed_events_total' in module, 'Each module should have processed_events_total field'
        assert isinstance(module['processed_events_total'], (int, float)), 'processed_events_total should be a number'
        assert module['processed_events_total'] > 0, 'processed_events_total should be positive'
        assert 'controller_versions' in module, 'Each module should have controller_versions field'
        assert isinstance(module['controller_versions'], list), 'controller_versions should be a list'
        # controller_versions should contain the ansible_version values from the jobs
        # Jobs 1-3: 2.15.0, Job 4: 2.16.0, Job 5: 2.16.0, Job 6: 2.17.0, Job 7: 2.18.0, Job 8: 2.19.0
        assert len(module['controller_versions']) > 0, 'controller_versions should not be empty'
        # Verify all versions are valid (should be in the expected set)
        expected_versions = {'2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0'}
        for version in module['controller_versions']:
            assert version in expected_versions, f'Unexpected controller_versions {version} in module {module.get("module_name")}'

    # ========== Validate Collection Stats ==========
    collection_stats = result['collection_name_stats']
    assert isinstance(collection_stats, list), 'collection_name_stats should be a list'
    # Expected: 3 unique collections (ansible.builtin, community.general, community.weird)
    assert len(collection_stats) == 3, f'Should have 3 unique collections, got {len(collection_stats)}'

    # Verify collection stats structure (collection names are anonymized, so we check structure)
    for collection in collection_stats:
        assert 'collection_name' in collection, 'Each collection should have collection_name field'
        assert 'collection_source' in collection, 'Each collection should have collection_source field'
        assert 'jobs_total' in collection, 'Each collection should have jobs_total field'
        assert 'processed_events_total' in collection, 'Each collection should have processed_events_total field'
        assert isinstance(collection['processed_events_total'], (int, float)), 'processed_events_total should be a number'
        assert collection['processed_events_total'] > 0, 'processed_events_total should be positive'
        assert 'controller_versions' in collection, 'Each collection should have controller_versions field'
        assert isinstance(collection['controller_versions'], list), 'controller_versions should be a list'
        # controller_versions should contain the ansible_version values from the jobs
        assert len(collection['controller_versions']) > 0, 'controller_versions should not be empty'
        # Verify all versions are valid (should be in the expected set)
        expected_versions = {'2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0'}
        for version in collection['controller_versions']:
            assert version in expected_versions, f'Unexpected controller_versions {version} in collection {collection.get("collection_name")}'

    # ========== Validate Playbooks ==========
    # modules_used_per_playbook is computed but not included in final output
    assert 'rollup_period_playbooks_total' in result['statistics'], 'Should have playbooks_total in statistics'
    assert result['statistics']['rollup_period_playbooks_total'] >= 2, (
        f'Should have at least 2 total playbooks, got {result["statistics"]["rollup_period_playbooks_total"]}'
    )

    # ========== Validate Execution Environments ==========
    assert 'rollup_period_execution_environments_total' in result['statistics']
    assert result['statistics']['rollup_period_execution_environments_total'] == 8
    assert result['statistics']['rollup_period_EE_default_total'] == 4
    assert result['statistics']['rollup_period_EE_custom_total'] == 4

    # ========== Validate Credentials ==========
    # Expected credential types from credentials.py (all jobs combined):
    # - Amazon Web Services (jobs 1, 5, 7)
    # - Container Registry (job 6)
    # - Machine (all jobs)
    # - Network (job 3)
    # - Source Control (job 4)
    # - Vault (jobs 2, 8)
    assert 'rollup_period_credential_types' in result
    credential_types = result['rollup_period_credential_types']
    assert isinstance(credential_types, list)
    assert 'Amazon Web Services' in credential_types
    assert 'Container Registry' in credential_types
    assert 'Machine' in credential_types
    assert 'Network' in credential_types
    assert 'Source Control' in credential_types
    assert 'Vault' in credential_types
    assert len(credential_types) == 6
    assert credential_types == sorted(credential_types)  # Should be sorted

    # ========== Verify totals match between all groupings ==========
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in jobs_by_launch_type_list)
    total_jobs_by_controller_version = sum(j.get('jobs_total', 0) for j in jobs_by_controller_version_list)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_controller_version == result['statistics']['rollup_period_jobs_total'], (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_controller_version={total_jobs_by_controller_version}, '
        f'statistics={result["statistics"]["rollup_period_jobs_total"]}'
    )

    print('\n=== All validations passed! ===')
