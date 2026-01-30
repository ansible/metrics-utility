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

    # 1. Jobs data - split into 2 CSV files
    jobs_part1 = jobs[:3]  # First 3 jobs
    jobs_part2 = jobs[3:]  # Remaining jobs

    jobs_csv_files = []
    csv1 = create_csv_file(jobs_part1, f'{data_dir}/part1_unified_jobs.csv')
    if csv1:
        jobs_csv_files.append(csv1)
    csv2 = create_csv_file(jobs_part2, f'{data_dir}/part2_unified_jobs.csv')
    if csv2:
        jobs_csv_files.append(csv2)

    # 2. Events data - split into 3 CSV files
    events_part1 = events[:100]  # First 100 events
    events_part2 = events[100:200]  # Middle events
    events_part3 = events[200:]  # Remaining events

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

    # 3. Execution environments - split into 2 CSV files
    ee_part1 = execution_environments[:2]
    ee_part2 = execution_environments[2:]

    ee_csv_files = []
    csv1 = create_csv_file(ee_part1, f'{data_dir}/part1_execution_environments.csv')
    if csv1:
        ee_csv_files.append(csv1)
    csv2 = create_csv_file(ee_part2, f'{data_dir}/part2_execution_environments.csv')
    if csv2:
        ee_csv_files.append(csv2)

    # 4. Job host summary - split into 2 CSV files
    jhs_part1 = jobhostsummary[:8]  # First 8 entries
    jhs_part2 = jobhostsummary[8:]  # Remaining entries

    jhs_csv_files = []
    csv1 = create_csv_file(jhs_part1, f'{data_dir}/part1_job_host_summary.csv')
    if csv1:
        jhs_csv_files.append(csv1)
    csv2 = create_csv_file(jhs_part2, f'{data_dir}/part2_job_host_summary.csv')
    if csv2:
        jhs_csv_files.append(csv2)

    # ========== Run the anonymized rollup computation ==========

    # Create input_data dict with lists of CSV file paths
    input_data = {
        'unified_jobs': jobs_csv_files,
        'job_host_summary': jhs_csv_files,
        'main_jobevent': events_csv_files,
        'execution_environments': ee_csv_files,
    }

    result = compute_anonymized_rollup_from_raw_data(
        input_data=input_data, salt='test_salt', since=since, until=until, base_path=base_path, save_rollups=False
    )

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
        # job_host_summary is now merged into jobs_by_job_type
        assert 'module_stats' in result
        assert 'collection_name_stats' in result
        assert 'modules_used_per_playbook' in result

    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 3  # job, workflowjob, adhoccommand
    assert result['statistics']['jobs_total'] == 5  # Total jobs across all job types

    # 'job' type should have data from both tarballs (jobs 1, 2, 4)
    job_type_jobs = [j for j in jobs_list if j['job_type'] == 'job' and j['jobs_total'] == 3]
    assert len(job_type_jobs) == 1
    job_type = job_type_jobs[0]
    assert job_type['jobs_total'] == 3
    assert job_type['jobs_failed_total'] == 1
    assert job_type['jobs_succeeded_total'] == 2
    assert job_type['jobs_never_started_total'] == 0
    # Check timing statistics
    assert job_type['job_duration_total_seconds'] == pytest.approx(10.0)
    assert job_type['job_duration_minimum_seconds'] == pytest.approx(2.0)
    assert job_type['job_duration_maximum_seconds'] == pytest.approx(5.0)
    assert job_type['job_waiting_time_total_seconds'] == pytest.approx(3.0)
    assert job_type['job_waiting_time_minimum_seconds'] == pytest.approx(0.0)
    assert job_type['job_waiting_time_maximum_seconds'] == pytest.approx(2.0)
    # Check job_type field
    assert job_type['job_type'] == 'job'

    # 'workflowjob' type should have 1 job executed
    workflowjob_type_jobs = [j for j in jobs_list if j['job_type'] == 'workflowjob' and j['jobs_never_started_total'] == 0]
    assert len(workflowjob_type_jobs) == 1
    workflowjob_type = workflowjob_type_jobs[0]
    assert workflowjob_type['jobs_total'] == 1
    assert workflowjob_type['jobs_failed_total'] == 0
    assert workflowjob_type['jobs_succeeded_total'] == 1
    assert workflowjob_type['job_duration_total_seconds'] == pytest.approx(7.0)
    assert workflowjob_type['job_waiting_time_total_seconds'] == pytest.approx(4.0)
    # Check job_type field
    assert workflowjob_type['job_type'] == 'workflowjob'

    # 'adhoccommand' type should have never started job
    adhoccommand_type_jobs = [j for j in jobs_list if j['job_type'] == 'adhoccommand' and j['jobs_never_started_total'] == 1]
    assert len(adhoccommand_type_jobs) == 1
    adhoccommand_type = adhoccommand_type_jobs[0]
    assert adhoccommand_type['jobs_total'] == 1
    assert adhoccommand_type['jobs_failed_total'] == 1
    assert adhoccommand_type['jobs_succeeded_total'] == 0
    assert adhoccommand_type['job_duration_total_seconds'] == pytest.approx(0.0)
    assert adhoccommand_type['job_waiting_time_total_seconds'] == pytest.approx(0.0)
    # Check job_type field
    assert adhoccommand_type['job_type'] == 'adhoccommand'

    # ========== Validate Execution Environments ==========
    assert result['statistics']['execution_environments_total'] == 5
    assert result['statistics']['execution_environments_default_total'] == 2
    assert result['statistics']['execution_environments_custom_total'] == 3

    # ========== Validate Job Host Summary (merged into jobs_by_job_type) ==========
    # unique_hosts_total is now summed across all job_type groups
    # job type has 5 unique hosts (h1-h5), workflowjob type has 3 unique hosts (h1-h3)
    # Total = 5 + 3 = 8 (some hosts appear in both types)
    assert result['statistics']['unique_hosts_total'] == 8, 'Should have 8 unique hosts total (5 for job + 3 for workflowjob)'

    # Find the 'job' type group in jobs_by_job_type
    job_type_entry = next((j for j in jobs_list if j['job_type'] == 'job'), None)
    assert job_type_entry is not None, 'Should have job_type job'
    # Validate merged host summary fields
    assert job_type_entry['unique_hosts_total'] == 5, 'Should have 5 unique hosts for job type'
    assert job_type_entry['ok_total'] == 26, 'Should have 26 ok tasks for job type'
    assert job_type_entry['failures_total'] == 2, 'Should have 2 failures for job type'
    assert job_type_entry['skipped_total'] == 2, 'Should have 2 skipped for job type'
    assert job_type_entry['dark_total'] == 0, 'Should have 0 dark for job type'
    assert job_type_entry['ignored_total'] == 0, 'Should have 0 ignored for job type'
    assert job_type_entry['rescued_total'] == 0, 'Should have 0 rescued for job type'
    
    # Find the 'workflowjob' type group in jobs_by_job_type
    workflowjob_type_entry = next((j for j in jobs_list if j['job_type'] == 'workflowjob'), None)
    assert workflowjob_type_entry is not None, 'Should have job_type workflowjob'
    # Validate merged host summary fields
    assert workflowjob_type_entry['unique_hosts_total'] == 3, 'Should have 3 unique hosts for workflowjob type'
    assert workflowjob_type_entry['ok_total'] == 26, 'Should have 26 ok tasks for workflowjob type'
    assert workflowjob_type_entry['failures_total'] == 4, 'Should have 4 failures for workflowjob type'
    assert workflowjob_type_entry['skipped_total'] == 0, 'Should have 0 skipped for workflowjob type'
    assert workflowjob_type_entry['dark_total'] == 0, 'Should have 0 dark for workflowjob type'
    assert workflowjob_type_entry['ignored_total'] == 0, 'Should have 0 ignored for workflowjob type'
    assert workflowjob_type_entry['rescued_total'] == 0, 'Should have 0 rescued for workflowjob type'
    
    # 'adhoccommand' type should have default values (0) for host summary fields since no match
    adhoccommand_type_entry = next((j for j in jobs_list if j['job_type'] == 'adhoccommand'), None)
    assert adhoccommand_type_entry is not None, 'Should have job_type adhoccommand'
    assert adhoccommand_type_entry['unique_hosts_total'] == 0, 'Should have 0 unique hosts (no job_host_summary match)'
    assert adhoccommand_type_entry['ok_total'] == 0, 'Should have 0 ok tasks (no job_host_summary match)'
    assert adhoccommand_type_entry['failures_total'] == 0, 'Should have 0 failures (no job_host_summary match)'
    assert adhoccommand_type_entry['skipped_total'] == 0, 'Should have 0 skipped (no job_host_summary match)'
    
    # Verify totals across all job types
    total_ok = sum(j.get('ok_total', 0) for j in jobs_list)
    total_failures = sum(j.get('failures_total', 0) for j in jobs_list)
    total_skipped = sum(j.get('skipped_total', 0) for j in jobs_list)
    assert total_ok == 52, 'Should have 52 ok tasks total (26 from job + 26 from workflowjob)'
    assert total_failures == 6, 'Should have 6 failures total (2 from job + 4 from workflowjob)'
    assert total_skipped == 2, 'Should have 2 skipped total (2 from job + 0 from workflowjob)'

    # ========== Validate Events Modules ==========
    # In flattened structure, events_modules data is now in statistics and direct arrays

    # Verify values from concatenated data across 3 tarballs
    assert result['statistics']['modules_used_to_automate_total'] == 7, 'Should have 7 unique modules from all tarballs'
    assert result['statistics']['hosts_automated_total'] == 9, 'Should have 9 unique hosts from all tarballs'

    # Check specific known modules are present in module_stats
    module_names = [m['module_name'] for m in result['module_stats'] if 'module_name' in m]
    assert 'ansible.netcommon.cli_config' in module_names
    assert 'ansible.posix.firewalld' in module_names
    assert 'ansible.windows.win_copy' in module_names
    assert 'community.aws.ec2' in module_names
    assert 'community.general.yum' in module_names
    assert 'community.mongodb.insert' in module_names

    # Verify module stats have data from all tarballs
    module_stats = result['module_stats']
    assert isinstance(module_stats, list), 'module_stats should be a list'
    assert len(module_stats) == 7, 'Should have stats for all 7 modules'

    # Verify specific module stats (ansible.windows.win_copy as an example)
    win_copy_stats = [m for m in module_stats if m.get('module_name') == 'ansible.windows.win_copy']
    assert len(win_copy_stats) == 1, 'Should have exactly one entry for ansible.windows.win_copy'
    win_copy = win_copy_stats[0]
    assert win_copy['collection_source'] == 'certified'
    assert win_copy['collection_name'] == 'ansible.windows'
    assert win_copy['jobs_total'] == 3
    assert win_copy['hosts_total'] == 3
    assert win_copy['task_clean_success_total'] == 1
    assert win_copy['task_success_with_reruns_total'] == 2
    assert win_copy['task_failed_total'] == 0
    assert win_copy['job_duration_total_seconds'] == pytest.approx(2100.0)

    # Verify another module (community.general.yum)
    yum_stats = [m for m in module_stats if m.get('module_name') == 'community.general.yum']
    assert len(yum_stats) == 1, 'Should have exactly one entry for community.general.yum'
    yum = yum_stats[0]
    assert yum['collection_source'] == 'community'
    assert yum['jobs_total'] == 3
    assert yum['jobs_never_started_total'] == 1
    assert yum['task_failed_total'] == 3
    assert yum['jobs_failed_because_of_module_failure_total'] == 3

    # Verify collection stats
    collection_stats = result['collection_name_stats']
    assert isinstance(collection_stats, list), 'collection_name_stats should be a list'
    assert len(collection_stats) == 7, 'Should have stats for all 7 collections'

    # Verify specific collection stats (ansible.windows)
    windows_collection = [c for c in collection_stats if c.get('collection_name') == 'ansible.windows']
    assert len(windows_collection) == 1, 'Should have exactly one entry for ansible.windows collection'
    windows_coll = windows_collection[0]
    assert windows_coll['collection_source'] == 'certified'
    assert windows_coll['jobs_total'] == 3
    assert windows_coll['hosts_total'] == 3
    assert windows_coll['task_clean_success_total'] == 1
    assert windows_coll['task_success_with_reruns_total'] == 2

    # Verify modules_used_per_playbook is now an array with 5 entries (flattened structure)
    playbook_modules = result['modules_used_per_playbook']
    assert isinstance(playbook_modules, list), 'modules_used_per_playbook should be a list'
    assert len(playbook_modules) == 5, 'Should have 5 playbooks'
    # Check values sum to expected total
    total_module_usage = sum(p['modules_used'] for p in playbook_modules)
    assert total_module_usage == 15, 'Total module usage across playbooks should be 15'


def test_empty_csv_files_handling(cleanup_test_data):
    """
    Test that the system handles case with no CSV files gracefully.
    """

    base_path = './out'
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)
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
    }

    # Should not crash, but return empty/default results
    result = compute_anonymized_rollup_from_raw_data(
        input_data=input_data, salt='test_salt', since=since, until=until, base_path=base_path, save_rollups=False
    )

    # Print the result for debugging
    import json

    json_content = json.dumps(result, indent=4)
    print('\n=== Empty CSV Files Result ===')
    print(json_content)

    # Validate flattened structure exists even with empty data
    assert 'statistics' in result
    assert 'jobs_by_job_type' in result
    # job_host_summary is now merged into jobs_by_job_type
    assert 'module_stats' in result
    assert 'collection_name_stats' in result
    assert 'modules_used_per_playbook' in result

    # Verify statistics contains all fields (with null values for empty data)
    statistics = result['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dict'
    assert 'modules_used_to_automate_total' in statistics
    assert 'hosts_automated_total' in statistics
    assert 'execution_environments_total' in statistics
    assert 'execution_environments_default_total' in statistics
    assert 'execution_environments_custom_total' in statistics
    assert 'jobs_total' in statistics
    assert 'unique_hosts_total' in statistics

    # All statistics should be None for empty data
    assert statistics['modules_used_to_automate_total'] is None
    assert statistics['hosts_automated_total'] is None
    assert statistics['execution_environments_total'] is None
    assert statistics['execution_environments_default_total'] is None
    assert statistics['execution_environments_custom_total'] is None
    assert statistics['jobs_total'] is None
    assert statistics['unique_hosts_total'] is None

    # Verify all arrays are empty
    assert isinstance(result['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    assert len(result['jobs_by_job_type']) == 0, 'jobs_by_job_type should be empty with no data'
    # job_host_summary is now merged into jobs_by_job_type

    assert isinstance(result['module_stats'], list), 'module_stats should be a list'
    assert len(result['module_stats']) == 0, 'module_stats should be empty with no data'

    assert isinstance(result['collection_name_stats'], list), 'collection_name_stats should be a list'
    assert len(result['collection_name_stats']) == 0, 'collection_name_stats should be empty with no data'

    assert isinstance(result['modules_used_per_playbook'], list), 'modules_used_per_playbook should be a list'
    assert len(result['modules_used_per_playbook']) == 0, 'modules_used_per_playbook should be empty with no data'
