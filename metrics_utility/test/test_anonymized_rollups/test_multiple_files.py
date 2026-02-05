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
from metrics_utility.test.test_anonymized_rollups.test_credentials_anonymized_rollup import credentials


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
    # Note: There are 6 jobs in the test data, split evenly: part1: 3 jobs, part2: 3 jobs
    jobs_part1 = jobs[:3]  # First 3 jobs
    jobs_part2 = jobs[3:]  # Remaining 3 jobs

    jobs_csv_files = []
    csv1 = create_csv_file(jobs_part1, f'{data_dir}/part1_unified_jobs.csv')
    if csv1:
        jobs_csv_files.append(csv1)
    csv2 = create_csv_file(jobs_part2, f'{data_dir}/part2_unified_jobs.csv')
    if csv2:
        jobs_csv_files.append(csv2)

    # 2. Events data - split into 3 CSV files
    # Note: There are 23 events in the test data (20 task events + 2 warnings + 1 deprecated),
    # so we split them into 3 parts: part1: 8 events, part2: 8 events, part3: 7 events
    # This ensures all three batches are tested for proper batch processing
    events_part1 = events[:8]  # First 8 events
    events_part2 = events[8:16]  # Middle 8 events
    events_part3 = events[16:]  # Remaining 7 events

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
    # Note: There are 5 entries in the test data, split as: part1: 2 entries, part2: 3 entries
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
    # Note: There are 16 entries in the test data, split evenly: part1: 8 entries, part2: 8 entries
    jhs_part1 = jobhostsummary[:8]  # First 8 entries
    jhs_part2 = jobhostsummary[8:]  # Remaining 8 entries

    jhs_csv_files = []
    csv1 = create_csv_file(jhs_part1, f'{data_dir}/part1_job_host_summary.csv')
    if csv1:
        jhs_csv_files.append(csv1)
    csv2 = create_csv_file(jhs_part2, f'{data_dir}/part2_job_host_summary.csv')
    if csv2:
        jhs_csv_files.append(csv2)

    # 5. Credentials - split into 2 CSV files
    # Note: There are 10 entries in the test data, split evenly: part1: 5 entries, part2: 5 entries
    cred_part1 = credentials[:5]  # First 5 entries
    cred_part2 = credentials[5:]  # Remaining 5 entries

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
        assert 'collection_name_stats' in result
        assert 'modules_used_per_playbook' in result
        assert 'collections_versions' in result

    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 3  # job, workflowjob, adhoccommand
    assert result['statistics']['jobs_total'] == 5  # Total jobs across all job types
    # job_templates_total should be sum of templates_total from all job_type groups (1 + 1 + 1 = 3)
    assert result['statistics']['job_templates_total'] == 3, 'Should have 3 total job templates (sum from all job_type groups)'
    
    # Validate ansible_versions in statistics is merged from jobs_by_job_type
    assert 'ansible_versions' in result['statistics'], 'Should have ansible_versions in statistics'
    statistics_ansible_versions = result['statistics']['ansible_versions']
    assert isinstance(statistics_ansible_versions, list), 'ansible_versions should be a list'
    # Get ansible_versions from jobs_by_job_type and merge them
    jobs_by_job_type = result.get('jobs_by_job_type', [])
    expected_versions_set = set()
    for job in jobs_by_job_type:
        ansible_versions = job.get('ansible_versions', [])
        if isinstance(ansible_versions, list):
            expected_versions_set.update(ansible_versions)
    expected_versions = sorted(list(expected_versions_set))
    assert statistics_ansible_versions == expected_versions, (
        f"Expected ansible_versions {expected_versions} in statistics, got {statistics_ansible_versions}"
    )
    # Based on test data, we should have: 2.9.0, 2.10.0, 2.11.0, 2.12.0, 2.14.0
    # Sorted: ['2.10.0', '2.11.0', '2.12.0', '2.14.0', '2.9.0']
    assert len(statistics_ansible_versions) == 5, (
        f"Expected 5 unique ansible versions, got {len(statistics_ansible_versions)}"
    )
    assert '2.9.0' in statistics_ansible_versions
    assert '2.10.0' in statistics_ansible_versions
    assert '2.11.0' in statistics_ansible_versions
    assert '2.12.0' in statistics_ansible_versions
    assert '2.14.0' in statistics_ansible_versions

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
    # Validate ansible_versions in by_job_type
    # 'job' type has jobs 1, 2, 4 with versions: 2.9.0, 2.10.0, 2.12.0
    assert 'ansible_versions' in job_type, 'Should have ansible_versions field in by_job_type'
    assert job_type['ansible_versions'] == ['2.10.0', '2.12.0', '2.9.0'], (
        f"Expected ['2.10.0', '2.12.0', '2.9.0'] for job type, got {job_type['ansible_versions']}"
    )

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
    # Validate ansible_versions in by_job_type
    # 'workflowjob' type has job 3 with version: 2.11.0
    assert 'ansible_versions' in workflowjob_type, 'Should have ansible_versions field in by_job_type'
    assert workflowjob_type['ansible_versions'] == ['2.11.0'], (
        f"Expected ['2.11.0'] for workflowjob type, got {workflowjob_type['ansible_versions']}"
    )

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
    # Validate ansible_versions in by_job_type
    # 'adhoccommand' type has job 6 with version: 2.14.0
    assert 'ansible_versions' in adhoccommand_type, 'Should have ansible_versions field in by_job_type'
    assert adhoccommand_type['ansible_versions'] == ['2.14.0'], (
        f"Expected ['2.14.0'] for adhoccommand type, got {adhoccommand_type['ansible_versions']}"
    )

    # ========== Validate Execution Environments ==========
    assert result['statistics']['execution_environments_total'] == 5
    assert result['statistics']['execution_environments_default_total'] == 2
    assert result['statistics']['execution_environments_custom_total'] == 3

    # ========== Validate Job Host Summary (merged into jobs_by_job_type) ==========
    # unique_hosts_total is now summed across all job_type groups
    # job type has 5 unique hosts (h1-h5), workflowjob type has 3 unique hosts (h1-h3)
    # Total = 5 + 3 = 8 (some hosts appear in both types)
    assert result['statistics']['unique_hosts_total'] == 8, 'Should have 8 unique hosts total (5 for job + 3 for workflowjob)'
    # job_host_pairs_total should be 16 (10 for job type + 6 for workflowjob type)
    assert result['statistics']['job_host_pairs_total'] == 16, (
        f'Should have 16 total job host summary records, got {result["statistics"]["job_host_pairs_total"]}'
    )

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
    
    # Verify warnings_total and deprecations_total
    # Test data has 2 warnings (job 1 and job 2) and 1 deprecated (job 3)
    assert 'warnings_total' in result['statistics'], 'Should have warnings_total in statistics'
    assert result['statistics']['warnings_total'] == 2, f"Expected 2 warnings, got {result['statistics']['warnings_total']}"
    assert 'deprecations_total' in result['statistics'], 'Should have deprecations_total in statistics'
    assert result['statistics']['deprecations_total'] == 1, f"Expected 1 deprecated event, got {result['statistics']['deprecations_total']}"

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
    assert result['statistics']['playbooks_total'] == 5, 'Should have 5 total playbooks'
    # Check values sum to expected total
    total_module_usage = sum(p['modules_used'] for p in playbook_modules)
    assert total_module_usage == 15, 'Total module usage across playbooks should be 15'

    # ========== Validate Credentials ==========
    print('--- Validating credentials data values ---')
    # Credentials are added to statistics
    # Expected counts from credentials test data (from test_credentials_anonymized_rollup.py):
    # Machine: 2, Vault: 1, Source Control: 2, Network: 1, Amazon Web Services: 3, Container Registry: 1
    assert 'credential_type_machine_total' in result['statistics'], 'Should have credential_type_machine_total in statistics'
    assert result['statistics']['credential_type_machine_total'] == 2, 'Should have 2 Machine credentials'
    assert 'credential_type_vault_total' in result['statistics'], 'Should have credential_type_vault_total in statistics'
    assert result['statistics']['credential_type_vault_total'] == 1, 'Should have 1 Vault credential'
    assert 'credential_type_source_control_total' in result['statistics'], 'Should have credential_type_source_control_total in statistics'
    assert result['statistics']['credential_type_source_control_total'] == 2, 'Should have 2 Source Control credentials'
    assert 'credential_type_network_total' in result['statistics'], 'Should have credential_type_network_total in statistics'
    assert result['statistics']['credential_type_network_total'] == 1, 'Should have 1 Network credential'
    assert 'credential_type_amazon_web_services_total' in result['statistics'], 'Should have credential_type_amazon_web_services_total in statistics'
    assert result['statistics']['credential_type_amazon_web_services_total'] == 3, 'Should have 3 Amazon Web Services credentials'
    assert 'credential_type_container_registry_total' in result['statistics'], 'Should have credential_type_container_registry_total in statistics'
    assert result['statistics']['credential_type_container_registry_total'] == 1, 'Should have 1 Container Registry credential'

    # ========== Validate Collections Versions ==========
    print('--- Validating collections_versions data values ---')
    collections_versions = result['collections_versions']
    assert isinstance(collections_versions, list), 'collections_versions should be a list'

    # Expected collections from jobs (jobs 1-4 and 6, job 5 is filtered out):
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
        (c['name'], c['version']): c['job_count']
        for c in collections_versions
    }

    # Verify ansible.builtin 2.9.10 appears in 5 jobs
    assert collections_dict.get(('ansible.builtin', '2.9.10')) == 5, (
        f"Expected ansible.builtin 2.9.10 in 5 jobs, got {collections_dict.get(('ansible.builtin', '2.9.10'))}"
    )

    # Verify community.general appears with different versions (testing same collection with different versions)
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
    assert len(collections_versions) == 6, (
        f"Expected 6 unique collection-version pairs, got {len(collections_versions)}"
    )

    # Verify all entries have required fields (name, version, job_count)
    for collection in collections_versions:
        assert 'name' in collection, 'Each collection should have name field'
        assert 'version' in collection, 'Each collection should have version field'
        assert 'job_count' in collection, 'Each collection should have job_count field'
        assert isinstance(collection['job_count'], int), 'job_count should be an integer'
        assert collection['job_count'] > 0, 'job_count should be greater than 0'

    # ========== Validate Jobs by Launch Type ==========
    jobs_by_launch_type_list = result['jobs_by_launch_type']
    assert isinstance(jobs_by_launch_type_list, list), 'jobs_by_launch_type should be a list'
    
    # Expected launch types from test data (jobs 1-4 and 6, job 5 is filtered out):
    # Job 1: manual
    # Job 2: scheduled
    # Job 3: workflow
    # Job 4: callback
    # Job 6: scheduled
    # So we should have: manual, scheduled, workflow, callback (4 launch types)
    assert len(jobs_by_launch_type_list) == 4, f'Should have 4 launch types, got {len(jobs_by_launch_type_list)}'

    # Find launch type entries
    manual_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'manual'), None)
    scheduled_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'scheduled'), None)
    workflow_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'workflow'), None)
    callback_entry = next((j for j in jobs_by_launch_type_list if j.get('launch_type') == 'callback'), None)

    assert manual_entry is not None, 'Should have manual launch_type'
    assert scheduled_entry is not None, 'Should have scheduled launch_type'
    assert workflow_entry is not None, 'Should have workflow launch_type'
    assert callback_entry is not None, 'Should have callback launch_type'

    # Validate 'manual' launch_type (job 1)
    assert manual_entry['jobs_total'] == 1, 'manual should have 1 job'
    assert manual_entry['jobs_failed_total'] == 0, 'manual should have 0 failed jobs'
    assert manual_entry['jobs_succeeded_total'] == 1, 'manual should have 1 succeeded job'
    assert manual_entry['job_type_total'] == 1, 'manual should have 1 job type (job)'
    assert manual_entry['job_duration_total_seconds'] == pytest.approx(3.0), 'manual should have 3s total duration'
    # Should have default host summary fields (all zeros) since job_host_summary is grouped by job_type
    assert manual_entry['unique_hosts_total'] == 0, 'manual should have 0 unique hosts (no job_host_summary merge)'
    assert manual_entry['ok_total'] == 0, 'manual should have 0 ok tasks (no job_host_summary merge)'
    assert manual_entry['failures_total'] == 0, 'manual should have 0 failures (no job_host_summary merge)'

    # Validate 'scheduled' launch_type (jobs 2 and 6)
    assert scheduled_entry['jobs_total'] == 2, 'scheduled should have 2 jobs'
    assert scheduled_entry['jobs_failed_total'] == 2, 'scheduled should have 2 failed jobs (both job 2 and job 6 have failed=1)'
    assert scheduled_entry['jobs_succeeded_total'] == 0, 'scheduled should have 0 succeeded jobs'
    assert scheduled_entry['jobs_never_started_total'] == 1, 'scheduled should have 1 never started job'
    assert scheduled_entry['job_type_total'] == 2, 'scheduled should have 2 job types (job and adhoccommand)'
    assert scheduled_entry['job_duration_total_seconds'] == pytest.approx(5.0), 'scheduled should have 5s total duration'
    # Should have default host summary fields
    assert scheduled_entry['unique_hosts_total'] == 0, 'scheduled should have 0 unique hosts (no job_host_summary merge)'

    # Validate 'workflow' launch_type (job 3)
    assert workflow_entry['jobs_total'] == 1, 'workflow should have 1 job'
    assert workflow_entry['jobs_failed_total'] == 0, 'workflow should have 0 failed jobs'
    assert workflow_entry['jobs_succeeded_total'] == 1, 'workflow should have 1 succeeded job'
    assert workflow_entry['job_type_total'] == 1, 'workflow should have 1 job type (workflowjob)'
    assert workflow_entry['job_duration_total_seconds'] == pytest.approx(7.0), 'workflow should have 7s total duration'
    # Should have default host summary fields
    assert workflow_entry['unique_hosts_total'] == 0, 'workflow should have 0 unique hosts (no job_host_summary merge)'

    # Validate 'callback' launch_type (job 4)
    assert callback_entry['jobs_total'] == 1, 'callback should have 1 job'
    assert callback_entry['jobs_failed_total'] == 0, 'callback should have 0 failed jobs'
    assert callback_entry['jobs_succeeded_total'] == 1, 'callback should have 1 succeeded job'
    assert callback_entry['job_type_total'] == 1, 'callback should have 1 job type (job)'
    assert callback_entry['job_duration_total_seconds'] == pytest.approx(2.0), 'callback should have 2s total duration'
    # Should have default host summary fields
    assert callback_entry['unique_hosts_total'] == 0, 'callback should have 0 unique hosts (no job_host_summary merge)'

    # Verify that launch_type_*_total fields are NOT present (since we're grouping by launch_type)
    assert 'launch_type_manual_total' not in manual_entry, 'Should not have launch_type_manual_total when grouped by launch_type'
    assert 'launch_type_scheduled_total' not in scheduled_entry, 'Should not have launch_type_scheduled_total when grouped by launch_type'
    assert 'launch_type_workflow_total' not in workflow_entry, 'Should not have launch_type_workflow_total when grouped by launch_type'
    assert 'launch_type_callback_total' not in callback_entry, 'Should not have launch_type_callback_total when grouped by launch_type'

    # Verify that job_type_total is present (instead of launch_type counts)
    assert 'job_type_total' in manual_entry, 'Should have job_type_total field'
    assert 'job_type_total' in scheduled_entry, 'Should have job_type_total field'
    assert 'job_type_total' in workflow_entry, 'Should have job_type_total field'
    assert 'job_type_total' in callback_entry, 'Should have job_type_total field'

    # Validate ansible_versions in by_launch_type
    # 'manual' launch_type has job 1 with version: 2.9.0
    assert 'ansible_versions' in manual_entry, 'Should have ansible_versions field in by_launch_type'
    assert manual_entry['ansible_versions'] == ['2.9.0'], (
        f"Expected ['2.9.0'] for manual launch_type, got {manual_entry['ansible_versions']}"
    )
    # 'scheduled' launch_type has jobs 2, 6 with versions: 2.10.0, 2.14.0
    assert 'ansible_versions' in scheduled_entry, 'Should have ansible_versions field in by_launch_type'
    assert scheduled_entry['ansible_versions'] == ['2.10.0', '2.14.0'], (
        f"Expected ['2.10.0', '2.14.0'] for scheduled launch_type, got {scheduled_entry['ansible_versions']}"
    )
    # 'workflow' launch_type has job 3 with version: 2.11.0
    assert 'ansible_versions' in workflow_entry, 'Should have ansible_versions field in by_launch_type'
    assert workflow_entry['ansible_versions'] == ['2.11.0'], (
        f"Expected ['2.11.0'] for workflow launch_type, got {workflow_entry['ansible_versions']}"
    )
    # 'callback' launch_type has job 4 with version: 2.12.0
    assert 'ansible_versions' in callback_entry, 'Should have ansible_versions field in by_launch_type'
    assert callback_entry['ansible_versions'] == ['2.12.0'], (
        f"Expected ['2.12.0'] for callback launch_type, got {callback_entry['ansible_versions']}"
    )

    # ========== Validate Jobs by Ansible Version ==========
    jobs_by_ansible_version_list = result['jobs_by_ansible_version']
    assert isinstance(jobs_by_ansible_version_list, list), 'jobs_by_ansible_version should be a list'
    
    # Expected ansible versions from test data (jobs 1-4 and 6, job 5 is filtered out):
    # Job 1: 2.9.0
    # Job 2: 2.10.0
    # Job 3: 2.11.0
    # Job 4: 2.12.0
    # Job 6: 2.14.0
    # So we should have 5 ansible versions
    assert len(jobs_by_ansible_version_list) == 5, f'Should have 5 ansible versions, got {len(jobs_by_ansible_version_list)}'

    # Find ansible version entries
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

    # Validate '2.9.0' ansible_version (job 1)
    assert version_2_9_0['jobs_total'] == 1, '2.9.0 should have 1 job'
    assert version_2_9_0['jobs_failed_total'] == 0, '2.9.0 should have 0 failed jobs'
    assert version_2_9_0['jobs_succeeded_total'] == 1, '2.9.0 should have 1 succeeded job'
    assert version_2_9_0['job_type_total'] == 1, '2.9.0 should have 1 job type (job)'
    assert version_2_9_0['launch_type_manual_total'] == 1, '2.9.0 should have 1 manual launch type'
    assert version_2_9_0['job_duration_total_seconds'] == pytest.approx(3.0), '2.9.0 should have 3s total duration'
    # Should have default host summary fields
    assert version_2_9_0['unique_hosts_total'] == 0, '2.9.0 should have 0 unique hosts (no job_host_summary merge)'

    # Validate '2.10.0' ansible_version (job 2)
    assert version_2_10_0['jobs_total'] == 1, '2.10.0 should have 1 job'
    assert version_2_10_0['jobs_failed_total'] == 1, '2.10.0 should have 1 failed job'
    assert version_2_10_0['jobs_succeeded_total'] == 0, '2.10.0 should have 0 succeeded jobs'
    assert version_2_10_0['job_type_total'] == 1, '2.10.0 should have 1 job type (job)'
    assert version_2_10_0['launch_type_scheduled_total'] == 1, '2.10.0 should have 1 scheduled launch type'
    assert version_2_10_0['job_duration_total_seconds'] == pytest.approx(5.0), '2.10.0 should have 5s total duration'
    # Should have default host summary fields
    assert version_2_10_0['unique_hosts_total'] == 0, '2.10.0 should have 0 unique hosts (no job_host_summary merge)'

    # Validate '2.11.0' ansible_version (job 3)
    assert version_2_11_0['jobs_total'] == 1, '2.11.0 should have 1 job'
    assert version_2_11_0['jobs_failed_total'] == 0, '2.11.0 should have 0 failed jobs'
    assert version_2_11_0['jobs_succeeded_total'] == 1, '2.11.0 should have 1 succeeded job'
    assert version_2_11_0['job_type_total'] == 1, '2.11.0 should have 1 job type (workflowjob)'
    assert version_2_11_0['launch_type_workflow_total'] == 1, '2.11.0 should have 1 workflow launch type'
    assert version_2_11_0['job_duration_total_seconds'] == pytest.approx(7.0), '2.11.0 should have 7s total duration'
    # Should have default host summary fields
    assert version_2_11_0['unique_hosts_total'] == 0, '2.11.0 should have 0 unique hosts (no job_host_summary merge)'

    # Validate '2.12.0' ansible_version (job 4)
    assert version_2_12_0['jobs_total'] == 1, '2.12.0 should have 1 job'
    assert version_2_12_0['jobs_failed_total'] == 0, '2.12.0 should have 0 failed jobs'
    assert version_2_12_0['jobs_succeeded_total'] == 1, '2.12.0 should have 1 succeeded job'
    assert version_2_12_0['job_type_total'] == 1, '2.12.0 should have 1 job type (job)'
    assert version_2_12_0['launch_type_callback_total'] == 1, '2.12.0 should have 1 callback launch type'
    assert version_2_12_0['job_duration_total_seconds'] == pytest.approx(2.0), '2.12.0 should have 2s total duration'
    # Should have default host summary fields
    assert version_2_12_0['unique_hosts_total'] == 0, '2.12.0 should have 0 unique hosts (no job_host_summary merge)'

    # Validate '2.14.0' ansible_version (job 6)
    assert version_2_14_0['jobs_total'] == 1, '2.14.0 should have 1 job'
    assert version_2_14_0['jobs_failed_total'] == 1, '2.14.0 should have 1 failed job'
    assert version_2_14_0['jobs_succeeded_total'] == 0, '2.14.0 should have 0 succeeded jobs'
    assert version_2_14_0['jobs_never_started_total'] == 1, '2.14.0 should have 1 never started job'
    assert version_2_14_0['job_type_total'] == 1, '2.14.0 should have 1 job type (adhoccommand)'
    assert version_2_14_0['launch_type_scheduled_total'] == 1, '2.14.0 should have 1 scheduled launch type'
    assert version_2_14_0['job_duration_total_seconds'] == pytest.approx(0.0), '2.14.0 should have 0s total duration'
    # Should have default host summary fields
    assert version_2_14_0['unique_hosts_total'] == 0, '2.14.0 should have 0 unique hosts (no job_host_summary merge)'

    # Verify that job_type_total is present (counts distinct job types per ansible_version)
    assert 'job_type_total' in version_2_9_0, 'Should have job_type_total field'
    assert 'job_type_total' in version_2_10_0, 'Should have job_type_total field'
    assert 'job_type_total' in version_2_11_0, 'Should have job_type_total field'
    assert 'job_type_total' in version_2_12_0, 'Should have job_type_total field'
    assert 'job_type_total' in version_2_14_0, 'Should have job_type_total field'

    # Verify that launch_type_*_total fields are present (since we're grouping by ansible_version)
    assert 'launch_type_manual_total' in version_2_9_0, 'Should have launch_type_manual_total field'
    assert 'launch_type_scheduled_total' in version_2_10_0, 'Should have launch_type_scheduled_total field'
    assert 'launch_type_workflow_total' in version_2_11_0, 'Should have launch_type_workflow_total field'
    assert 'launch_type_callback_total' in version_2_12_0, 'Should have launch_type_callback_total field'
    assert 'launch_type_scheduled_total' in version_2_14_0, 'Should have launch_type_scheduled_total field'

    # Verify totals match between all groupings
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in jobs_by_launch_type_list)
    total_jobs_by_ansible_version = sum(j.get('jobs_total', 0) for j in jobs_by_ansible_version_list)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_ansible_version == result['statistics']['jobs_total'], (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_ansible_version={total_jobs_by_ansible_version}, '
        f'statistics={result["statistics"]["jobs_total"]}'
    )


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
        'credentials': [],
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
    assert 'jobs_by_launch_type' in result
    # job_host_summary is now merged into jobs_by_job_type
    assert 'module_stats' in result
    assert 'collection_name_stats' in result
    assert 'modules_used_per_playbook' in result
    assert 'collections_versions' in result

    # Verify statistics contains all fields (with null values for empty data)
    statistics = result['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dict'
    assert 'modules_used_to_automate_total' in statistics
    assert 'hosts_automated_total' in statistics
    assert 'warnings_total' in statistics
    assert 'deprecations_total' in statistics
    assert 'execution_environments_total' in statistics
    assert 'execution_environments_default_total' in statistics
    assert 'execution_environments_custom_total' in statistics
    assert 'jobs_total' in statistics
    assert 'organizations_total' in statistics
    assert 'ansible_version' in statistics
    assert 'forks_total' in statistics
    assert 'unique_hosts_total' in statistics
    assert 'job_host_pairs_total' in statistics
    assert 'playbooks_total' in statistics
    assert 'job_templates_total' in statistics

    # All statistics should be None for empty data (except counts which should be 0)
    assert statistics['modules_used_to_automate_total'] is None
    assert statistics['hosts_automated_total'] is None
    assert statistics['warnings_total'] == 0, f'warnings_total should be 0 for empty data, got {statistics["warnings_total"]}'
    assert statistics['deprecations_total'] == 0, f'deprecations_total should be 0 for empty data, got {statistics["deprecations_total"]}'
    assert statistics['execution_environments_total'] is None
    assert statistics['execution_environments_default_total'] is None
    assert statistics['execution_environments_custom_total'] is None
    assert statistics['jobs_total'] is None
    assert statistics['organizations_total'] is None
    assert statistics['ansible_version'] is None
    assert 'ansible_versions' in statistics, 'Should have ansible_versions field in statistics'
    assert statistics['ansible_versions'] == [], 'ansible_versions should be empty list for empty data'
    assert statistics['forks_total'] is None
    assert statistics['unique_hosts_total'] is None
    # job_host_pairs_total should be 0 (not None) when there's no data, as it represents a count
    assert statistics['job_host_pairs_total'] == 0, f'job_host_pairs_total should be 0 for empty data, got {statistics["job_host_pairs_total"]}'
    # playbooks_total should be 0 (not None) when there's no data, as it represents a count
    assert statistics['playbooks_total'] == 0, f'playbooks_total should be 0 for empty data, got {statistics["playbooks_total"]}'
    # job_templates_total should be None when there's no data (no job_type groups)
    assert statistics['job_templates_total'] is None, f'job_templates_total should be None for empty data, got {statistics["job_templates_total"]}'

    # Verify all arrays are empty
    assert isinstance(result['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    assert len(result['jobs_by_job_type']) == 0, 'jobs_by_job_type should be empty with no data'
    assert isinstance(result['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'
    assert len(result['jobs_by_launch_type']) == 0, 'jobs_by_launch_type should be empty with no data'
    # job_host_summary is now merged into jobs_by_job_type

    assert isinstance(result['module_stats'], list), 'module_stats should be a list'
    assert len(result['module_stats']) == 0, 'module_stats should be empty with no data'

    assert isinstance(result['collection_name_stats'], list), 'collection_name_stats should be a list'
    assert len(result['collection_name_stats']) == 0, 'collection_name_stats should be empty with no data'

    assert isinstance(result['modules_used_per_playbook'], list), 'modules_used_per_playbook should be a list'
    assert len(result['modules_used_per_playbook']) == 0, 'modules_used_per_playbook should be empty with no data'

    assert isinstance(result['collections_versions'], list), 'collections_versions should be a list'
    assert len(result['collections_versions']) == 0, 'collections_versions should be empty with no data'

    # Verify credentials fields are not present in statistics when there's no data
    # (credentials_data would be empty dict, so no credential_type_* fields should exist)
    credential_fields = [k for k in statistics.keys() if k.startswith('credential_type_')]
    assert len(credential_fields) == 0, 'Should have no credential fields in statistics when there is no credentials data'
