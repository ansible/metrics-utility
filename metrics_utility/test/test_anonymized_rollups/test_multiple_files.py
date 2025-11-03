"""
This test verifies that data split into multiple tarballs is correctly concatenated.
It tests the logic for loading and merging dataframes from multiple tarball files.

The test:
1. Takes data from other test files (jobs, events, execution_environments, jobhostsummary)
2. Splits each dataset into 2-3 separate tarballs
3. Creates tarball files with CSV data inside
4. Tests that compute_anonymized_rollup_from_raw_data properly loads and concatenates the data
5. Validates the final output matches expected aggregated results
"""

import os
import shutil
import tarfile

from io import BytesIO

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


def create_tarball_with_csv(data_list, csv_filename, tarball_path):
    """
    Create a tarball containing a single CSV file.

    Args:
        data_list: List of dictionaries to convert to CSV
        csv_filename: Name of the CSV file inside the tarball
        tarball_path: Path where to save the tarball
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(tarball_path), exist_ok=True)

    # Skip creating tarballs for empty data
    if not data_list:
        return

    # Convert list of dicts to DataFrame then to CSV
    df = pd.DataFrame(data_list)
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Create tarball with the CSV file inside
    with tarfile.open(tarball_path, 'w:gz') as tar:
        tarinfo = tarfile.TarInfo(name=csv_filename)
        tarinfo.size = len(csv_buffer.getvalue())
        tar.addfile(tarinfo, csv_buffer)


def test_multiple_tarballs_concatenation(cleanup_test_data):
    """
    Test that multiple tarballs are properly concatenated and aggregated.

    This test splits the test data into multiple tarballs (2-3 parts each)
    and verifies that the concatenation logic works correctly.
    """
    base_path = './out'
    year, month, day = 2025, 6, 13
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # ========== Split and create tarball files for each collector ==========

    # 1. Jobs data - split into 2 tarballs
    jobs_part1 = jobs[:3]  # First 3 jobs
    jobs_part2 = jobs[3:]  # Remaining jobs

    create_tarball_with_csv(jobs_part1, 'unified_jobs.csv', f'{data_dir}/tarball1_unified_jobs.tar.gz')
    create_tarball_with_csv(jobs_part2, 'unified_jobs.csv', f'{data_dir}/tarball2_unified_jobs.tar.gz')

    # 2. Events data - split into 3 tarballs
    # Note: collector name is 'main_jobevent_service'
    events_part1 = events[:100]  # First 100 events
    events_part2 = events[100:200]  # Middle events
    events_part3 = events[200:]  # Remaining events

    create_tarball_with_csv(events_part1, 'main_jobevent_service.csv', f'{data_dir}/tarball1_main_jobevent_service.tar.gz')
    create_tarball_with_csv(events_part2, 'main_jobevent_service.csv', f'{data_dir}/tarball2_main_jobevent_service.tar.gz')
    create_tarball_with_csv(events_part3, 'main_jobevent_service.csv', f'{data_dir}/tarball3_main_jobevent_service.tar.gz')

    # 3. Execution environments - split into 2 tarballs
    ee_part1 = execution_environments[:2]
    ee_part2 = execution_environments[2:]

    create_tarball_with_csv(ee_part1, 'execution_environments.csv', f'{data_dir}/tarball1_execution_environments.tar.gz')
    create_tarball_with_csv(ee_part2, 'execution_environments.csv', f'{data_dir}/tarball2_execution_environments.tar.gz')

    # 4. Job host summary - split into 2 tarballs
    # Note: collector name is 'job_host_summary_service'
    jhs_part1 = jobhostsummary[:8]  # First 8 entries
    jhs_part2 = jobhostsummary[8:]  # Remaining entries

    create_tarball_with_csv(jhs_part1, 'job_host_summary_service.csv', f'{data_dir}/tarball1_job_host_summary_service.tar.gz')
    create_tarball_with_csv(jhs_part2, 'job_host_summary_service.csv', f'{data_dir}/tarball2_job_host_summary_service.tar.gz')

    # ========== Run the anonymized rollup computation ==========

    result = compute_anonymized_rollup_from_raw_data(salt='test_salt', year=year, month=month, day=day, base_path=base_path, save_rollups=False)

    # print the result with pretty json
    import json

    json_content = json.dumps(result, indent=4)
    print(json_content)

    # save the result as json inside rollups/2025/06/13/anonymized.json - based on the year, month, day
    json_path = f'./out/rollups/{year}/{month:02d}/{day:02d}/anonymized.json'

    # ensure the directory exists
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        print(f'Saving result to {json_path}')
        # write result as json to file
        f.write(json_content)

    # ========== Validate the results ==========

    # Validate structure
    assert 'jobs' in result
    assert 'job_host_summary' in result
    assert 'execution_environments' in result
    assert 'events_modules' in result

    # ========== Validate Jobs ==========
    jobs_list = result['jobs']
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 3  # T1, T2, T3

    # T1 should have data from both tarballs (jobs 1, 2, 4)
    t1_jobs = [j for j in jobs_list if j['number_of_jobs_executed'] == 3]
    assert len(t1_jobs) == 1
    t1 = t1_jobs[0]
    assert t1['number_of_jobs_executed'] == 3
    assert t1['number_of_jobs_failed'] == 1
    assert t1['number_of_jobs_succeeded'] == 2

    # ========== Validate Execution Environments ==========
    ee_result = result['execution_environments']
    assert ee_result['total_EE'] == 5
    assert ee_result['default_EE'] == 2
    assert ee_result['custom_EE'] == 3

    # ========== Validate Job Host Summary ==========
    jhs_list = result['job_host_summary']
    assert isinstance(jhs_list, list)
    assert len(jhs_list) == 2  # T1 and T2

    # Verify data was concatenated from both tarballs
    # verify number of ok, failures, skipped, ignored, rescued, dark for each template
    assert jhs_list[0]['ok_total'] == 26
    assert jhs_list[0]['failures_total'] == 2
    assert jhs_list[0]['skipped_total'] == 2
    assert jhs_list[0]['ignored_total'] == 0
    assert jhs_list[0]['rescued_total'] == 0
    assert jhs_list[0]['dark_total'] == 0

    assert jhs_list[1]['ok_total'] == 26
    assert jhs_list[1]['failures_total'] == 4
    assert jhs_list[1]['skipped_total'] == 0
    assert jhs_list[1]['ignored_total'] == 0
    assert jhs_list[1]['rescued_total'] == 0
    assert jhs_list[1]['dark_total'] == 0

    # ========== Validate Events Modules ==========
    events_modules = result['events_modules']
    assert isinstance(events_modules, dict), 'events_modules should be a dictionary'

    # Assert required keys are present
    assert 'modules_used_to_automate_total' in events_modules, "Missing 'modules_used_to_automate_total'"
    assert 'list_of_modules_used_to_automate' in events_modules, "Missing 'list_of_modules_used_to_automate'"
    assert 'module_stats' in events_modules, "Missing 'module_stats'"
    assert 'collection_name_stats' in events_modules, "Missing 'collection_name_stats'"
    assert 'total_hosts_automated' in events_modules, "Missing 'total_hosts_automated'"

    # Verify values from concatenated data across 3 tarballs
    assert events_modules['modules_used_to_automate_total'] == 7, 'Should have 7 unique modules from all tarballs'
    assert events_modules['total_hosts_automated'] == 9, 'Should have 9 unique hosts from all tarballs'

    # Verify module stats have data from all tarballs
    module_stats = events_modules['module_stats']
    assert isinstance(module_stats, list), 'module_stats should be a list'
    assert len(module_stats) == 7, 'Should have stats for all 7 modules'

    # Verify collection stats
    collection_stats = events_modules['collection_name_stats']
    assert isinstance(collection_stats, list), 'collection_name_stats should be a list'
    assert len(collection_stats) == 7, 'Should have stats for all 7 collections'

    # ========== Validate Anonymization ==========
    # Check that job template names are hashed (128 character hex strings)
    for job in jobs_list:
        template_name = job['job_template_name']
        assert len(template_name) == 128, f'Template name should be hashed: {template_name}'
        assert all(c in '0123456789abcdef' for c in template_name), 'Template name should be hex'

    for jhs_item in jhs_list:
        template_name = jhs_item['job_template_name']
        assert len(template_name) == 128, f'Template name should be hashed: {template_name}'
        assert all(c in '0123456789abcdef' for c in template_name), 'Template name should be hex'


def test_empty_tarballs_handling(cleanup_test_data):
    """
    Test that the system handles case with no tarball files gracefully.
    """
    base_path = './out'
    year, month, day = 2025, 6, 14
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # Create the directory but don't create any tarball files
    # This simulates a scenario where no data was collected
    os.makedirs(data_dir, exist_ok=True)

    # Should not crash, but return empty/default results
    result = compute_anonymized_rollup_from_raw_data(salt='test_salt', year=year, month=month, day=day, base_path=base_path, save_rollups=False)

    # Validate structure exists even with empty data
    assert 'jobs' in result
    assert 'job_host_summary' in result
    assert 'execution_environments' in result
    assert 'events_modules' in result
