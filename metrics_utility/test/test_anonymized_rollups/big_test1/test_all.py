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

import json
import os
import shutil

from datetime import datetime

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.library.storage.segment import StorageSegment

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


def split_data_into_parts(data_list, num_parts):
    """Split a list into roughly equal parts."""
    if not data_list:
        return []

    total = len(data_list)
    items_per_part = total // num_parts
    parts = []

    for i in range(num_parts):
        start = i * items_per_part
        end = (i + 1) * items_per_part if i < num_parts - 1 else total
        parts.append(data_list[start:end])

    return parts


def create_csv_files_from_parts(data_parts, data_dir, filename_template):
    """Create CSV files from data parts and return list of created file paths."""
    csv_files = []
    for i, part in enumerate(data_parts, 1):
        csv_path = f'{data_dir}/{filename_template.format(part_num=i)}'
        csv_file = create_csv_file(part, csv_path)
        if csv_file:
            csv_files.append(csv_file)
    return csv_files


def combine_all_job_data():
    """Combine all job data from job1 through job8."""
    all_jobs = jobs1 + jobs2 + jobs3 + jobs4 + jobs5 + jobs6 + jobs7 + jobs8
    all_events = events1 + events2 + events3 + events4 + events5 + events6 + events7 + events8
    all_jobhostsummary = jhs1 + jhs2 + jhs3 + jhs4 + jhs5 + jhs6 + jhs7 + jhs8
    return all_jobs, all_events, all_jobhostsummary


def create_all_csv_files(data_dir, all_jobs, all_events, all_jobhostsummary):
    """Create all CSV files for the test data."""
    # Jobs: split into 3 parts (3, 3, 2)
    jobs_parts = [all_jobs[:3], all_jobs[3:6], all_jobs[6:]]
    jobs_csv_files = create_csv_files_from_parts(jobs_parts, data_dir, 'part{part_num}_unified_jobs.csv')

    # Events: split into 4 equal parts
    events_parts = split_data_into_parts(all_events, 4)
    events_csv_files = create_csv_files_from_parts(events_parts, data_dir, 'part{part_num}_main_jobevent.csv')

    # Job host summary: split into 3 equal parts
    jhs_parts = split_data_into_parts(all_jobhostsummary, 3)
    jhs_csv_files = create_csv_files_from_parts(jhs_parts, data_dir, 'part{part_num}_job_host_summary.csv')

    # Execution environments: snapshot collector - keep all data in single batch (no splitting)
    ee_csv_file = create_csv_file(execution_environments, f'{data_dir}/execution_environments.csv')
    ee_csv_files = [ee_csv_file] if ee_csv_file else []

    # Credentials: split into 2 parts
    cred_parts = [credentials[:8], credentials[8:]]
    cred_csv_files = create_csv_files_from_parts(cred_parts, data_dir, 'part{part_num}_credentials.csv')

    return {
        'unified_jobs': jobs_csv_files,
        'job_host_summary': jhs_csv_files,
        'main_jobevent': events_csv_files,
        'execution_environments': ee_csv_files,
        'credentials': cred_csv_files,
    }


def save_result_and_chunks(result, since, until, year, month, day):
    """Save the result as JSON and split into chunks."""
    json_content = json.dumps(result, indent=4)
    print('\n' + '=' * 80)
    print('=== ANONYMIZED ROLLUP RESULT (all jobs combined) ===')
    print('=' * 80)
    print(json_content)
    print('=' * 80)

    # Save the result as json
    json_path = f'./out/rollups/{year}/{month:02d}/{day:02d}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d")}.json'
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        print(f'Saving result to {json_path}')
        f.write(json_content)

    # Split data into Segment chunks
    print('\n' + '=' * 80)
    print('=== SPLITTING DATA INTO SEGMENT CHUNKS ===')
    print('=' * 80)

    storage_segment = StorageSegment()
    chunks = storage_segment._split_into_chunks(result, storage_segment.REGULAR_MESSAGE_LIMIT)

    print(f'Total chunks created: {len(chunks)}')
    print(f'Message size limit: {storage_segment.REGULAR_MESSAGE_LIMIT} bytes ({storage_segment.REGULAR_MESSAGE_LIMIT / 1024:.1f} KB)')

    segment_chunks_dir = f'./out/rollups/{year}/{month:02d}/{day:02d}/segment_chunks'
    os.makedirs(segment_chunks_dir, exist_ok=True)

    for i, chunk in enumerate(chunks, 1):
        chunk_size = storage_segment._calculate_size(chunk)
        chunk_json = json.dumps(chunk, indent=4)
        chunk_path = f'{segment_chunks_dir}/chunk_{i:03d}_of_{len(chunks):03d}.json'
        chunk_key = list(chunk.keys())[0] if chunk else 'unknown'

        with open(chunk_path, 'w') as f:
            f.write(chunk_json)

        print(f'Chunk {i}/{len(chunks)}: {chunk_key} - {chunk_size} bytes ({chunk_size / 1024:.1f} KB) - saved to {chunk_path}')

        if isinstance(chunk[chunk_key], list):
            print(f'  └─ Contains {len(chunk[chunk_key])} items in {chunk_key}')

    print('=' * 80)


def test_all_jobs_combined(cleanup_test_data):
    """
    Test that combines all jobs (job1-job8) and validates anonymized rollups.

    Expected data summary:
    - Total jobs: 8
    - Job types: 'job' (7 jobs), 'workflowjob' (1 job)
    - Launch types: 'manual' (5 jobs), 'scheduled' (1 job), 'workflow' (1 job), 'callback' (1 job)
    - Ansible versions: '2.15.0' (3 jobs), '2.16.0' (2 jobs), '2.17.0' (1 job), '2.18.0' (1 job), '2.19.0' (1 job)
    """
    since = datetime(2024, 1, 15, 0, 0, 0)
    until = datetime(2024, 1, 16, 0, 0, 0)
    base_path = './out'
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # Combine all job data and create CSV files
    all_jobs, all_events, all_jobhostsummary = combine_all_job_data()
    input_data = create_all_csv_files(data_dir, all_jobs, all_events, all_jobhostsummary)

    # Run the anonymized rollup computation
    result = compute_anonymized_rollup_from_raw_data(input_data=input_data, salt='test_salt')

    # Save result and split into chunks
    save_result_and_chunks(result, since, until, year, month, day)

    # Validate results
    validate_result_structure(result)
    validate_task_statistics(result['statistics'])
    validate_events_statistics(result['statistics'])

    job_type, workflowjob_type = validate_jobs(result)
    validate_ansible_versions(result)
    validate_scm_types(result)

    jobs_by_launch_type_list = validate_jobs_by_launch_type(result)
    jobs_by_ansible_version_list = validate_jobs_by_ansible_version(result)

    validate_job_host_summary(result, job_type, workflowjob_type)
    validate_module_stats(result)
    validate_collection_stats(result)
    validate_role_stats(result)
    validate_playbooks(result)
    validate_execution_environments(result)
    validate_credentials(result)
    validate_job_totals_match(result, jobs_by_launch_type_list, jobs_by_ansible_version_list)
    validate_jobs_by_controller_version(result)

    print('\n=== All validations passed! ===')


def validate_result_structure(result):
    """Validate that the result has the expected top-level structure."""
    assert 'statistics' in result
    assert 'jobs_by_job_type' in result
    assert 'jobs_by_launch_type' in result
    assert 'jobs_by_ansible_version' in result
    assert 'jobs_by_controller_version' in result
    assert 'module_stats' in result
    assert 'collection_stats' in result
    assert 'collections_versions' in result


def validate_task_statistics(statistics):
    """Validate task statistics from job host summary data."""
    required_fields = [
        'rollup_period_tasks_total',
        'rollup_period_task_ok_total',
        'rollup_period_task_failed_total',
        'rollup_period_task_skipped_total',
        'rollup_period_task_unreachable_total',
        'rollup_period_task_ignored_total',
    ]
    for field in required_fields:
        assert field in statistics

    # Expected totals: ok=98, failures=5, dark=5, skipped=0, ignored=0, tasks_total=108
    assert statistics['rollup_period_task_ok_total'] == 98
    assert statistics['rollup_period_task_failed_total'] == 5
    assert statistics['rollup_period_task_unreachable_total'] == 5
    assert statistics['rollup_period_task_skipped_total'] == 0
    assert statistics['rollup_period_task_ignored_total'] == 0
    assert statistics['rollup_period_tasks_total'] == 108

    # Verify sum matches
    calculated_total = (
        statistics['rollup_period_task_ok_total']
        + statistics['rollup_period_task_failed_total']
        + statistics['rollup_period_task_unreachable_total']
        + statistics['rollup_period_task_skipped_total']
        + statistics['rollup_period_task_ignored_total']
    )
    assert calculated_total == statistics['rollup_period_tasks_total']


def validate_events_statistics(statistics):
    """Validate events statistics."""
    assert statistics['rollup_period_collected_events_total'] == 142
    assert statistics['rollup_period_warnings_total'] == 3
    assert statistics['rollup_period_deprecations_total'] == 2


def validate_jobs(result):
    """Validate job-related data."""
    jobs_list = result['jobs_by_job_type']
    assert isinstance(jobs_list, list)
    assert result['statistics']['rollup_period_jobs_total'] == 8

    # Validate job types: 'job' (7 jobs) and 'workflowjob' (1 job)
    job_type_jobs = [j for j in jobs_list if j['job_type'] == 'job']
    workflowjob_type_jobs = [j for j in jobs_list if j['job_type'] == 'workflowjob']

    assert len(job_type_jobs) == 1
    assert len(workflowjob_type_jobs) == 1

    job_type = job_type_jobs[0]
    assert job_type['jobs_total'] == 7
    assert job_type['job_type'] == 'job'

    workflowjob_type = workflowjob_type_jobs[0]
    assert workflowjob_type['jobs_total'] == 1
    assert workflowjob_type['job_type'] == 'workflowjob'

    return job_type, workflowjob_type


def validate_ansible_versions(result):
    """Validate controller versions at top level."""
    assert 'rollup_period_ansible_versions' in result
    statistics_ansible_versions = result['rollup_period_ansible_versions']
    assert isinstance(statistics_ansible_versions, list)

    expected_versions = ['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0']
    assert len(statistics_ansible_versions) == 5
    for version in expected_versions:
        assert version in statistics_ansible_versions


def validate_scm_types(result):
    """Validate SCM types at top level."""
    assert 'rollup_period_scm_types' in result
    assert result['rollup_period_scm_types'] == ['git', 'manual']


def find_entry_by_key(entries, key, value):
    """Find an entry in a list by a key-value pair."""
    return next((entry for entry in entries if entry.get(key) == value), None)


def validate_jobs_by_launch_type(result):
    """Validate jobs grouped by launch type."""
    jobs_by_launch_type_list = result['jobs_by_launch_type']
    assert isinstance(jobs_by_launch_type_list, list)
    assert len(jobs_by_launch_type_list) == 4

    expected_launch_types = {
        'manual': 5,
        'scheduled': 1,
        'workflow': 1,
        'callback': 1,
    }

    for launch_type, expected_count in expected_launch_types.items():
        entry = find_entry_by_key(jobs_by_launch_type_list, 'launch_type', launch_type)
        assert entry is not None, f'Should have {launch_type} launch_type'
        assert entry['jobs_total'] == expected_count

    return jobs_by_launch_type_list


def validate_jobs_by_ansible_version(result):
    """Validate jobs grouped by controller version."""
    jobs_by_ansible_version_list = result['jobs_by_ansible_version']
    assert isinstance(jobs_by_ansible_version_list, list)
    assert len(jobs_by_ansible_version_list) == 5

    expected_versions = {
        '2.15.0': 3,
        '2.16.0': 2,
        '2.17.0': 1,
        '2.18.0': 1,
        '2.19.0': 1,
    }

    for version, expected_count in expected_versions.items():
        entry = find_entry_by_key(jobs_by_ansible_version_list, 'ansible_version', version)
        assert entry is not None, f'Should have ansible_version {version}'
        assert entry['jobs_total'] == expected_count

    return jobs_by_ansible_version_list


def validate_job_host_summary(result, job_type, workflowjob_type):
    """Validate job host summary data."""
    assert result['statistics']['rollup_period_job_host_pairs_total'] == 32

    # Validate merged host summary fields for 'job' type
    required_fields = ['ok_total', 'failures_total', 'dark_total']
    for field in required_fields:
        assert field in job_type
    assert job_type['ok_total'] > 0
    assert job_type['failures_total'] > 0
    assert job_type['dark_total'] > 0

    # Validate merged host summary fields for 'workflowjob' type
    for field in required_fields:
        assert field in workflowjob_type
    assert workflowjob_type['ok_total'] > 0
    assert workflowjob_type['failures_total'] > 0
    assert workflowjob_type['dark_total'] > 0


def validate_ansible_versions_in_list(items, item_type):
    """Validate that ansible_versions in items are valid."""
    expected_versions = {'2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0'}
    for item in items:
        if 'ansible_versions' in item:
            assert isinstance(item['ansible_versions'], list)
            assert len(item['ansible_versions']) > 0
            for version in item['ansible_versions']:
                assert version in expected_versions, (
                    f'Unexpected ansible_versions {version} in {item_type} {item.get("module_name") or item.get("collection_name")}'
                )


def validate_module_stats(result):
    """Validate module statistics."""
    module_stats = result['module_stats']
    assert isinstance(module_stats, list)
    assert len(module_stats) == 6
    assert result['statistics']['rollup_period_modules_total'] == 6

    required_fields = [
        'module_name',
        'collection_name',
        'jobs_total',
        'unique_hosts_total',
        'processed_events_total',
        'ansible_versions',
    ]

    for module in module_stats:
        for field in required_fields:
            assert field in module
        assert isinstance(module['processed_events_total'], (int, float))
        assert module['processed_events_total'] > 0

    validate_ansible_versions_in_list(module_stats, 'module')


def validate_collection_stats(result):
    """Validate collection statistics."""
    collection_stats = result['collection_stats']
    assert isinstance(collection_stats, list)
    assert len(collection_stats) == 3

    required_fields = [
        'collection_name',
        'collection_source',
        'jobs_total',
        'processed_events_total',
        'ansible_versions',
    ]

    for collection in collection_stats:
        for field in required_fields:
            assert field in collection
        assert isinstance(collection['processed_events_total'], (int, float))
        assert collection['processed_events_total'] > 0

    validate_ansible_versions_in_list(collection_stats, 'collection')


def validate_role_stats(result):
    """Validate role statistics."""
    assert 'role_stats' in result
    role_stats = result['role_stats']
    assert isinstance(role_stats, list)
    assert len(role_stats) > 0

    required_fields = [
        'role',
        'collection_name',
        'collection_source',
        'jobs_total',
        'tasks_total',
        'processed_events_total',
    ]

    for role_stat in role_stats:
        for field in required_fields:
            assert field in role_stat
        assert isinstance(role_stat['processed_events_total'], (int, float))
        assert role_stat['processed_events_total'] > 0

    # Verify that at least one role has a known collection_source
    known_collection_roles = [r for r in role_stats if r.get('collection_source') != 'Custom']
    assert len(known_collection_roles) > 0

    valid_sources = {'certified', 'community', 'validated', 'partner'}
    for role_stat in known_collection_roles:
        assert role_stat['collection_source'] in valid_sources
        assert role_stat['collection_name'] is not None and role_stat['collection_name'] != ''


def validate_playbooks(result):
    """Validate playbook statistics."""
    assert 'rollup_period_playbooks_total' in result['statistics']
    assert result['statistics']['rollup_period_playbooks_total'] >= 2


def validate_execution_environments(result):
    """Validate execution environment statistics."""
    statistics = result['statistics']
    assert statistics['rollup_period_execution_environments_total'] == 8
    assert statistics['rollup_period_EE_default_total'] == 4
    assert statistics['rollup_period_EE_custom_total'] == 4
    assert statistics['rollup_period_execution_environments_total'] == (
        statistics['rollup_period_EE_default_total'] + statistics['rollup_period_EE_custom_total']
    )


def validate_credentials(result):
    """Validate credential types."""
    assert 'rollup_period_credential_types' in result
    credential_types = result['rollup_period_credential_types']
    assert isinstance(credential_types, list)

    expected_types = {
        'Amazon Web Services',
        'Container Registry',
        'Machine',
        'Network',
        'Source Control',
        'Vault',
    }

    assert len(credential_types) == 6
    for cred_type in expected_types:
        assert cred_type in credential_types
    assert credential_types == sorted(credential_types)


def validate_jobs_by_controller_version(result):
    """Validate the jobs_by_controller_version field - single summary item for all jobs."""
    jobs_by_cv = result['jobs_by_controller_version']
    assert isinstance(jobs_by_cv, list)
    assert len(jobs_by_cv) == 1, f'Expected 1 item in jobs_by_controller_version, got {len(jobs_by_cv)}'

    item = jobs_by_cv[0]

    # Jobs counts
    assert item['jobs_total'] == 8
    assert item['jobs_failed_total'] == 5
    assert item['jobs_successful_total'] == 3
    assert item['jobs_never_started_total'] == 0

    # Duration totals
    assert item['jobs_duration_total_seconds'] == 3180.0
    assert item['job_duration_maximum_seconds'] == 480.0
    assert item['job_duration_minimum_seconds'] == 330.0
    assert item['jobs_successful_duration_total_seconds'] == 1110.0
    assert item['jobs_failed_duration_total_seconds'] == 2070.0

    # Waiting times
    assert item['job_waiting_time_total_seconds'] == 240.0
    assert item['job_waiting_time_maximum_seconds'] == 30.0
    assert item['job_waiting_time_minimum_seconds'] == 30.0

    # Deduplication totals
    assert item['templates_total'] == 3
    assert item['inventories_total'] == 1

    # Ansible versions (all 5 versions across all jobs)
    assert 'ansible_versions' in item
    assert sorted(item['ansible_versions']) == ['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0']

    # controller_version is None since no controller_version data is provided in this test
    assert item.get('controller_version') is None


def validate_job_totals_match(result, jobs_by_launch_type_list, jobs_by_ansible_version_list):
    """Verify that job totals match across all groupings."""
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in jobs_by_launch_type_list)
    total_jobs_by_ansible_version = sum(j.get('jobs_total', 0) for j in jobs_by_ansible_version_list)
    expected_total = result['statistics']['rollup_period_jobs_total']

    assert total_jobs_by_job_type == expected_total
    assert total_jobs_by_launch_type == expected_total
    assert total_jobs_by_ansible_version == expected_total
