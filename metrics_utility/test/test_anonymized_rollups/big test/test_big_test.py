"""
Test for consistent job data from job1.py.

This test:
1. Loads data from job1.py (single job with consistent job host summaries and events)
2. Splits the data into multiple CSV files to test concatenation
3. Computes expected results based on input data (not output)
4. Validates that the anonymized rollup matches expected values
"""

import os
import shutil

from datetime import datetime

import pandas as pd
import pytest

import sys
import os

# Add the current directory to the path to import job1
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from job1 import job, job_hostsummaries, events


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


def test_big_test_job1(cleanup_test_data):
    """
    Test anonymized rollup computation for job1.py data.
    
    This test computes expected values from the input data and validates
    that the anonymized rollup matches those expectations.
    """
    
    # ========== Compute Expected Values from Input Data ==========
    
    # Job data analysis
    expected_job = job
    expected_job_duration = 30.0  # seconds (finished - started)
    expected_job_waiting_time = 5.0  # seconds (started - created)
    expected_job_failed = True  # failed = 1
    expected_job_model = 'job'
    expected_job_launch_type = 'manual'
    expected_job_ansible_version = '2.9.0'
    expected_job_template_name = 'T1'
    expected_job_scm_type = 'git'
    expected_job_inventory_name = 'inventory1'
    expected_job_organization_name = 'Org1'
    
    # Collections from installed_collections
    import json
    installed_collections = json.loads(expected_job['installed_collections'])
    expected_collections = [
        {'name': 'ansible.builtin', 'version': '2.9.10', 'job_count': 1},
        {'name': 'ansible.windows', 'version': '1.0.0', 'job_count': 1},
        {'name': 'ansible.netcommon', 'version': '1.0.0', 'job_count': 1},
        {'name': 'community.general', 'version': '1.0.0', 'job_count': 1},
    ]
    
    # Job host summary totals
    expected_total_ok = sum(h['ok'] for h in job_hostsummaries)  # 3 + 3 + 2 + 1 = 9
    expected_total_failures = sum(h['failures'] for h in job_hostsummaries)  # 0 + 1 + 0 + 1 = 2
    expected_total_skipped = sum(h['skipped'] for h in job_hostsummaries)  # 0 + 0 + 1 + 0 = 1
    expected_total_dark = sum(h['dark'] for h in job_hostsummaries)  # 0 + 0 + 0 + 1 = 1
    expected_unique_hosts = len(job_hostsummaries)  # 4 hosts
    expected_job_host_pairs = len(job_hostsummaries)  # 4 job-host pairs
    
    # Events analysis
    expected_total_events = len(events)  # 13 events
    
    # Extract unique modules and collections from events
    unique_modules = set()
    unique_collections = set()
    unique_hosts_from_events = set()
    playbooks = set()
    
    for event in events:
        if event.get('task_action'):
            unique_modules.add(event['task_action'])
            # Extract collection name (first two parts: namespace.collection)
            parts = event['task_action'].split('.')
            if len(parts) >= 2:
                unique_collections.add(f"{parts[0]}.{parts[1]}")
        if event.get('host_id'):
            unique_hosts_from_events.add(event['host_id'])
        if event.get('playbook'):
            playbooks.add(event['playbook'])
    
    expected_unique_modules = len(unique_modules)  # 4 modules
    expected_unique_collections = len(unique_collections)  # 4 collections
    expected_unique_hosts_from_events = len(unique_hosts_from_events)  # 4 hosts
    expected_playbooks = len(playbooks)  # 1 playbook
    
    # Module statistics from events
    # Group events by module to compute stats
    module_stats = {}
    for event in events:
        if not event.get('task_action'):
            continue
        module = event['task_action']
        if module not in module_stats:
            module_stats[module] = {
                'jobs': set(),
                'hosts': set(),
                'clean_success': 0,
                'success_with_reruns': 0,
                'failed': 0,
                'skipped': 0,
                'unreachable': 0,
            }
        
        module_stats[module]['jobs'].add(event['job_id'])
        if event.get('host_id'):
            module_stats[module]['hosts'].add(event['host_id'])
        
        event_type = event['event']
        if event_type == 'runner_on_ok':
            # Check if this task had a failure before (rerun)
            # For t001 on h2, we have both failed and ok events
            task_uuid = event.get('task_uuid')
            host_id = event.get('host_id')
            # Check if there's a failed event for same task_uuid and host_id
            has_failure = any(
                e.get('task_uuid') == task_uuid and 
                e.get('host_id') == host_id and 
                e.get('event') in ['runner_on_failed', 'runner_on_async_failed', 'runner_item_on_failed']
                for e in events
            )
            if has_failure:
                module_stats[module]['success_with_reruns'] += 1
            else:
                module_stats[module]['clean_success'] += 1
        elif event_type in ['runner_on_failed', 'runner_on_async_failed', 'runner_item_on_failed']:
            # Check if this failure is followed by a success (rerun)
            task_uuid = event.get('task_uuid')
            host_id = event.get('host_id')
            has_success_after = any(
                e.get('task_uuid') == task_uuid and 
                e.get('host_id') == host_id and 
                e.get('event') in ['runner_on_ok', 'runner_on_async_ok', 'runner_item_on_ok'] and
                events.index(e) > events.index(event)
                for e in events
            )
            if not has_success_after:
                module_stats[module]['failed'] += 1
        elif event_type in ['runner_on_skipped', 'runner_item_on_skipped']:
            module_stats[module]['skipped'] += 1
        elif event_type in ['runner_on_unreachable', 'runner_item_on_unreachable']:
            module_stats[module]['unreachable'] += 1
    
    # ========== Setup Test Data ==========
    
    since = datetime(2024, 1, 1, 0, 0, 0)
    until = datetime(2024, 1, 2, 0, 0, 0)
    
    base_path = './out'
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'
    
    # Split data into multiple CSV files to test concatenation
    # Jobs: 1 job, split into 1 file (but we can still test with 1 file)
    jobs_csv_files = []
    csv1 = create_csv_file([job], f'{data_dir}/part1_unified_jobs.csv')
    if csv1:
        jobs_csv_files.append(csv1)
    
    # Job host summaries: 4 entries, split into 2 files
    jhs_part1 = job_hostsummaries[:2]  # First 2 hosts
    jhs_part2 = job_hostsummaries[2:]  # Remaining 2 hosts
    
    jhs_csv_files = []
    csv1 = create_csv_file(jhs_part1, f'{data_dir}/part1_job_host_summary.csv')
    if csv1:
        jhs_csv_files.append(csv1)
    csv2 = create_csv_file(jhs_part2, f'{data_dir}/part2_job_host_summary.csv')
    if csv2:
        jhs_csv_files.append(csv2)
    
    # Events: 13 events, split into 2 files
    events_part1 = events[:7]  # First 7 events
    events_part2 = events[7:]  # Remaining 6 events
    
    events_csv_files = []
    csv1 = create_csv_file(events_part1, f'{data_dir}/part1_main_jobevent.csv')
    if csv1:
        events_csv_files.append(csv1)
    csv2 = create_csv_file(events_part2, f'{data_dir}/part2_main_jobevent.csv')
    if csv2:
        events_csv_files.append(csv2)
    
    # Empty lists for execution_environments and credentials (not in job1.py)
    ee_csv_files = []
    cred_csv_files = []
    
    # ========== Run the anonymized rollup computation ==========
    
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
    print('=== ANONYMIZED ROLLUP RESULT (from job1.py) ===')
    print('=' * 80)
    print(json_content)
    print('=' * 80)
    
    # ========== Validate Results Based on Input Data ==========
    
    # Validate structure
    assert 'statistics' in result
    assert 'jobs_by_job_type' in result
    assert 'jobs_by_launch_type' in result
    assert 'module_stats' in result
    assert 'collection_name_stats' in result
    assert 'modules_used_per_playbook' in result
    assert 'collections_versions' in result
    
    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 1  # Only 'job' type
    
    job_type_entry = jobs_list[0]
    assert job_type_entry['job_type'] == expected_job_model
    assert job_type_entry['jobs_total'] == 1
    assert job_type_entry['jobs_failed_total'] == 1  # Job failed
    assert job_type_entry['jobs_succeeded_total'] == 0
    assert job_type_entry['jobs_never_started_total'] == 0
    assert job_type_entry['job_duration_total_seconds'] == pytest.approx(expected_job_duration)
    assert job_type_entry['job_duration_minimum_seconds'] == pytest.approx(expected_job_duration)
    assert job_type_entry['job_duration_maximum_seconds'] == pytest.approx(expected_job_duration)
    assert job_type_entry['job_waiting_time_total_seconds'] == pytest.approx(expected_job_waiting_time)
    assert job_type_entry['job_waiting_time_minimum_seconds'] == pytest.approx(expected_job_waiting_time)
    assert job_type_entry['job_waiting_time_maximum_seconds'] == pytest.approx(expected_job_waiting_time)
    assert job_type_entry['templates_total'] == 1  # Template T1
    assert job_type_entry['ansible_versions'] == [expected_job_ansible_version]
    
    # Validate statistics
    assert result['statistics']['jobs_total'] == 1
    assert result['statistics']['job_templates_total'] == 1
    assert result['statistics']['ansible_versions'] == [expected_job_ansible_version]
    # SCM type is in jobs_by_job_type, not statistics
    assert job_type_entry['jobs_using_scm_type_git_total'] == 1
    
    # ========== Validate Job Host Summary ==========
    assert result['statistics']['unique_hosts_total'] == expected_unique_hosts
    assert result['statistics']['job_host_pairs_total'] == expected_job_host_pairs
    
    # Validate merged host summary fields in job_type
    assert job_type_entry['unique_hosts_total'] == expected_unique_hosts
    assert job_type_entry['ok_total'] == expected_total_ok
    assert job_type_entry['failures_total'] == expected_total_failures
    assert job_type_entry['skipped_total'] == expected_total_skipped
    assert job_type_entry['dark_total'] == expected_total_dark
    
    # ========== Validate Events Modules ==========
    assert result['statistics']['modules_used_to_automate_total'] == expected_unique_modules
    assert result['statistics']['hosts_automated_total'] == expected_unique_hosts_from_events
    assert result['statistics']['playbooks_total'] == expected_playbooks
    
    # Validate module stats
    module_stats_result = result['module_stats']
    assert isinstance(module_stats_result, list)
    assert len(module_stats_result) == expected_unique_modules
    
    # Validate specific modules exist (some may be anonymized/hashed)
    module_names = [m['module_name'] for m in module_stats_result]
    # Certified and community collections are not anonymized
    assert 'ansible.windows.win_copy' in module_names
    assert 'community.general.yum' in module_names
    assert 'ansible.netcommon.cli_config' in module_names
    # ansible.builtin.copy may be anonymized, so we just check we have 4 modules total
    # and verify the ones we can identify
    
    # Validate collection stats
    collection_stats = result['collection_name_stats']
    assert isinstance(collection_stats, list)
    assert len(collection_stats) == expected_unique_collections
    
    # Validate collections_versions
    collections_versions = result['collections_versions']
    assert isinstance(collections_versions, list)
    assert len(collections_versions) == len(expected_collections)
    
    # Verify each expected collection is present
    collections_dict = {
        (c['name'], c['version']): c['job_count']
        for c in collections_versions
    }
    for expected_col in expected_collections:
        assert collections_dict.get((expected_col['name'], expected_col['version'])) == expected_col['job_count'], (
            f"Expected {expected_col['name']} {expected_col['version']} with job_count {expected_col['job_count']}"
        )
    
    # ========== Validate Jobs by Launch Type ==========
    jobs_by_launch_type_list = result['jobs_by_launch_type']
    assert isinstance(jobs_by_launch_type_list, list)
    assert len(jobs_by_launch_type_list) == 1  # Only 'manual' launch type
    
    launch_type_entry = jobs_by_launch_type_list[0]
    assert launch_type_entry['launch_type'] == expected_job_launch_type
    assert launch_type_entry['jobs_total'] == 1
    assert launch_type_entry['jobs_failed_total'] == 1
    assert launch_type_entry['jobs_succeeded_total'] == 0
    assert launch_type_entry['job_type_total'] == 1
    assert launch_type_entry['ansible_versions'] == [expected_job_ansible_version]
    
    # ========== Validate Jobs by Ansible Version ==========
    jobs_by_ansible_version_list = result['jobs_by_ansible_version']
    assert isinstance(jobs_by_ansible_version_list, list)
    assert len(jobs_by_ansible_version_list) == 1  # Only '2.9.0' version
    
    ansible_version_entry = jobs_by_ansible_version_list[0]
    assert ansible_version_entry['ansible_version'] == expected_job_ansible_version
    assert ansible_version_entry['jobs_total'] == 1
    assert ansible_version_entry['jobs_failed_total'] == 1
    assert ansible_version_entry['jobs_succeeded_total'] == 0
    assert ansible_version_entry['job_type_total'] == 1
    assert ansible_version_entry['launch_type_manual_total'] == 1
    assert ansible_version_entry['ansible_versions'] == [expected_job_ansible_version]
    
    # ========== Validate Modules Used Per Playbook ==========
    playbook_modules = result['modules_used_per_playbook']
    assert isinstance(playbook_modules, list)
    assert len(playbook_modules) == expected_playbooks
    
    # Verify totals match
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in result['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in jobs_by_launch_type_list)
    total_jobs_by_ansible_version = sum(j.get('jobs_total', 0) for j in jobs_by_ansible_version_list)
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_ansible_version == result['statistics']['jobs_total'], (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_ansible_version={total_jobs_by_ansible_version}, '
        f'statistics={result["statistics"]["jobs_total"]}'
    )
    
    print('\n✅ All assertions passed!')
