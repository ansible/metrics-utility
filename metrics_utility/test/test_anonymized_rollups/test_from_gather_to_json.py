import json
import os
import shutil

from datetime import datetime

import pytest

from django.db import connection

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.anonymized_rollups.compute_anonymized_rollup import compute_anonymized_rollup


# where to find the tar.gz (match jobhostsummary test layout)


@pytest.fixture
def cleanup_glob():
    out_dir = './out'

    # --- Cleanup before test ---
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    yield  # Run your test

    # --- Cleanup after test ---
    # if os.path.exists(out_dir):
    #    shutil.rmtree(out_dir)


def test_empty_data(cleanup_glob):
    # since = begining of the day
    # until = begining of the next day
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)

    compute_anonymized_rollup_from_raw_data(
        {'unified_jobs': [], 'job_host_summary': [], 'main_jobevent': [], 'execution_environments': []}, 'salt', since, until, './out'
    )


def test_from_gather_to_json(cleanup_glob):
    # since = begining of the day
    # until = begining of the next day
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)

    # runher
    # here what the connection should be? The postgres is in docker compose
    db = connection
    json_data = compute_anonymized_rollup(db, 'salt', since, until, './out', save_rollups=False)

    print(json_data)

    # save as json inside rollups/2025/06/13/anonymized.json
    json_path = f'./out/rollups/{since.year}/{since.month}/{since.day}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d")}.json'

    # create the dir
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=4)

    # ========== Validate the json_data that are containing what they should ==========

        # Validate top-level flattened structure
        assert 'statistics' in json_data, "Missing 'statistics' in json_data"
        assert 'module_stats' in json_data, "Missing 'module_stats' in json_data"
        assert 'collection_name_stats' in json_data, "Missing 'collection_name_stats' in json_data"
        assert 'modules_used_per_playbook' in json_data, "Missing 'modules_used_per_playbook' in json_data"
        assert 'jobs_by_job_type' in json_data, "Missing 'jobs_by_job_type' in json_data"
        # job_host_summary is now merged into jobs_by_job_type

    # Validate statistics structure (contains all the scalar totals)
    statistics = json_data['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dictionary'
    assert 'modules_used_to_automate_total' in statistics
    assert 'hosts_automated_total' in statistics
    assert 'execution_environments_total' in statistics
    assert 'execution_environments_default_total' in statistics
    assert 'execution_environments_custom_total' in statistics
    assert 'jobs_total' in statistics
    assert 'organizations_total' in statistics
    assert 'ansible_version' in statistics
    assert 'unique_hosts_total' in statistics

    # Validate statistics data types
    assert isinstance(statistics['modules_used_to_automate_total'], int)
    assert isinstance(statistics['hosts_automated_total'], int)
    assert isinstance(statistics['execution_environments_total'], int)
    assert isinstance(statistics['execution_environments_default_total'], int)
    assert isinstance(statistics['execution_environments_custom_total'], int)
    assert isinstance(statistics['jobs_total'], int)
    assert isinstance(statistics['unique_hosts_total'], int)

    # Validate arrays structure
    assert isinstance(json_data['modules_used_per_playbook'], list), 'modules_used_per_playbook should be a list'
    assert isinstance(json_data['module_stats'], list), 'module_stats should be a list'
    assert isinstance(json_data['collection_name_stats'], list), 'collection_name_stats should be a list'
    assert isinstance(json_data['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'

    # Validate module_stats have required fields
    if json_data['module_stats']:
        for module_stat in json_data['module_stats']:
            assert 'module_name' in module_stat
            assert 'collection_source' in module_stat
            assert 'collection_name' in module_stat
            assert 'jobs_total' in module_stat
            assert 'hosts_total' in module_stat

    # Validate jobs_by_job_type have required fields (now grouped by job_type, merged with job_host_summary)
    if json_data['jobs_by_job_type']:
        for job in json_data['jobs_by_job_type']:
            assert 'job_type' in job
            assert 'jobs_total' in job
            assert 'jobs_failed_total' in job
            assert 'jobs_succeeded_total' in job
            assert 'templates_total' in job
            # Host summary fields (merged from job_host_summary)
            assert 'dark_total' in job
            assert 'failures_total' in job
            assert 'ok_total' in job
            assert 'skipped_total' in job
            assert 'ignored_total' in job
            assert 'rescued_total' in job
            assert 'unique_hosts_total' in job

    # ========== Validate actual data values and relationships ==========

    # Validate statistics actual values
    print('\n--- Validating statistics data values ---')
    assert statistics['modules_used_to_automate_total'] == 1, 'Should have 1 module'
    assert statistics['hosts_automated_total'] == 2, 'Should have 2 hosts automated'
    assert len(json_data['module_stats']) == 1, 'Should have 1 module stats'
    assert len(json_data['collection_name_stats']) == 1, 'Should have 1 collection stats'

    # Validate module_stats actual values
    print('--- Validating module_stats data values ---')
    first_module_stats = json_data['module_stats'][0]
    assert first_module_stats['module_name'] == 'a10.acos_axapi.a10_slb_virtual_server', 'Module stats should match module'
    assert first_module_stats['jobs_total'] == 3, 'Should have 3 jobs using this module'
    assert first_module_stats['hosts_total'] == 2, 'Should have 2 hosts for this module'
    assert first_module_stats['task_clean_success_total'] == 6, 'Should have 6 successful tasks (3 jobs × 2 hosts)'
    assert first_module_stats['task_success_with_reruns_total'] == 0, 'Should have 0 reruns'
    assert first_module_stats['task_failed_total'] == 0, 'Should have 0 failures'

    # Validate collection_name_stats
    print('--- Validating collection_name_stats data values ---')
    first_collection_stats = json_data['collection_name_stats'][0]
    assert first_collection_stats['collection_name'] == 'a10.acos_axapi', 'Collection name should match'
    assert first_collection_stats['collection_source'] == 'community', 'Collection should be from community'
    assert first_collection_stats['jobs_total'] == 3, 'Collection should have 3 jobs'
    assert first_collection_stats['hosts_total'] == 2, 'Collection should have 2 hosts'
    assert first_collection_stats['task_clean_success_total'] == 6, 'Collection should have 6 successful tasks'

    # Validate modules_used_per_playbook structure and values (now an array, not dict)
    print('--- Validating modules_used_per_playbook ---')
    assert len(json_data['modules_used_per_playbook']) == 1, 'Should have 1 playbook'
    playbook_entry = json_data['modules_used_per_playbook'][0]
    assert 'playbook_id' in playbook_entry, 'Playbook entry should have playbook_id'
    assert 'modules_used' in playbook_entry, 'Playbook entry should have modules_used'
    assert playbook_entry['modules_used'] == 1, 'Playbook should use 1 module'


    # Validate execution_environments actual values
    print('--- Validating execution_environments data values ---')
    assert statistics['execution_environments_total'] == 2, 'Should have 2 total execution environments'
    assert statistics['execution_environments_default_total'] == 1, 'Should have 1 default execution environment'
    assert statistics['execution_environments_custom_total'] == 1, 'Should have 1 custom execution environment'
    # Validate that total = default + custom
    assert statistics['execution_environments_total'] == statistics['execution_environments_default_total'] + statistics['execution_environments_custom_total'], 'Total EE should equal default + custom'

    # Validate jobs actual values
    print('--- Validating jobs data values ---')
    assert statistics['jobs_total'] == 3, 'Should have 3 total jobs'
    assert len(json_data['jobs_by_job_type']) == 1, 'Should have 1 job_type group'
    job = json_data['jobs_by_job_type'][0]
    assert job['jobs_total'] == 3, 'Job type should have 3 jobs'
    assert job['jobs_failed_total'] == 0, 'Should have 0 failed jobs'
    assert job['jobs_succeeded_total'] == 3, 'Should have 3 succeeded jobs'
    assert job['jobs_succeeded_total'] + job['jobs_failed_total'] == job['jobs_total'], (
        'Succeeded + failed should equal total'
    )
    # job_type should be 'job' from django_content_type.model
    assert job['job_type'] == 'job', f"Expected job_type to be 'job', but got {job['job_type']}"

    # Validate job duration fields are non-negative
    assert job['job_duration_total_seconds'] >= 0, 'Job duration total should be non-negative'
    assert job['job_duration_maximum_seconds'] >= job['job_duration_minimum_seconds'], 'Max duration should be >= min duration'

    # Validate job waiting time fields are non-negative
    assert job['job_waiting_time_total_seconds'] >= 0, 'Job waiting time total should be non-negative'

    # Validate job_host_summary data merged into jobs_by_job_type
    print('--- Validating job_host_summary data values (merged into jobs_by_job_type) ---')
    assert statistics['unique_hosts_total'] == 2, 'Should have 2 unique hosts'

    # Find the job entry with job_type='job'
    job_entry = next((j for j in json_data['jobs_by_job_type'] if j.get('job_type') == 'job'), None)
    assert job_entry is not None, 'Should have job_type job in jobs_by_job_type'

    # Validate job_host_summary fields are merged into jobs_by_job_type
    assert job_entry['ok_total'] == 6, 'Should have 6 ok tasks'
    assert job_entry['failures_total'] == 0, 'Should have 0 failures'
    assert job_entry['dark_total'] == 0, 'Should have 0 dark (unreachable) hosts'
    assert job_entry['skipped_total'] == 0, 'Should have 0 skipped tasks'
    assert job_entry['unique_hosts_total'] == 2, 'Should have 2 unique hosts'

    # Validate cross-section data consistency
    print('--- Validating cross-section data consistency ---')
    # Validate that module stats hosts match the total automated hosts
    for module_stat in json_data['module_stats']:
        assert module_stat['hosts_total'] <= statistics['hosts_automated_total'], (
            f'Module {module_stat["module_name"][:50]} hosts should not exceed total automated hosts'
        )

    print('✅ All data value assertions passed!')


def test_half_day_rollup(cleanup_glob):
    """Test with half-day time range: from midnight to noon"""
    # since = beginning of the day
    # until = half of the day (noon)
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 13, 12, 0, 0)

    # Get the data from the database
    db = connection
    json_data = compute_anonymized_rollup(db, 'salt', since, until, './out', save_rollups=False)

    print('\n========== Half-Day Rollup JSON Data ==========')
    print(json.dumps(json_data, indent=4))
    print('================================================\n')

    # Save as json for inspection
    json_path = (
        f'./out/rollups/{since.year}/{since.month}/{since.day}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d-%H-%M")}.json'
    )

    # Create the directory
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=4)

    print(f'JSON saved to: {json_path}')

    # Basic assertions - just validate structure
    assert 'statistics' in json_data, "Missing 'statistics' in json_data"
    assert 'module_stats' in json_data, "Missing 'module_stats' in json_data"
    assert 'collection_name_stats' in json_data, "Missing 'collection_name_stats' in json_data"
    assert 'modules_used_per_playbook' in json_data, "Missing 'modules_used_per_playbook' in json_data"
    assert 'jobs_by_job_type' in json_data, "Missing 'jobs_by_job_type' in json_data"
    # job_host_summary is now merged into jobs_by_job_type

    # Validate basic types
    assert isinstance(json_data['statistics'], dict), 'statistics should be a dictionary'
    assert isinstance(json_data['module_stats'], list), 'module_stats should be a list'
    assert isinstance(json_data['collection_name_stats'], list), 'collection_name_stats should be a list'
    assert isinstance(json_data['modules_used_per_playbook'], list), 'modules_used_per_playbook should be a list'
    assert isinstance(json_data['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    # job_host_summary is now merged into jobs_by_job_type

    print('✅ Basic structure assertions passed!')
