import json
import os
import shutil

import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.anonymized_rollups.task_anonymized_rollups import task_anonymized_rollups


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
    compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13, './out')


def test_from_gather_to_json(cleanup_glob):
    # run gather
    json_data = task_anonymized_rollups('salt', 2025, 6, 13, './out', save_rollups=False)

    print(json_data)

    # save as json inside rollups/2025/06/13/anonymized.json
    json_path = f'./out/rollups/{2025}/06/13/anonymized.json'

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
    assert 'jobs_by_template' in json_data, "Missing 'jobs_by_template' in json_data"
    assert 'job_host_summary' in json_data, "Missing 'job_host_summary' in json_data"

    # Validate statistics structure (contains all the scalar totals)
    statistics = json_data['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dictionary'
    assert 'modules_used_to_automate_total' in statistics
    assert 'avg_number_of_modules_used_in_a_playbooks' in statistics
    assert 'total_hosts_automated' in statistics
    assert 'total_EE' in statistics
    assert 'default_EE' in statistics
    assert 'custom_EE' in statistics
    assert 'jobs_total' in statistics
    assert 'total_unique_hosts' in statistics

    # Validate statistics data types
    assert isinstance(statistics['modules_used_to_automate_total'], int)
    assert isinstance(statistics['avg_number_of_modules_used_in_a_playbooks'], (int, float))
    assert isinstance(statistics['total_hosts_automated'], int)
    assert isinstance(statistics['total_EE'], int)
    assert isinstance(statistics['default_EE'], int)
    assert isinstance(statistics['custom_EE'], int)
    assert isinstance(statistics['jobs_total'], int)
    assert isinstance(statistics['total_unique_hosts'], int)

    # Validate arrays structure
    assert isinstance(json_data['modules_used_per_playbook'], list), 'modules_used_per_playbook should be a list'
    assert isinstance(json_data['module_stats'], list), 'module_stats should be a list'
    assert isinstance(json_data['collection_name_stats'], list), 'collection_name_stats should be a list'
    assert isinstance(json_data['jobs_by_template'], list), 'jobs_by_template should be a list'
    assert isinstance(json_data['job_host_summary'], list), 'job_host_summary should be a list'

    # Validate module_stats have required fields
    if json_data['module_stats']:
        for module_stat in json_data['module_stats']:
            assert 'module_name' in module_stat
            assert 'collection_source' in module_stat
            assert 'collection_name' in module_stat
            assert 'jobs_total' in module_stat
            assert 'hosts_total' in module_stat

    # Validate jobs_by_template have required fields
    if json_data['jobs_by_template']:
        for job in json_data['jobs_by_template']:
            assert 'job_template_name' in job
            assert 'number_of_jobs_executed' in job
            assert 'number_of_jobs_failed' in job
            assert 'job_duration_average_in_seconds' in job
            assert 'job_waiting_time_average_in_seconds' in job

    # ========== Validate actual data values and relationships ==========

    # Validate statistics actual values
    print('\n--- Validating statistics data values ---')
    assert statistics['modules_used_to_automate_total'] == 1, 'Should have 1 module'
    assert statistics['total_hosts_automated'] == 2, 'Should have 2 hosts automated'
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
    assert first_module_stats['avg_hosts_per_job'] == pytest.approx(2.0, rel=1e-6), 'Should average 2 hosts per job'

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

    # Validate avg_number_of_modules_used_in_a_playbooks calculation
    total_modules_across_playbooks = sum(p['modules_used'] for p in json_data['modules_used_per_playbook'])
    num_playbooks = len(json_data['modules_used_per_playbook'])
    expected_avg = total_modules_across_playbooks / num_playbooks if num_playbooks > 0 else 0
    assert statistics['avg_number_of_modules_used_in_a_playbooks'] == pytest.approx(expected_avg, rel=1e-6), (
        f'Average should be {expected_avg}, got {statistics["avg_number_of_modules_used_in_a_playbooks"]}'
    )

    # Validate execution_environments actual values
    print('--- Validating execution_environments data values ---')
    assert statistics['total_EE'] == 2, 'Should have 2 total execution environments'
    assert statistics['default_EE'] == 1, 'Should have 1 default execution environment'
    assert statistics['custom_EE'] == 1, 'Should have 1 custom execution environment'
    # Validate that total = default + custom
    assert statistics['total_EE'] == statistics['default_EE'] + statistics['custom_EE'], 'Total EE should equal default + custom'

    # Validate jobs actual values
    print('--- Validating jobs data values ---')
    assert statistics['jobs_total'] == 3, 'Should have 3 total jobs'
    assert len(json_data['jobs_by_template']) == 1, 'Should have 1 job template'
    job = json_data['jobs_by_template'][0]
    assert job['number_of_jobs_executed'] == 3, 'Job template should have 3 executions'
    assert job['number_of_jobs_failed'] == 0, 'Should have 0 failed jobs'
    assert job['number_of_jobs_succeeded'] == 3, 'Should have 3 succeeded jobs'
    assert job['number_of_jobs_succeeded'] + job['number_of_jobs_failed'] == job['number_of_jobs_executed'], (
        'Succeeded + failed should equal total executed'
    )

    # Validate job duration fields are non-negative
    assert job['job_duration_average_in_seconds'] >= 0, 'Job duration average should be non-negative'
    assert job['job_duration_total_in_seconds'] >= 0, 'Job duration total should be non-negative'
    assert job['job_duration_maximum_in_seconds'] >= job['job_duration_minimum_in_seconds'], 'Max duration should be >= min duration'

    # Validate job waiting time fields are non-negative
    assert job['job_waiting_time_average_in_seconds'] >= 0, 'Job waiting time average should be non-negative'
    assert job['job_waiting_time_total_in_seconds'] >= 0, 'Job waiting time total should be non-negative'

    # Validate job_host_summary structure (now a direct array, not nested)
    print('--- Validating job_host_summary data values ---')
    job_host_summary = json_data['job_host_summary']
    assert isinstance(job_host_summary, list), 'job_host_summary should be a list'
    assert len(job_host_summary) == 1, 'Should have 1 job_host_summary entry'
    assert statistics['total_unique_hosts'] == 2, 'Should have 2 unique hosts'

    jhs = job_host_summary[0]
    assert 'job_template_name' in jhs
    assert 'dark_total' in jhs
    assert 'failures_total' in jhs
    assert 'ok_total' in jhs
    assert 'skipped_total' in jhs
    assert 'ignored_total' in jhs
    assert 'rescued_total' in jhs

    # Validate job_host_summary actual values
    assert jhs['ok_total'] == 6, 'Should have 6 ok tasks'
    assert jhs['failures_total'] == 0, 'Should have 0 failures'
    assert jhs['dark_total'] == 0, 'Should have 0 dark (unreachable) hosts'
    assert jhs['skipped_total'] == 0, 'Should have 0 skipped tasks'

    # Validate cross-section data consistency
    print('--- Validating cross-section data consistency ---')
    # Validate that module stats hosts match the total automated hosts
    for module_stat in json_data['module_stats']:
        assert module_stat['hosts_total'] <= statistics['total_hosts_automated'], (
            f'Module {module_stat["module_name"][:50]} hosts should not exceed total automated hosts'
        )

    print('✅ All data value assertions passed!')
