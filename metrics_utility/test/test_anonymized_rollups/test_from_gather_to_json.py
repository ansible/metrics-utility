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

    # Validate top-level structure
    assert 'events_modules' in json_data, "Missing 'events_modules' in json_data"
    assert 'execution_environments' in json_data, "Missing 'execution_environments' in json_data"
    assert 'jobs' in json_data, "Missing 'jobs' in json_data"
    assert 'job_host_summary' in json_data, "Missing 'job_host_summary' in json_data"

    # Validate events_modules structure
    events_modules = json_data['events_modules']
    assert isinstance(events_modules, dict), 'events_modules should be a dictionary'
    assert 'modules_used_to_automate_total' in events_modules
    assert 'avg_number_of_modules_used_in_a_playbooks' in events_modules
    assert 'modules_used_per_playbook_total' in events_modules
    assert 'module_stats' in events_modules
    assert 'collection_name_stats' in events_modules
    assert 'total_hosts_automated' in events_modules

    # Validate events_modules data types
    assert isinstance(events_modules['modules_used_to_automate_total'], int)
    assert isinstance(events_modules['avg_number_of_modules_used_in_a_playbooks'], (int, float))
    assert isinstance(events_modules['modules_used_per_playbook_total'], dict)
    assert isinstance(events_modules['module_stats'], list)
    assert isinstance(events_modules['collection_name_stats'], list)
    assert isinstance(events_modules['total_hosts_automated'], int)

    # Validate module_stats have required fields
    if events_modules['module_stats']:
        for module_stat in events_modules['module_stats']:
            assert 'module_name' in module_stat
            assert 'collection_source' in module_stat
            assert 'collection_name' in module_stat
            assert 'jobs_total' in module_stat
            assert 'hosts_total' in module_stat

    # Validate execution_environments structure
    execution_envs = json_data['execution_environments']
    assert isinstance(execution_envs, dict), 'execution_environments should be a dictionary'
    assert 'total_EE' in execution_envs
    assert 'default_EE' in execution_envs
    assert 'custom_EE' in execution_envs
    assert isinstance(execution_envs['total_EE'], int)
    assert isinstance(execution_envs['default_EE'], int)
    assert isinstance(execution_envs['custom_EE'], int)

    # Validate jobs structure
    jobs = json_data['jobs']
    assert isinstance(jobs, dict), 'jobs should be a dictionary'
    assert 'by_template' in jobs, 'jobs should have by_template key'
    assert 'jobs_total' in jobs, 'jobs should have jobs_total key'
    assert isinstance(jobs['by_template'], list), 'by_template should be a list'
    assert isinstance(jobs['jobs_total'], int), 'jobs_total should be an integer'
    if jobs['by_template']:
        for job in jobs['by_template']:
            assert 'job_template_name' in job
            assert 'number_of_jobs_executed' in job
            assert 'number_of_jobs_failed' in job
            assert 'job_duration_average_in_seconds' in job
            assert 'job_waiting_time_average_in_seconds' in job

    # ========== Validate actual data values and relationships ==========

    # Validate events_modules actual values
    print('\n--- Validating events_modules data values ---')
    assert events_modules['modules_used_to_automate_total'] == 1, 'Should have 1 module'
    assert events_modules['total_hosts_automated'] == 2, 'Should have 2 hosts automated'
    assert len(events_modules['module_stats']) == 1, 'Should have 1 module stats'
    assert len(events_modules['collection_name_stats']) == 1, 'Should have 1 collection stats'

    # Validate module_stats actual values
    print('--- Validating module_stats data values ---')
    first_module_stats = events_modules['module_stats'][0]
    assert first_module_stats['module_name'] == 'a10.acos_axapi.a10_slb_virtual_server', 'Module stats should match module'
    assert first_module_stats['jobs_total'] == 3, 'Should have 3 jobs using this module'
    assert first_module_stats['hosts_total'] == 2, 'Should have 2 hosts for this module'
    assert first_module_stats['task_clean_success_total'] == 6, 'Should have 6 successful tasks (3 jobs × 2 hosts)'
    assert first_module_stats['task_success_with_reruns_total'] == 0, 'Should have 0 reruns'
    assert first_module_stats['task_failed_total'] == 0, 'Should have 0 failures'
    assert first_module_stats['avg_hosts_per_job'] == pytest.approx(2.0, rel=1e-6), 'Should average 2 hosts per job'

    # Validate collection_name_stats
    print('--- Validating collection_name_stats data values ---')
    first_collection_stats = events_modules['collection_name_stats'][0]
    assert first_collection_stats['collection_name'] == 'a10.acos_axapi', 'Collection name should match'
    assert first_collection_stats['collection_source'] == 'community', 'Collection should be from community'
    assert first_collection_stats['jobs_total'] == 3, 'Collection should have 3 jobs'
    assert first_collection_stats['hosts_total'] == 2, 'Collection should have 2 hosts'
    assert first_collection_stats['task_clean_success_total'] == 6, 'Collection should have 6 successful tasks'

    # Validate modules_used_per_playbook_total structure and values
    print('--- Validating modules_used_per_playbook_total ---')
    assert len(events_modules['modules_used_per_playbook_total']) == 1, 'Should have 1 playbook'
    playbook_module_count = list(events_modules['modules_used_per_playbook_total'].values())[0]
    assert playbook_module_count == 1, 'Playbook should use 1 module'

    # Validate avg_number_of_modules_used_in_a_playbooks calculation
    total_modules_across_playbooks = sum(events_modules['modules_used_per_playbook_total'].values())
    num_playbooks = len(events_modules['modules_used_per_playbook_total'])
    expected_avg = total_modules_across_playbooks / num_playbooks if num_playbooks > 0 else 0
    assert events_modules['avg_number_of_modules_used_in_a_playbooks'] == pytest.approx(expected_avg, rel=1e-6), (
        f'Average should be {expected_avg}, got {events_modules["avg_number_of_modules_used_in_a_playbooks"]}'
    )

    # Validate execution_environments actual values
    print('--- Validating execution_environments data values ---')
    assert execution_envs['total_EE'] == 2, 'Should have 2 total execution environments'
    assert execution_envs['default_EE'] == 1, 'Should have 1 default execution environment'
    assert execution_envs['custom_EE'] == 1, 'Should have 1 custom execution environment'
    # Validate that total = default + custom
    assert execution_envs['total_EE'] == execution_envs['default_EE'] + execution_envs['custom_EE'], 'Total EE should equal default + custom'

    # Validate jobs actual values
    print('--- Validating jobs data values ---')
    assert jobs['jobs_total'] == 3, 'Should have 3 total jobs'
    assert len(jobs['by_template']) == 1, 'Should have 1 job template'
    job = jobs['by_template'][0]
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

    # Validate job_host_summary structure
    print('--- Validating job_host_summary data values ---')
    job_host_summary = json_data['job_host_summary']
    assert isinstance(job_host_summary, dict), 'job_host_summary should be a dict'
    assert 'aggregated' in job_host_summary, 'job_host_summary should have aggregated key'
    assert 'total_unique_hosts' in job_host_summary, 'job_host_summary should have total_unique_hosts key'

    assert isinstance(job_host_summary['aggregated'], list), 'aggregated should be a list'
    assert len(job_host_summary['aggregated']) == 1, 'Should have 1 job_host_summary entry'
    assert job_host_summary['total_unique_hosts'] == 2, 'Should have 2 unique hosts'

    jhs = job_host_summary['aggregated'][0]
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
    for module_stat in events_modules['module_stats']:
        assert module_stat['hosts_total'] <= events_modules['total_hosts_automated'], (
            f'Module {module_stat["module_name"][:50]} hosts should not exceed total automated hosts'
        )

    print('✅ All data value assertions passed!')
