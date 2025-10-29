import json
import os
import shutil
import tarfile

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
    compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13)


def test_from_gather_to_json(cleanup_glob):
    # run gather
    json_data = task_anonymized_rollups('salt', 2025, 6, 13, './out')

    print(json_data)

    # save as json inside rollups/2025/06/13/anonymized.json
    json_path = f'./out/rollups/{2025}/06/13/anonymized.json'
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
    assert 'list_of_modules_used_to_automate' in events_modules
    assert 'modules_used_to_automate_total' in events_modules
    assert 'avg_number_of_modules_used_in_a_playbooks' in events_modules
    assert 'modules_used_per_playbook_total' in events_modules
    assert 'module_stats' in events_modules
    assert 'collection_name_stats' in events_modules
    assert 'total_hosts_automated' in events_modules

    # Validate events_modules data types
    assert isinstance(events_modules['list_of_modules_used_to_automate'], list)
    assert isinstance(events_modules['modules_used_to_automate_total'], int)
    assert isinstance(events_modules['avg_number_of_modules_used_in_a_playbooks'], (int, float))
    assert isinstance(events_modules['modules_used_per_playbook_total'], dict)
    assert isinstance(events_modules['module_stats'], list)
    assert isinstance(events_modules['collection_name_stats'], list)
    assert isinstance(events_modules['total_hosts_automated'], int)

    # Validate modules have required fields
    if events_modules['list_of_modules_used_to_automate']:
        for module in events_modules['list_of_modules_used_to_automate']:
            assert 'module_name' in module
            assert 'collection_source' in module
            assert 'collection_name' in module

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
    assert isinstance(jobs, list), 'jobs should be a list'
    if jobs:
        for job in jobs:
            assert 'job_template_name' in job
            assert 'number_of_jobs_executed' in job
            assert 'number_of_jobs_failed' in job
            assert 'job_duration_average_in_seconds' in job
            assert 'job_waiting_time_average_in_seconds' in job

    # Validate job_host_summary structure
    job_host_summary = json_data['job_host_summary']
    assert isinstance(job_host_summary, list), 'job_host_summary should be a list'
    if job_host_summary:
        for jhs in job_host_summary:
            assert 'job_template_name' in jhs
            assert 'jobs_total' in jhs
            assert 'hosts_total' in jhs
            assert 'ok_total' in jhs

    # Validate anonymization occurred (check for hashed values)
    # Job template names should be hashed (64 character hex strings)
    if jobs:
        for job in jobs:
            job_template_name = job['job_template_name']
            assert len(job_template_name) == 128, f'Job template name should be hashed (128 chars): {job_template_name}'
            assert all(c in '0123456789abcdef' for c in job_template_name), 'Job template name should be hex string'

    # ========== Validate the rollups are correctly saved in place ==========

    # Verify anonymized.json was saved
    assert os.path.exists(json_path), f'anonymized.json should be saved at {json_path}'
    with open(json_path, 'r') as f:
        saved_json = json.load(f)
        assert saved_json == json_data, 'Saved JSON should match the returned json_data'

    # Verify all rollup tarballs exist
    rollup_base_path = './out/rollups/2025/06/13'
    expected_rollups = ['jobs', 'job_host_summary', 'events_modules', 'execution_environments']

    for rollup_name in expected_rollups:
        tarball_path = os.path.join(rollup_base_path, rollup_name, 'data_rollups_2025_06_13.tar.gz')
        assert os.path.exists(tarball_path), f'Tarball should exist at {tarball_path}'

    # ========== Validate tarballs contain correct files ==========

    # Validate jobs tarball
    jobs_tarball = os.path.join(rollup_base_path, 'jobs', 'data_rollups_2025_06_13.tar.gz')
    with tarfile.open(jobs_tarball, 'r:gz') as tar:
        members = tar.getmembers()
        member_names = [m.name for m in members]
        print(f'Jobs tarball contains: {member_names}')
        assert './aggregations_by_template.csv' in member_names, 'Jobs tarball should contain aggregations_by_template.csv'

    # Validate job_host_summary tarball
    jhs_tarball = os.path.join(rollup_base_path, 'job_host_summary', 'data_rollups_2025_06_13.tar.gz')
    with tarfile.open(jhs_tarball, 'r:gz') as tar:
        members = tar.getmembers()
        member_names = [m.name for m in members]
        print(f'Job host summary tarball contains: {member_names}')
        assert './aggregated.csv' in member_names, 'Job host summary tarball should contain aggregated.csv'

    # Validate events_modules tarball
    em_tarball = os.path.join(rollup_base_path, 'events_modules', 'data_rollups_2025_06_13.tar.gz')
    with tarfile.open(em_tarball, 'r:gz') as tar:
        members = tar.getmembers()
        member_names = [m.name for m in members]
        print(f'Events modules tarball contains: {member_names}')
        assert './module_stats.csv' in member_names, 'Events modules tarball should contain module_stats.csv'
        # Should also contain a JSON file for total_hosts_automated
        json_files = [name for name in member_names if name.endswith('.json')]
        assert len(json_files) > 0, 'Events modules tarball should contain at least one JSON file'

    # Validate execution_environments tarball
    ee_tarball = os.path.join(rollup_base_path, 'execution_environments', 'data_rollups_2025_06_13.tar.gz')
    with tarfile.open(ee_tarball, 'r:gz') as tar:
        members = tar.getmembers()
        member_names = [m.name for m in members]
        print(f'Execution environments tarball contains: {member_names}')
        # Should contain a JSON file
        json_files = [name for name in member_names if name.endswith('.json')]
        assert len(json_files) > 0, 'Execution environments tarball should contain at least one JSON file'

    # Verify data directory exists and contains raw data tarballs
    data_path = './out/data/2025/06/13'
    assert os.path.exists(data_path), f'Data directory should exist at {data_path}'

    # Check that raw data tarballs were created
    data_tarballs = [f for f in os.listdir(data_path) if f.endswith('.tar.gz')]
    assert len(data_tarballs) > 0, 'Should have raw data tarballs in data directory'
    print(f'Found {len(data_tarballs)} raw data tarballs')

    print('\n✅ All assertions passed!')
