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
        {'unified_jobs': [], 'job_host_summary': [], 'main_jobevent': [], 'execution_environments': [], 'credentials': []},
        'salt',
        since,
        until,
        './out',
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
    assert 'rollup_period_controller_versions' in json_data, "Missing 'rollup_period_controller_versions' at top level"
    assert 'rollup_period_scm_types' in json_data, "Missing 'rollup_period_scm_types' at top level"
    assert 'rollup_period_credential_types' in json_data, "Missing 'rollup_period_credential_types' at top level"
    assert 'module_stats' in json_data, "Missing 'module_stats' in json_data"
    assert 'collection_stats' in json_data, "Missing 'collection_stats' in json_data"
    assert 'jobs_by_job_type' in json_data, "Missing 'jobs_by_job_type' in json_data"
    assert 'jobs_by_launch_type' in json_data, "Missing 'jobs_by_launch_type' in json_data"
    # job_host_summary is now merged into jobs_by_job_type

    # Validate statistics structure (contains all the scalar totals)
    statistics = json_data['statistics']
    assert isinstance(statistics, dict), 'statistics should be a dictionary'
    assert 'rollup_period_modules_total' in statistics
    assert 'rollup_period_hosts_automated_total' in statistics
    assert 'rollup_period_execution_environments_total' in statistics
    assert 'rollup_period_EE_default_total' in statistics
    assert 'rollup_period_EE_custom_total' in statistics
    assert 'rollup_period_jobs_total' in statistics
    assert 'rollup_period_jobs_successful' in statistics
    assert 'rollup_period_jobs_failed' in statistics
    assert 'rollup_period_jobs_duration_all_statuses_seconds' in statistics
    assert 'rollup_period_jobs_successful_duration_total_seconds' in statistics
    assert 'rollup_period_jobs_failed_duration_total_seconds' in statistics
    assert 'rollup_period_organizations_total' in statistics
    assert 'rollup_period_forks_total' in statistics
    assert 'rollup_period_unique_hosts_total' in statistics
    assert 'rollup_period_job_host_pairs_total' in statistics
    assert 'rollup_period_successful_hosts_total' in statistics
    assert 'rollup_period_failed_hosts_total' in statistics
    assert 'rollup_period_unreachable_hosts_total' in statistics
    assert 'rollup_period_playbooks_total' in statistics
    assert 'rollup_period_templates_total' in statistics
    assert 'rollup_period_tasks_total' in statistics
    assert 'rollup_period_task_ok_total' in statistics
    assert 'rollup_period_task_failed_total' in statistics
    assert 'rollup_period_task_skipped_total' in statistics
    assert 'rollup_period_task_unreachable_total' in statistics
    assert 'rollup_period_task_ignored_total' in statistics
    # Arrays are now at top level, not in statistics
    # (rollup_period_controller_versions, rollup_period_scm_types, rollup_period_credential_types are at top level)

    # Validate statistics data types
    assert isinstance(statistics['rollup_period_modules_total'], int)
    assert isinstance(statistics['rollup_period_hosts_automated_total'], int)
    assert isinstance(statistics['rollup_period_execution_environments_total'], int)
    assert isinstance(statistics['rollup_period_EE_default_total'], int)
    assert isinstance(statistics['rollup_period_EE_custom_total'], int)
    assert isinstance(statistics['rollup_period_jobs_total'], int)
    # New job statistics fields can be int or None (if no jobs)
    if statistics['rollup_period_jobs_successful'] is not None:
        assert isinstance(statistics['rollup_period_jobs_successful'], (int, float)), 'jobs_successful should be int or float'
    if statistics['rollup_period_jobs_failed'] is not None:
        assert isinstance(statistics['rollup_period_jobs_failed'], (int, float)), 'jobs_failed should be int or float'
    if statistics['rollup_period_jobs_duration_all_statuses_seconds'] is not None:
        assert isinstance(statistics['rollup_period_jobs_duration_all_statuses_seconds'], (int, float)), (
            'jobs_duration_all_statuses_seconds should be int or float'
        )
    if statistics['rollup_period_jobs_successful_duration_total_seconds'] is not None:
        assert isinstance(statistics['rollup_period_jobs_successful_duration_total_seconds'], (int, float)), (
            'jobs_successful_duration_total_seconds should be int or float'
        )
    if statistics['rollup_period_jobs_failed_duration_total_seconds'] is not None:
        assert isinstance(statistics['rollup_period_jobs_failed_duration_total_seconds'], (int, float)), (
            'jobs_failed_duration_total_seconds should be int or float'
        )
    assert isinstance(statistics['rollup_period_forks_total'], int)
    assert isinstance(statistics['rollup_period_unique_hosts_total'], int)
    assert isinstance(statistics['rollup_period_job_host_pairs_total'], int), 'job_host_pairs_total should be an integer'
    # Host outcome totals can be int or None (if no job host summary data)
    if statistics['rollup_period_successful_hosts_total'] is not None:
        assert isinstance(statistics['rollup_period_successful_hosts_total'], int), 'successful_hosts_total should be an integer'
    if statistics['rollup_period_failed_hosts_total'] is not None:
        assert isinstance(statistics['rollup_period_failed_hosts_total'], int), 'failed_hosts_total should be an integer'
    if statistics['rollup_period_unreachable_hosts_total'] is not None:
        assert isinstance(statistics['rollup_period_unreachable_hosts_total'], int), 'unreachable_hosts_total should be an integer'
    assert isinstance(statistics['rollup_period_playbooks_total'], int), 'playbooks_total should be an integer'
    assert isinstance(statistics['rollup_period_templates_total'], int), 'templates_total should be an integer'

    # Validate arrays structure
    assert isinstance(json_data['module_stats'], list), 'module_stats should be a list'
    assert isinstance(json_data['collection_stats'], list), 'collection_stats should be a list'
    assert isinstance(json_data['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    assert isinstance(json_data['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'

    # Validate module_stats have required fields
    if json_data['module_stats']:
        for module_stat in json_data['module_stats']:
            assert 'module_name' in module_stat
            assert 'collection_source' in module_stat
            assert 'collection_name' in module_stat
            assert 'jobs_total' in module_stat
            assert 'unique_hosts_total' in module_stat
            assert 'processed_events_total' in module_stat
            assert 'controller_versions' in module_stat, 'Each module_stat should have controller_versions field'
            assert isinstance(module_stat['controller_versions'], list), 'controller_versions should be a list'

    # Validate collection_stats have required fields
    if json_data['collection_stats']:
        for collection_stat in json_data['collection_stats']:
            assert 'collection_name' in collection_stat
            assert 'collection_source' in collection_stat
            assert 'jobs_total' in collection_stat
            assert 'processed_events_total' in collection_stat
            assert 'controller_versions' in collection_stat, 'Each collection_stat should have controller_versions field'
            assert isinstance(collection_stat['controller_versions'], list), 'controller_versions should be a list'

    # Validate jobs_by_job_type have required fields (now grouped by job_type, merged with job_host_summary)
    if json_data['jobs_by_job_type']:
        for job in json_data['jobs_by_job_type']:
            assert 'job_type' in job
            assert 'jobs_total' in job
            assert 'jobs_failed_total' in job
            assert 'templates_total' in job
            # Host summary fields (merged from job_host_summary)
            assert 'dark_total' in job
            assert 'failures_total' in job
            assert 'ok_total' in job
            assert 'skipped_total' in job
            assert 'ignored_total' in job
            assert 'rescued_total' in job
            assert 'unique_hosts_total' in job

    # Validate jobs_by_launch_type have required fields (grouped by launch_type, with default host summary fields)
    if json_data['jobs_by_launch_type']:
        for job in json_data['jobs_by_launch_type']:
            assert 'launch_type' in job
            assert 'jobs_total' in job
            assert 'jobs_failed_total' in job
            assert 'templates_total' in job
            assert 'job_type_total' in job  # Count of distinct job types
            # Host summary fields (default values, not merged from job_host_summary)
            assert 'dark_total' in job
            assert 'failures_total' in job
            assert 'ok_total' in job
            assert 'skipped_total' in job
            assert 'ignored_total' in job
            assert 'rescued_total' in job
            assert 'unique_hosts_total' in job
            # Should NOT have launch_type_*_total fields (since we're grouping by launch_type)
            assert 'launch_type_manual_total' not in job
            assert 'launch_type_scheduled_total' not in job

    # Validate jobs_by_controller_version have required fields (grouped by controller_version, with default host summary fields)
    if json_data['jobs_by_controller_version']:
        for job in json_data['jobs_by_controller_version']:
            assert 'controller_version' in job
            assert 'jobs_total' in job
            assert 'jobs_failed_total' in job
            assert 'templates_total' in job
            assert 'job_type_total' in job  # Count of distinct job types
            # Host summary fields (default values, not merged from job_host_summary)
            assert 'dark_total' in job
            assert 'failures_total' in job
            assert 'ok_total' in job
            assert 'skipped_total' in job
            assert 'ignored_total' in job
            assert 'rescued_total' in job
            assert 'unique_hosts_total' in job
            # Should have launch_type_*_total fields (since we're grouping by ansible_version)
            assert 'launch_type_manual_total' in job or 'launch_type_scheduled_total' in job or 'launch_type_workflow_total' in job, (
                'Should have at least one launch_type_*_total field when grouping by ansible_version'
            )

    # ========== Validate actual data values and relationships ==========

    # Validate statistics actual values
    print('\n--- Validating statistics data values ---')
    assert statistics['rollup_period_modules_total'] == 2, 'Should have 2 modules (ansible.builtin.yum and a10.acos_axapi.a10_slb_virtual_server)'
    assert statistics['rollup_period_hosts_automated_total'] == 2, 'Should have 2 hosts automated'
    assert len(json_data['module_stats']) == 2, 'Should have 2 module stats'
    assert len(json_data['collection_stats']) == 2, 'Should have 2 collection stats (ansible.builtin and a10.acos_axapi)'

    # Validate module_stats actual values
    print('--- Validating module_stats data values ---')
    # Find modules by name (order may vary)
    # Note: ansible.builtin.yum will be anonymized (replaced with "Unknown") because ansible.builtin is not in collections.json
    module_stats_dict = {m['module_name']: m for m in json_data['module_stats']}

    # Find the anonymized module (has Unknown collection_source and "Unknown" name)
    anonymized_modules = [m for m in json_data['module_stats'] if m.get('collection_source') == 'Unknown']
    assert len(anonymized_modules) == 1, f'Should have 1 anonymized module (ansible.builtin.yum), got {len(anonymized_modules)}'
    yum_module = anonymized_modules[0]
    assert yum_module['jobs_total'] == 3, 'Should have 3 jobs using ansible.builtin.yum (anonymized)'
    assert yum_module['unique_hosts_total'] == 2, 'Should have 2 hosts for ansible.builtin.yum (anonymized)'
    assert yum_module['task_ok_total'] == 6, 'Should have 6 successful tasks for ansible.builtin.yum (3 jobs × 2 hosts)'
    assert yum_module['task_ok_with_retries_total'] == 0, 'Should have 0 reruns for ansible.builtin.yum'
    assert yum_module['task_failed_total'] == 0, 'Should have 0 failures for ansible.builtin.yum'
    assert yum_module['processed_events_total'] == 6, 'Should have 6 processed events for ansible.builtin.yum (3 jobs × 2 hosts)'
    assert yum_module['controller_versions'] == ['2.9.10'], (
        f'Expected controller_versions to be ["2.9.10"], got {yum_module.get("controller_versions")}'
    )
    # Module name should be "Unknown" (not hashed)
    assert yum_module['module_name'] == 'Unknown', f'Anonymized module name should be "Unknown", got {yum_module["module_name"]}'
    assert yum_module['collection_name'] == 'Unknown', f'Anonymized collection name should be "Unknown", got {yum_module.get("collection_name")}'

    # Validate a10.acos_axapi.a10_slb_virtual_server module (not anonymized)
    a10_module = module_stats_dict.get('a10.acos_axapi.a10_slb_virtual_server')
    assert a10_module is not None, 'Should have a10.acos_axapi.a10_slb_virtual_server module'
    assert a10_module['jobs_total'] == 3, 'Should have 3 jobs using a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['unique_hosts_total'] == 2, 'Should have 2 hosts for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['task_ok_total'] == 6, 'Should have 6 successful tasks for a10.acos_axapi.a10_slb_virtual_server (3 jobs × 2 hosts)'
    assert a10_module['task_ok_with_retries_total'] == 0, 'Should have 0 reruns for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['task_failed_total'] == 0, 'Should have 0 failures for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['processed_events_total'] == 6, 'Should have 6 processed events for a10.acos_axapi.a10_slb_virtual_server (3 jobs × 2 hosts)'
    assert a10_module['controller_versions'] == ['2.9.10'], (
        f'Expected controller_versions to be ["2.9.10"], got {a10_module.get("controller_versions")}'
    )

    # Validate collection_stats
    print('--- Validating collection_stats data values ---')
    # Find collections by name (order may vary)
    # Note: ansible.builtin will be anonymized (replaced with "Unknown") because it's not in collections.json
    collection_stats_dict = {c['collection_name']: c for c in json_data['collection_stats']}

    # Validate a10.acos_axapi collection (not anonymized)
    a10_collection = collection_stats_dict.get('a10.acos_axapi')
    assert a10_collection is not None, 'Should have a10.acos_axapi collection'
    assert a10_collection['collection_source'] == 'community', 'a10.acos_axapi collection should be from community'
    assert a10_collection['jobs_total'] == 3, 'a10.acos_axapi collection should have 3 jobs'
    assert 'controller_versions' in a10_collection, 'Each collection_stat should have controller_versions field'
    assert isinstance(a10_collection['controller_versions'], list), 'controller_versions should be a list'
    assert a10_collection['controller_versions'] == ['2.9.10'], (
        f'Expected controller_versions to be ["2.9.10"], got {a10_collection.get("controller_versions")}'
    )
    assert a10_collection['unique_hosts_total'] == 2, 'a10.acos_axapi collection should have 2 hosts'
    assert a10_collection['task_ok_total'] == 6, 'a10.acos_axapi collection should have 6 successful tasks'
    assert a10_collection['processed_events_total'] == 6, 'a10.acos_axapi collection should have 6 processed events (3 jobs × 2 hosts)'

    # Validate anonymized ansible.builtin collection (has Unknown collection_source and "Unknown" name)
    anonymized_collections = [c for c in json_data['collection_stats'] if c.get('collection_source') == 'Unknown']
    assert len(anonymized_collections) == 1, f'Should have 1 anonymized collection (ansible.builtin), got {len(anonymized_collections)}'
    builtin_collection = anonymized_collections[0]
    assert builtin_collection['collection_source'] == 'Unknown', 'ansible.builtin collection should be Unknown (not in collections.json)'
    assert builtin_collection['jobs_total'] == 3, 'ansible.builtin collection should have 3 jobs'
    assert 'controller_versions' in builtin_collection, 'Each collection_stat should have controller_versions field'
    assert isinstance(builtin_collection['controller_versions'], list), 'controller_versions should be a list'
    assert builtin_collection['controller_versions'] == ['2.9.10'], (
        f'Expected controller_versions to be ["2.9.10"], got {builtin_collection.get("controller_versions")}'
    )
    assert builtin_collection['unique_hosts_total'] == 2, 'ansible.builtin collection should have 2 hosts'
    assert builtin_collection['task_ok_total'] == 6, 'ansible.builtin collection should have 6 successful tasks'
    assert builtin_collection['processed_events_total'] == 6, 'ansible.builtin collection should have 6 processed events (3 jobs × 2 hosts)'
    # Collection name should be "Unknown" (not hashed)
    assert builtin_collection['collection_name'] == 'Unknown', (
        f'Anonymized collection name should be "Unknown", got {builtin_collection["collection_name"]}'
    )

    # Validate anonymized role_stats (roles with Unknown collection_source should have "Unknown" role names)
    if 'role_stats' in json_data and json_data['role_stats']:
        anonymized_roles = [r for r in json_data['role_stats'] if r.get('collection_source') == 'Unknown']
        for role_stat in anonymized_roles:
            # Role name should be "Unknown" (not hashed)
            if role_stat.get('role'):
                assert role_stat['role'] == 'Unknown', f'Anonymized role name should be "Unknown", got {role_stat.get("role")}'
            # Collection name should also be "Unknown" if present
            if role_stat.get('collection_name'):
                assert role_stat['collection_name'] == 'Unknown', (
                    f'Anonymized collection_name in role_stat should be "Unknown", got {role_stat.get("collection_name")}'
                )

    # Validate collections_versions - unknown collections should have "Unknown" as name and version
    print('--- Validating collections_versions data values ---')
    if 'collections_versions' in json_data and json_data['collections_versions']:
        collections_versions = json_data['collections_versions']
        assert isinstance(collections_versions, list), 'collections_versions should be a list'
        # Find unknown collections (ansible.builtin should be replaced with "Unknown")
        unknown_collections = [c for c in collections_versions if c.get('name') == 'Unknown']
        known_collections = [c for c in collections_versions if c.get('name') != 'Unknown']
        # Should have at least one unknown collection (ansible.builtin)
        assert len(unknown_collections) > 0, 'Should have at least one collection with "Unknown" name (ansible.builtin)'
        # Verify unknown collections have "Unknown" as both name and version
        for collection in unknown_collections:
            assert collection['name'] == 'Unknown', f'Unknown collection should have name "Unknown", got {collection.get("name")}'
            assert collection['version'] == 'Unknown', f'Unknown collection should have version "Unknown", got {collection.get("version")}'
        # Verify known collections have their actual names and versions
        for collection in known_collections:
            assert collection['name'] != 'Unknown', f'Known collection should not have name "Unknown", got {collection.get("name")}'
            assert collection['version'] != 'Unknown', f'Known collection should not have version "Unknown", got {collection.get("version")}'
            assert 'version' in collection, 'Each collection should have version field'
            assert 'job_count' in collection, 'Each collection should have job_count field'

    # Validate playbooks_total in statistics (modules_used_per_playbook is computed but not in final output)
    print('--- Validating playbooks_total ---')
    assert statistics['rollup_period_playbooks_total'] == 1, 'Should have 1 total playbook'

    # Validate execution_environments actual values
    print('--- Validating execution_environments data values ---')
    assert statistics['rollup_period_execution_environments_total'] == 2, 'Should have 2 total execution environments'
    assert statistics['rollup_period_EE_default_total'] == 1, 'Should have 1 default execution environment'
    assert statistics['rollup_period_EE_custom_total'] == 1, 'Should have 1 custom execution environment'
    # Validate that total = default + custom
    assert (
        statistics['rollup_period_execution_environments_total']
        == statistics['rollup_period_EE_default_total'] + statistics['rollup_period_EE_custom_total']
    ), 'Total EE should equal default + custom'

    # Validate jobs actual values
    print('--- Validating jobs data values ---')
    assert statistics['rollup_period_jobs_total'] == 3, 'Should have 3 total jobs'
    # forks_total should be sum of all forks: 5 + 10 + 20 = 35 (from test data with 3 jobs)
    assert statistics['rollup_period_forks_total'] == 35, 'Should have 35 total forks (5 + 10 + 20)'
    assert len(json_data['jobs_by_job_type']) == 1, 'Should have 1 job_type group'
    job = json_data['jobs_by_job_type'][0]
    assert job['jobs_total'] == 3, 'Job type should have 3 jobs'
    assert statistics['rollup_period_templates_total'] == 1, 'Should have 1 total job template (sum from all job_type groups)'
    assert job['jobs_failed_total'] == 0, 'Should have 0 failed jobs'
    # job_type should be 'job' from django_content_type.model
    assert job['job_type'] == 'job', f"Expected job_type to be 'job', but got {job['job_type']}"

    # Validate job duration fields are non-negative
    assert job['jobs_duration_total_seconds'] >= 0, 'Job duration total should be non-negative'
    assert job['job_duration_maximum_seconds'] >= job['job_duration_minimum_seconds'], 'Max duration should be >= min duration'

    # Validate job waiting time fields are non-negative
    assert job['job_waiting_time_total_seconds'] >= 0, 'Job waiting time total should be non-negative'

    # Validate job_host_summary data merged into jobs_by_job_type
    print('--- Validating job_host_summary data values (merged into jobs_by_job_type) ---')
    assert statistics['rollup_period_unique_hosts_total'] == 2, 'Should have 2 unique hosts'
    # job_host_pairs_total should be the count of all job host summary records
    # With 3 jobs and 2 hosts, we should have 3 * 2 = 6 job host summary records
    assert statistics['rollup_period_job_host_pairs_total'] == 6, (
        f'Should have 6 total job host summary records (3 jobs × 2 hosts), got {statistics["rollup_period_job_host_pairs_total"]}'
    )

    # Find the job entry with job_type='job'
    job_entry = next((j for j in json_data['jobs_by_job_type'] if j.get('job_type') == 'job'), None)
    assert job_entry is not None, 'Should have job_type job in jobs_by_job_type'

    # Validate job_host_summary fields are merged into jobs_by_job_type
    assert job_entry['ok_total'] == 6, 'Should have 6 ok tasks'
    assert job_entry['failures_total'] == 0, 'Should have 0 failures'
    assert job_entry['dark_total'] == 0, 'Should have 0 dark (unreachable) hosts'
    assert job_entry['skipped_total'] == 0, 'Should have 0 skipped tasks'
    assert job_entry['unique_hosts_total'] == 2, 'Should have 2 unique hosts'

    # Validate jobs_by_launch_type actual values
    print('--- Validating jobs_by_launch_type data values ---')
    assert len(json_data['jobs_by_launch_type']) >= 1, 'Should have at least 1 launch_type group'
    # Find the launch_type entry (should have at least one)
    launch_type_entry = json_data['jobs_by_launch_type'][0]
    assert 'launch_type' in launch_type_entry, 'Should have launch_type field'
    assert launch_type_entry['jobs_total'] >= 1, 'Should have at least 1 job in launch_type group'
    assert 'job_type_total' in launch_type_entry, 'Should have job_type_total field'
    assert launch_type_entry['job_type_total'] >= 1, 'Should have at least 1 job type'

    # Validate jobs_by_controller_version actual values
    print('--- Validating jobs_by_controller_version data values ---')
    assert len(json_data['jobs_by_controller_version']) >= 1, 'Should have at least 1 controller_version group'
    # Find the controller_version entry (should have at least one)
    controller_version_entry = json_data['jobs_by_controller_version'][0]
    assert 'controller_version' in controller_version_entry, 'Should have controller_version field'
    assert controller_version_entry['jobs_total'] >= 1, 'Should have at least 1 job in controller_version group'
    assert 'job_type_total' in controller_version_entry, 'Should have job_type_total field'
    assert controller_version_entry['job_type_total'] >= 1, 'Should have at least 1 job type'
    # Verify launch_type counts are present (since we're grouping by controller_version)
    assert 'launch_type_manual_total' in controller_version_entry or 'launch_type_scheduled_total' in controller_version_entry, (
        'Should have launch_type_*_total fields when grouping by controller_version'
    )

    # Verify totals match between all groupings
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in json_data['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in json_data['jobs_by_launch_type'])
    total_jobs_by_controller_version = sum(j.get('jobs_total', 0) for j in json_data['jobs_by_controller_version'])
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_controller_version == statistics['rollup_period_jobs_total'], (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_controller_version={total_jobs_by_controller_version}, '
        f'statistics={statistics["rollup_period_jobs_total"]}'
    )

    # Validate new job statistics match sum from jobs_by_job_type
    print('--- Validating job statistics match jobs_by_job_type sums ---')
    if json_data['jobs_by_job_type']:
        expected_jobs_successful = sum(j.get('jobs_successful_total', 0) for j in json_data['jobs_by_job_type'])
        expected_jobs_failed = sum(j.get('jobs_failed_total', 0) for j in json_data['jobs_by_job_type'])
        expected_duration_all = sum(j.get('jobs_duration_total_seconds', 0) or 0 for j in json_data['jobs_by_job_type'])
        expected_duration_successful = sum(j.get('jobs_successful_duration_total_seconds', 0) or 0 for j in json_data['jobs_by_job_type'])
        expected_duration_failed = sum(j.get('jobs_failed_duration_total_seconds', 0) or 0 for j in json_data['jobs_by_job_type'])

        if statistics['rollup_period_jobs_successful'] is not None:
            assert statistics['rollup_period_jobs_successful'] == expected_jobs_successful, (
                f'jobs_successful should match sum from jobs_by_job_type: expected={expected_jobs_successful}, '
                f'got={statistics["rollup_period_jobs_successful"]}'
            )
        if statistics['rollup_period_jobs_failed'] is not None:
            assert statistics['rollup_period_jobs_failed'] == expected_jobs_failed, (
                f'jobs_failed should match sum from jobs_by_job_type: expected={expected_jobs_failed}, got={statistics["rollup_period_jobs_failed"]}'
            )
        if statistics['rollup_period_jobs_duration_all_statuses_seconds'] is not None:
            assert abs(statistics['rollup_period_jobs_duration_all_statuses_seconds'] - expected_duration_all) < 0.001, (
                f'jobs_duration_all_statuses_seconds should match sum from jobs_by_job_type: expected={expected_duration_all}, '
                f'got={statistics["rollup_period_jobs_duration_all_statuses_seconds"]}'
            )
        if statistics['rollup_period_jobs_successful_duration_total_seconds'] is not None:
            assert abs(statistics['rollup_period_jobs_successful_duration_total_seconds'] - expected_duration_successful) < 0.001, (
                f'jobs_successful_duration_total_seconds should match sum from jobs_by_job_type: expected={expected_duration_successful}, '
                f'got={statistics["rollup_period_jobs_successful_duration_total_seconds"]}'
            )
        if statistics['rollup_period_jobs_failed_duration_total_seconds'] is not None:
            assert abs(statistics['rollup_period_jobs_failed_duration_total_seconds'] - expected_duration_failed) < 0.001, (
                f'jobs_failed_duration_total_seconds should match sum from jobs_by_job_type: expected={expected_duration_failed}, '
                f'got={statistics["rollup_period_jobs_failed_duration_total_seconds"]}'
            )

    # Validate cross-section data consistency
    print('--- Validating cross-section data consistency ---')
    # Validate that module stats hosts match the total automated hosts
    for module_stat in json_data['module_stats']:
        assert module_stat['unique_hosts_total'] <= statistics['rollup_period_hosts_automated_total'], (
            f'Module {module_stat["module_name"][:50]} hosts should not exceed total automated hosts'
        )

    # ========== Validate Credentials ==========
    print('--- Validating credentials data values ---')
    # Based on main_jobhostsummary.sql:
    # - Job 1: Machine + Amazon Web Services
    # - Job 2: Machine + Vault
    # - Job 3: Machine + Amazon Web Services + Network
    # Expected unique credential types:
    # - Amazon Web Services
    # - Machine
    # - Network
    # - Vault

    assert 'rollup_period_credential_types' in json_data, 'Should have rollup_period_credential_types at top level'
    credential_types = json_data['rollup_period_credential_types']
    assert isinstance(credential_types, list), 'rollup_period_credential_types should be a list'
    assert 'Amazon Web Services' in credential_types
    assert 'Machine' in credential_types
    assert 'Network' in credential_types
    assert 'Vault' in credential_types
    assert len(credential_types) == 4, f'Should have 4 unique credential types, got {len(credential_types)}'
    assert credential_types == sorted(credential_types), 'credential_types should be sorted'

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
    assert 'collection_stats' in json_data, "Missing 'collection_stats' in json_data"
    assert 'jobs_by_job_type' in json_data, "Missing 'jobs_by_job_type' in json_data"
    assert 'jobs_by_launch_type' in json_data, "Missing 'jobs_by_launch_type' in json_data"
    # job_host_summary is now merged into jobs_by_job_type

    # Validate basic types
    assert isinstance(json_data['statistics'], dict), 'statistics should be a dictionary'
    assert isinstance(json_data['module_stats'], list), 'module_stats should be a list'
    assert isinstance(json_data['collection_stats'], list), 'collection_stats should be a list'
    assert isinstance(json_data['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    assert isinstance(json_data['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'
    # job_host_summary is now merged into jobs_by_job_type

    # Validate credentials structure (if present)
    # Based on main_jobhostsummary.sql, we expect 4 credential types
    assert 'rollup_period_credential_types' in json_data, 'Should have rollup_period_credential_types at top level'
    credential_types = json_data['rollup_period_credential_types']
    assert isinstance(credential_types, list), 'rollup_period_credential_types should be a list'
    assert len(credential_types) == 4, f'Should have 4 unique credential types, got {len(credential_types)}'
    assert credential_types == sorted(credential_types), 'credential_types should be sorted'

    print('✅ Basic structure assertions passed!')
