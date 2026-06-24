import json
import os
import re
import shutil

import pandas as pd
import pytest

from django.db import connection

from metrics_utility.library.collectors.controller import (
    controller_version_service,
    credentials_service,
    execution_environments,
    feature_flags_service,
    job_host_summary_service,
    main_indirectmanagednodeaudit,
    main_jobevent_service,
    table_metadata,
    unified_jobs,
)
from metrics_utility.test.test_anonymized_rollups.helpers import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.util import utcdt


def _is_valid_version(version_str):
    """Check if a string looks like a version (numbers alternate with dots).

    Valid patterns: 2.9.10, 2.9, 2.15.0, etc.
    Invalid patterns: 2., .9, 2..9, 2.9., abc, 2.9.10a, etc.
    """
    if not isinstance(version_str, str):
        return False
    # Pattern: one or more digits, followed by zero or more (dot + one or more digits)
    # This ensures numbers alternate with dots and the string doesn't start/end with a dot
    pattern = r'^\d+(\.\d+)*$'
    return bool(re.match(pattern, version_str))


# where to find the tar.gz (match jobhostsummary test layout)


def _validate_top_level_structure(json_data):
    """Validate top-level flattened structure."""
    assert 'statistics' in json_data, "Missing 'statistics' in json_data"
    assert 'rollup_period_ansible_versions' in json_data, "Missing 'rollup_period_ansible_versions' at top level"
    assert 'rollup_period_scm_types' in json_data, "Missing 'rollup_period_scm_types' at top level"
    assert 'rollup_period_credential_types' in json_data, "Missing 'rollup_period_credential_types' at top level"
    assert 'module_stats' in json_data, "Missing 'module_stats' in json_data"
    assert 'collection_stats' in json_data, "Missing 'collection_stats' in json_data"
    assert 'jobs_by_job_type' in json_data, "Missing 'jobs_by_job_type' in json_data"
    assert 'jobs_by_launch_type' in json_data, "Missing 'jobs_by_launch_type' in json_data"
    assert 'jobs_by_controller_version' in json_data, "Missing 'jobs_by_controller_version' in json_data"
    assert 'table_metadata' in json_data, "Missing 'table_metadata' at top level"
    assert 'controller_versions' in json_data, "Missing 'controller_versions' at top level"
    assert 'feature_flags' in json_data, "Missing 'feature_flags' at top level"
    assert 'observability_by_tasks' in json_data, "Missing 'observability_by_tasks' at top level"
    assert 'indirect_managed_nodes' not in json_data, (
        'indirect_managed_nodes IDs must not appear at the top level — only the count goes in statistics'
    )


def _validate_statistics_structure(statistics):
    """Validate statistics structure contains all required fields."""
    assert isinstance(statistics, dict), 'statistics should be a dictionary'
    required_fields = [
        'rollup_period_modules_total',
        'rollup_period_unique_hosts_automated_total',
        'rollup_period_execution_environments_total',
        'rollup_period_EE_default_total',
        'rollup_period_EE_custom_total',
        'rollup_period_jobs_total',
        'rollup_period_jobs_successful',
        'rollup_period_jobs_failed',
        'rollup_period_jobs_duration_all_statuses_seconds',
        'rollup_period_jobs_successful_duration_total_seconds',
        'rollup_period_jobs_failed_duration_total_seconds',
        'rollup_period_organizations_total',
        'rollup_period_forks_total',
        'rollup_period_unique_hosts_total',
        'rollup_period_job_host_pairs_total',
        'rollup_period_successful_hosts_total',
        'rollup_period_failed_hosts_total',
        'rollup_period_unreachable_hosts_total',
        'rollup_period_playbooks_total',
        'rollup_period_templates_total',
        'rollup_period_tasks_total',
        'rollup_period_task_ok_total',
        'rollup_period_task_failed_total',
        'rollup_period_task_skipped_total',
        'rollup_period_task_unreachable_total',
        'rollup_period_task_ignored_total',
        'rollup_period_indirect_managed_nodes_all_total',
    ]
    for field in required_fields:
        assert field in statistics, f"Missing '{field}' in statistics"


def _validate_statistics_data_types(statistics):
    """Validate statistics data types."""
    assert isinstance(statistics['rollup_period_modules_total'], int)
    assert isinstance(statistics['rollup_period_unique_hosts_automated_total'], int)
    assert isinstance(statistics['rollup_period_execution_environments_total'], int)
    assert statistics['rollup_period_execution_environments_total'] == (
        statistics['rollup_period_EE_default_total'] + statistics['rollup_period_EE_custom_total']
    ), 'execution_environments_total should be sum of EE_default and EE_custom'
    assert isinstance(statistics['rollup_period_EE_default_total'], int)
    assert isinstance(statistics['rollup_period_EE_custom_total'], int)
    assert isinstance(statistics['rollup_period_jobs_total'], int)

    optional_int_float_fields = [
        'rollup_period_jobs_successful',
        'rollup_period_jobs_failed',
        'rollup_period_jobs_duration_all_statuses_seconds',
        'rollup_period_jobs_successful_duration_total_seconds',
        'rollup_period_jobs_failed_duration_total_seconds',
    ]
    for field in optional_int_float_fields:
        if statistics[field] is not None:
            assert isinstance(statistics[field], (int, float)), f'{field} should be int or float'

    assert isinstance(statistics['rollup_period_forks_total'], int)
    assert isinstance(statistics['rollup_period_unique_hosts_total'], int)
    assert isinstance(statistics['rollup_period_job_host_pairs_total'], int), 'job_host_pairs_total should be an integer'

    optional_int_fields = [
        'rollup_period_successful_hosts_total',
        'rollup_period_failed_hosts_total',
        'rollup_period_unreachable_hosts_total',
    ]
    for field in optional_int_fields:
        if statistics[field] is not None:
            assert isinstance(statistics[field], int), f'{field} should be an integer'

    assert isinstance(statistics['rollup_period_playbooks_total'], int), 'playbooks_total should be an integer'
    assert isinstance(statistics['rollup_period_templates_total'], int), 'templates_total should be an integer'


def _validate_arrays_structure(json_data):
    """Validate arrays structure."""
    assert isinstance(json_data['module_stats'], list), 'module_stats should be a list'
    assert isinstance(json_data['collection_stats'], list), 'collection_stats should be a list'
    assert isinstance(json_data['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
    assert isinstance(json_data['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'


def _validate_module_stats_structure(json_data):
    """Validate module_stats have required fields."""
    if not json_data['module_stats']:
        return
    for module_stat in json_data['module_stats']:
        assert 'module_name' in module_stat
        assert 'collection_source' in module_stat
        assert 'collection_name' in module_stat
        assert 'jobs_total' in module_stat
        assert 'unique_hosts_total' in module_stat
        assert 'processed_events_total' in module_stat
        assert 'ansible_versions' in module_stat, 'Each module_stat should have ansible_versions field'
        assert isinstance(module_stat['ansible_versions'], list), 'ansible_versions should be a list'


def _validate_collection_stats_structure(json_data):
    """Validate collection_stats have required fields."""
    if not json_data['collection_stats']:
        return
    for collection_stat in json_data['collection_stats']:
        assert 'collection_name' in collection_stat
        assert 'collection_source' in collection_stat
        assert 'jobs_total' in collection_stat
        assert 'processed_events_total' in collection_stat
        assert 'ansible_versions' in collection_stat, 'Each collection_stat should have ansible_versions field'
        assert isinstance(collection_stat['ansible_versions'], list), 'ansible_versions should be a list'


def _validate_jobs_by_job_type_structure(json_data):
    """Validate jobs_by_job_type have required fields."""
    if not json_data['jobs_by_job_type']:
        return
    for job in json_data['jobs_by_job_type']:
        assert 'job_type' in job
        assert 'jobs_total' in job
        assert 'jobs_failed_total' in job
        assert 'templates_total' in job
        assert 'unreachable_total' in job
        assert 'failed_total' in job
        assert 'ok_total' in job
        assert 'skipped_total' in job
        assert 'ignored_total' in job
        assert 'rescued_total' in job
        # Note: unique_hosts_total is only at top level (rollup_period_unique_hosts_total),
        # not in groupings


def _validate_jobs_by_launch_type_structure(json_data):
    """Validate jobs_by_launch_type have required fields."""
    if not json_data['jobs_by_launch_type']:
        return
    for job in json_data['jobs_by_launch_type']:
        assert 'launch_type' in job
        assert 'jobs_total' in job
        assert 'jobs_failed_total' in job
        assert 'templates_total' in job
        assert 'unreachable_total' in job
        assert 'failed_total' in job
        assert 'ok_total' in job
        assert 'skipped_total' in job
        assert 'ignored_total' in job
        assert 'rescued_total' in job
        # Note: unique_hosts_total is only at top level (rollup_period_unique_hosts_total),
        # not in groupings
        assert 'launch_type_manual_total' not in job
        assert 'launch_type_scheduled_total' not in job


def _validate_jobs_by_ansible_version_structure(json_data):
    """Validate jobs_by_ansible_version have required fields."""
    if not json_data.get('jobs_by_ansible_version'):
        return
    for job in json_data['jobs_by_ansible_version']:
        assert 'ansible_version' in job
        assert 'jobs_total' in job
        assert 'jobs_failed_total' in job
        assert 'templates_total' in job
        assert 'unreachable_total' in job
        assert 'failed_total' in job
        assert 'ok_total' in job
        assert 'skipped_total' in job
        assert 'ignored_total' in job
        assert 'rescued_total' in job
        # Note: unique_hosts_total is only at top level (rollup_period_unique_hosts_total),
        # not in groupings
        assert 'launch_type_manual_total' not in job
        assert 'launch_type_scheduled_total' not in job
        assert 'launch_type_workflow_total' not in job


def _validate_module_stats_values(json_data):
    """Validate module_stats actual values."""
    print('--- Validating module_stats data values ---')
    module_stats_dict = {m['module_name']: m for m in json_data['module_stats']}

    anonymized_modules = [m for m in json_data['module_stats'] if m.get('collection_source') == 'Custom']
    assert len(anonymized_modules) == 1, f'Should have 1 anonymized module (ansible.builtin.yum), got {len(anonymized_modules)}'
    yum_module = anonymized_modules[0]
    assert yum_module['jobs_total'] == 3, 'Should have 3 jobs using ansible.builtin.yum (anonymized)'
    assert yum_module['unique_hosts_total'] == 2, 'Should have 2 hosts for ansible.builtin.yum (anonymized)'
    assert yum_module['task_ok_total'] == 6, 'Should have 6 successful tasks for ansible.builtin.yum (3 jobs × 2 hosts)'
    assert yum_module['task_ok_with_retries_total'] == 0, 'Should have 0 reruns for ansible.builtin.yum'
    assert yum_module['task_failed_total'] == 0, 'Should have 0 failures for ansible.builtin.yum'
    assert yum_module['processed_events_total'] == 6, 'Should have 6 processed events for ansible.builtin.yum (3 jobs × 2 hosts)'
    assert 'ansible_versions' in yum_module, 'yum_module should have ansible_versions field'
    assert isinstance(yum_module['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(yum_module['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in yum_module['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'
    assert yum_module['module_name'] == 'Custom', f'Anonymized module name should be "Custom", got {yum_module["module_name"]}'
    assert yum_module['collection_name'] == 'Custom', f'Anonymized collection name should be "Custom", got {yum_module.get("collection_name")}'

    a10_module = module_stats_dict.get('a10.acos_axapi.a10_slb_virtual_server')
    assert a10_module is not None, 'Should have a10.acos_axapi.a10_slb_virtual_server module'
    assert a10_module['jobs_total'] == 3, 'Should have 3 jobs using a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['unique_hosts_total'] == 2, 'Should have 2 hosts for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['task_ok_total'] == 6, 'Should have 6 successful tasks for a10.acos_axapi.a10_slb_virtual_server (3 jobs × 2 hosts)'
    assert a10_module['task_ok_with_retries_total'] == 0, 'Should have 0 reruns for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['task_failed_total'] == 0, 'Should have 0 failures for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['processed_events_total'] == 6, 'Should have 6 processed events for a10.acos_axapi.a10_slb_virtual_server (3 jobs × 2 hosts)'
    assert 'ansible_versions' in a10_module, 'a10_module should have ansible_versions field'
    assert isinstance(a10_module['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(a10_module['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in a10_module['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'


def _validate_collection_stats_values(json_data):
    """Validate collection_stats actual values."""
    print('--- Validating collection_stats data values ---')
    collection_stats_dict = {c['collection_name']: c for c in json_data['collection_stats']}

    a10_collection = collection_stats_dict.get('a10.acos_axapi')
    assert a10_collection is not None, 'Should have a10.acos_axapi collection'
    assert a10_collection['collection_source'] == 'community', 'a10.acos_axapi collection should be from community'
    assert a10_collection['jobs_total'] == 3, 'a10.acos_axapi collection should have 3 jobs'
    assert 'ansible_versions' in a10_collection, 'Each collection_stat should have ansible_versions field'
    assert isinstance(a10_collection['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(a10_collection['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in a10_collection['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'
    assert a10_collection['unique_hosts_total'] == 2, 'a10.acos_axapi collection should have 2 hosts'
    assert a10_collection['task_ok_total'] == 6, 'a10.acos_axapi collection should have 6 successful tasks'
    assert a10_collection['processed_events_total'] == 6, 'a10.acos_axapi collection should have 6 processed events (3 jobs × 2 hosts)'

    anonymized_collections = [c for c in json_data['collection_stats'] if c.get('collection_source') == 'Custom']
    assert len(anonymized_collections) == 1, f'Should have 1 anonymized collection (ansible.builtin), got {len(anonymized_collections)}'
    builtin_collection = anonymized_collections[0]
    assert builtin_collection['collection_source'] == 'Custom', 'ansible.builtin collection should be Custom (not in collections.json)'
    assert builtin_collection['jobs_total'] == 3, 'ansible.builtin collection should have 3 jobs'
    assert 'ansible_versions' in builtin_collection, 'Each collection_stat should have ansible_versions field'
    assert isinstance(builtin_collection['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(builtin_collection['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in builtin_collection['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'
    assert builtin_collection['unique_hosts_total'] == 2, 'ansible.builtin collection should have 2 hosts'
    assert builtin_collection['task_ok_total'] == 6, 'ansible.builtin collection should have 6 successful tasks'
    assert builtin_collection['processed_events_total'] == 6, 'ansible.builtin collection should have 6 processed events (3 jobs × 2 hosts)'
    assert builtin_collection['collection_name'] == 'Custom', (
        f'Anonymized collection name should be "Custom", got {builtin_collection["collection_name"]}'
    )


def _validate_role_stats(json_data):
    """Validate anonymized role_stats."""
    if not ('role_stats' in json_data and json_data['role_stats']):
        return
    anonymized_roles = [r for r in json_data['role_stats'] if r.get('collection_source') == 'Custom']
    for role_stat in anonymized_roles:
        if role_stat.get('role'):
            assert role_stat['role'] == 'Custom', f'Anonymized role name should be "Custom", got {role_stat.get("role")}'
        if role_stat.get('collection_name'):
            assert role_stat['collection_name'] == 'Custom', (
                f'Anonymized collection_name in role_stat should be "Custom", got {role_stat.get("collection_name")}'
            )


def _validate_jobs_by_installed_collections_versions(json_data):
    """Validate jobs_by_installed_collections_versions."""
    if not ('jobs_by_installed_collections_versions' in json_data and json_data['jobs_by_installed_collections_versions']):
        return
    print('--- Validating jobs_by_installed_collections_versions data values ---')
    jobs_by_installed_collections_versions = json_data['jobs_by_installed_collections_versions']
    assert isinstance(jobs_by_installed_collections_versions, list), 'jobs_by_installed_collections_versions should be a list'
    unknown_collections = [c for c in jobs_by_installed_collections_versions if c.get('collection') == 'Custom']
    known_collections = [c for c in jobs_by_installed_collections_versions if c.get('collection') != 'Custom']
    assert len(unknown_collections) > 0, 'Should have at least one collection with "Custom" collection (ansible.builtin)'
    for collection in unknown_collections:
        assert collection['collection'] == 'Custom', f'Custom collection should have collection "Custom", got {collection.get("collection")}'
        assert collection['version'] == 'Custom', f'Custom collection should have version "Custom", got {collection.get("version")}'
    new_fields = [
        'jobs_never_started_total',
        'jobs_duration_total_seconds',
        'jobs_successful_duration_total_seconds',
        'jobs_failed_duration_total_seconds',
        'job_duration_maximum_seconds',
        'job_duration_minimum_seconds',
        'job_waiting_time_total_seconds',
        'job_waiting_time_maximum_seconds',
        'job_waiting_time_minimum_seconds',
        'templates_total',
        'inventories_total',
        'ansible_versions',
    ]
    for collection in known_collections:
        assert collection['collection'] != 'Custom', f'Known collection should not have collection "Custom", got {collection.get("collection")}'
        assert collection['version'] != 'Custom', f'Known collection should not have version "Custom", got {collection.get("version")}'
        assert 'version' in collection, 'Each collection should have version field'
        assert 'jobs_total' in collection, 'Each collection should have jobs_total field'
        assert 'jobs_failed_total' in collection, 'Each collection should have jobs_failed_total field'
        assert 'jobs_successful_total' in collection, 'Each collection should have jobs_successful_total field'
        assert isinstance(collection.get('jobs_total'), int), 'jobs_total should be an integer'
        assert isinstance(collection.get('jobs_failed_total'), int), 'jobs_failed_total should be an integer'
        assert isinstance(collection.get('jobs_successful_total'), int), 'jobs_successful_total should be an integer'
        assert collection['jobs_failed_total'] + collection['jobs_successful_total'] == collection['jobs_total'], (
            f'jobs_failed_total + jobs_successful_total should equal jobs_total for {collection}'
        )
        for field in new_fields:
            assert field in collection, (
                f'Missing new field {field!r} in jobs_by_installed_collections_versions entry {collection["collection"]} {collection["version"]}'
            )
        assert isinstance(collection['jobs_never_started_total'], int), 'jobs_never_started_total should be an int'
        assert isinstance(collection['templates_total'], int), 'templates_total should be an int'
        assert isinstance(collection['inventories_total'], int), 'inventories_total should be an int'
        assert isinstance(collection['ansible_versions'], list), 'ansible_versions should be a list'
        assert collection['jobs_duration_total_seconds'] >= 0, 'jobs_duration_total_seconds should be non-negative'
        assert collection['job_waiting_time_total_seconds'] >= 0, 'job_waiting_time_total_seconds should be non-negative'
        # max >= min when both are set
        if collection['job_duration_maximum_seconds'] is not None and collection['job_duration_minimum_seconds'] is not None:
            assert collection['job_duration_maximum_seconds'] >= collection['job_duration_minimum_seconds'], (
                'job_duration_maximum_seconds should be >= job_duration_minimum_seconds'
            )
        if collection['job_waiting_time_maximum_seconds'] is not None and collection['job_waiting_time_minimum_seconds'] is not None:
            assert collection['job_waiting_time_maximum_seconds'] >= collection['job_waiting_time_minimum_seconds'], (
                'job_waiting_time_maximum_seconds should be >= job_waiting_time_minimum_seconds'
            )
    # same structural checks for unknown (Custom) collections
    for collection in unknown_collections:
        for field in new_fields:
            assert field in collection, f'Missing new field {field!r} in Custom jobs_by_installed_collections_versions entry'
        assert isinstance(collection['jobs_never_started_total'], int)
        assert isinstance(collection['templates_total'], int)
        assert isinstance(collection['inventories_total'], int)
        assert isinstance(collection['ansible_versions'], list)


def _validate_role_stats_and_jobs_by_installed_collections_versions(json_data):
    """Validate anonymized role_stats and jobs_by_installed_collections_versions."""
    _validate_role_stats(json_data)
    _validate_jobs_by_installed_collections_versions(json_data)


def _validate_jobs_values(json_data, statistics):
    """Validate jobs actual values."""
    print('--- Validating jobs data values ---')
    assert statistics['rollup_period_jobs_total'] == 3, 'Should have 3 total jobs'
    assert statistics['rollup_period_forks_total'] == 35, 'Should have 35 total forks (5 + 10 + 20)'
    assert len(json_data['jobs_by_job_type']) == 1, 'Should have 1 job_type group'
    job = json_data['jobs_by_job_type'][0]
    assert job['jobs_total'] == 3, 'Job type should have 3 jobs'
    assert statistics['rollup_period_templates_total'] == 1, 'Should have 1 total job template (sum from all job_type groups)'
    assert job['jobs_failed_total'] == 0, 'Should have 0 failed jobs'
    assert job['job_type'] == 'job', f"Expected job_type to be 'job', but got {job['job_type']}"
    # Job durations: 120s + 180s + 90s = 390s total
    assert job['jobs_duration_total_seconds'] == pytest.approx(390.0, rel=1e-6), (
        f'Job duration total should be 390 seconds (120+180+90), got {job["jobs_duration_total_seconds"]}'
    )
    assert job['job_duration_minimum_seconds'] == pytest.approx(90.0, rel=1e-6), (
        f'Job duration minimum should be 90 seconds, got {job["job_duration_minimum_seconds"]}'
    )
    assert job['job_duration_maximum_seconds'] == pytest.approx(180.0, rel=1e-6), (
        f'Job duration maximum should be 180 seconds, got {job["job_duration_maximum_seconds"]}'
    )
    assert job['job_duration_maximum_seconds'] >= job['job_duration_minimum_seconds'], 'Max duration should be >= min duration'
    # Waiting time: all jobs created at 10:00:00, started at 10:00:10, 10:00:20, 10:00:30
    # Job 1: 10s wait, Job 2: 20s wait, Job 3: 30s wait = 60s total
    assert job['job_waiting_time_total_seconds'] == pytest.approx(60.0, rel=1e-6), (
        f'Job waiting time total should be 60 seconds (10+20+30), got {job["job_waiting_time_total_seconds"]}'
    )


def _validate_jobs_values_multi_hour(json_data, statistics):
    """Validate jobs actual values for multi-hour data (10:00-12:00)."""
    print('--- Validating jobs data values (multi-hour) ---')
    # 3 jobs from 10:00 hour + 3 jobs from 11:00 hour = 6 total
    assert statistics['rollup_period_jobs_total'] == 6, 'Should have 6 total jobs (3 from 10:00 + 3 from 11:00)'
    # Forks: (5 + 10 + 20) from 10:00 + (8 + 15 + 25) from 11:00 = 35 + 48 = 83
    assert statistics['rollup_period_forks_total'] == 83, 'Should have 83 total forks (35 + 48)'
    assert len(json_data['jobs_by_job_type']) == 1, 'Should have 1 job_type group'
    job = json_data['jobs_by_job_type'][0]
    assert job['jobs_total'] == 6, 'Job type should have 6 jobs'
    assert statistics['rollup_period_templates_total'] == 1, 'Should have 1 total job template (sum from all job_type groups)'
    assert job['jobs_failed_total'] == 1, 'Should have 1 failed job (job 3 from 10:00h)'
    assert job['job_type'] == 'job', f"Expected job_type to be 'job', but got {job['job_type']}"
    # Job durations: 10:00 hour: 120s + 180s + 90s = 390s
    #                11:00 hour: 100s + 150s + 80s = 330s
    #                Total: 720s
    assert job['jobs_duration_total_seconds'] == pytest.approx(720.0, rel=1e-6), (
        f'Job duration total should be 720 seconds (390+330), got {job["jobs_duration_total_seconds"]}'
    )
    assert job['job_duration_minimum_seconds'] == pytest.approx(80.0, rel=1e-6), (
        f'Job duration minimum should be 80 seconds, got {job["job_duration_minimum_seconds"]}'
    )
    assert job['job_duration_maximum_seconds'] == pytest.approx(180.0, rel=1e-6), (
        f'Job duration maximum should be 180 seconds, got {job["job_duration_maximum_seconds"]}'
    )
    assert job['job_duration_maximum_seconds'] >= job['job_duration_minimum_seconds'], 'Max duration should be >= min duration'
    # Waiting time: 10:00 hour: 10s + 20s + 30s = 60s
    #              11:00 hour: 10s + 20s + 30s = 60s
    #              Total: 120s
    assert job['job_waiting_time_total_seconds'] == pytest.approx(120.0, rel=1e-6), (
        f'Job waiting time total should be 120 seconds (60+60), got {job["job_waiting_time_total_seconds"]}'
    )


def _validate_job_host_summary_values(json_data, statistics):
    """Validate job_host_summary data merged into jobs_by_job_type."""
    print('--- Validating job_host_summary data values (merged into jobs_by_job_type) ---')
    assert statistics['rollup_period_unique_hosts_total'] == 2, 'Should have 2 unique hosts'
    assert statistics['rollup_period_job_host_pairs_total'] == 6, (
        f'Should have 6 total job host summary records (3 jobs × 2 hosts), got {statistics["rollup_period_job_host_pairs_total"]}'
    )

    job_entry = next((j for j in json_data['jobs_by_job_type'] if j.get('job_type') == 'job'), None)
    assert job_entry is not None, 'Should have job_type job in jobs_by_job_type'
    assert job_entry['ok_total'] == 6, 'Should have 6 ok tasks'
    assert job_entry['failed_total'] == 0, 'Should have 0 failures'
    assert job_entry['unreachable_total'] == 0, 'Should have 0 dark (unreachable) hosts'
    assert job_entry['skipped_total'] == 0, 'Should have 0 skipped tasks'
    # Note: unique_hosts_total is only computed at the top level (rollup_period_unique_hosts_total),
    # not per job_type group, as host_ids are not tracked in groupings


def _validate_job_host_summary_values_multi_hour(json_data, statistics):
    """Validate job_host_summary data merged into jobs_by_job_type for multi-hour data."""
    print('--- Validating job_host_summary data values (merged into jobs_by_job_type, multi-hour) ---')
    assert statistics['rollup_period_unique_hosts_total'] == 2, 'Should have 2 unique hosts'
    # 6 jobs from 10:00 + 6 jobs from 11:00 = 12 total job-host pairs (6 jobs × 2 hosts)
    assert statistics['rollup_period_job_host_pairs_total'] == 12, (
        f'Should have 12 total job host summary records (6 jobs × 2 hosts), got {statistics["rollup_period_job_host_pairs_total"]}'
    )

    job_entry = next((j for j in json_data['jobs_by_job_type'] if j.get('job_type') == 'job'), None)
    assert job_entry is not None, 'Should have job_type job in jobs_by_job_type'
    # 10:00h: jobs 1 and 2 have ok=1 per host (2×2=4), job 3 (failed) has ok=0 per host (1×2=0)
    # 11:00h: all 3 jobs have ok=1 per host (3×2=6)
    # Total: 4 + 0 + 6 = 10
    assert job_entry['ok_total'] == 10, 'Should have 10 ok tasks (job 3 from 10:00h is failed with ok=0)'
    # job 3 (10:00h, failed) has failures=1 per host, 2 hosts → 2 total failures
    assert job_entry['failed_total'] == 2, 'Should have 2 failures (job 3 from 10:00h: 2 hosts × failures=1)'
    assert job_entry['unreachable_total'] == 0, 'Should have 0 dark (unreachable) hosts'
    assert job_entry['skipped_total'] == 0, 'Should have 0 skipped tasks'
    # Note: unique_hosts_total is only computed at the top level (rollup_period_unique_hosts_total),
    # not per job_type group, as host_ids are not tracked in groupings


def _validate_module_stats_values_multi_hour(json_data):
    """Validate module_stats actual values for multi-hour data (6 jobs total)."""
    print('--- Validating module_stats data values (multi-hour) ---')
    module_stats_dict = {m['module_name']: m for m in json_data['module_stats']}

    anonymized_modules = [m for m in json_data['module_stats'] if m.get('collection_source') == 'Custom']
    assert len(anonymized_modules) == 1, f'Should have 1 anonymized module (ansible.builtin.yum), got {len(anonymized_modules)}'
    yum_module = anonymized_modules[0]
    # 6 jobs total (3 from 10:00 + 3 from 11:00)
    assert yum_module['jobs_total'] == 6, 'Should have 6 jobs using ansible.builtin.yum (anonymized)'
    assert yum_module['unique_hosts_total'] == 2, 'Should have 2 hosts for ansible.builtin.yum (anonymized)'
    # 6 jobs × 2 hosts = 12 successful tasks
    assert yum_module['task_ok_total'] == 12, 'Should have 12 successful tasks for ansible.builtin.yum (6 jobs × 2 hosts)'
    assert yum_module['task_ok_with_retries_total'] == 0, 'Should have 0 reruns for ansible.builtin.yum'
    assert yum_module['task_failed_total'] == 0, 'Should have 0 failures for ansible.builtin.yum'
    # 6 jobs × 2 hosts = 12 processed events
    assert yum_module['processed_events_total'] == 12, 'Should have 12 processed events for ansible.builtin.yum (6 jobs × 2 hosts)'
    assert 'ansible_versions' in yum_module, 'yum_module should have ansible_versions field'
    assert isinstance(yum_module['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(yum_module['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in yum_module['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'
    assert yum_module['module_name'] == 'Custom', f'Anonymized module name should be "Custom", got {yum_module["module_name"]}'
    assert yum_module['collection_name'] == 'Custom', f'Anonymized collection name should be "Custom", got {yum_module.get("collection_name")}'

    a10_module = module_stats_dict.get('a10.acos_axapi.a10_slb_virtual_server')
    assert a10_module is not None, 'Should have a10.acos_axapi.a10_slb_virtual_server module'
    # 6 jobs total
    assert a10_module['jobs_total'] == 6, 'Should have 6 jobs using a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['unique_hosts_total'] == 2, 'Should have 2 hosts for a10.acos_axapi.a10_slb_virtual_server'
    # 6 jobs × 2 hosts = 12 successful tasks
    assert a10_module['task_ok_total'] == 12, 'Should have 12 successful tasks for a10.acos_axapi.a10_slb_virtual_server (6 jobs × 2 hosts)'
    assert a10_module['task_ok_with_retries_total'] == 0, 'Should have 0 reruns for a10.acos_axapi.a10_slb_virtual_server'
    assert a10_module['task_failed_total'] == 0, 'Should have 0 failures for a10.acos_axapi.a10_slb_virtual_server'
    # 6 jobs × 2 hosts = 12 processed events
    assert a10_module['processed_events_total'] == 12, 'Should have 12 processed events for a10.acos_axapi.a10_slb_virtual_server (6 jobs × 2 hosts)'
    assert 'ansible_versions' in a10_module, 'a10_module should have ansible_versions field'
    assert isinstance(a10_module['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(a10_module['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in a10_module['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'


def _validate_collection_stats_values_multi_hour(json_data):
    """Validate collection_stats actual values for multi-hour data (6 jobs total)."""
    print('--- Validating collection_stats data values (multi-hour) ---')
    collection_stats_dict = {c['collection_name']: c for c in json_data['collection_stats']}

    a10_collection = collection_stats_dict.get('a10.acos_axapi')
    assert a10_collection is not None, 'Should have a10.acos_axapi collection'
    assert a10_collection['collection_source'] == 'community', 'a10.acos_axapi collection should be from community'
    # 6 jobs total
    assert a10_collection['jobs_total'] == 6, 'a10.acos_axapi collection should have 6 jobs'
    assert 'ansible_versions' in a10_collection, 'Each collection_stat should have ansible_versions field'
    assert isinstance(a10_collection['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(a10_collection['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in a10_collection['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'
    assert a10_collection['unique_hosts_total'] == 2, 'a10.acos_axapi collection should have 2 hosts'
    # 6 jobs × 2 hosts = 12 successful tasks
    assert a10_collection['task_ok_total'] == 12, 'a10.acos_axapi collection should have 12 successful tasks'
    # 6 jobs × 2 hosts = 12 processed events
    assert a10_collection['processed_events_total'] == 12, 'a10.acos_axapi collection should have 12 processed events (6 jobs × 2 hosts)'

    anonymized_collections = [c for c in json_data['collection_stats'] if c.get('collection_source') == 'Custom']
    assert len(anonymized_collections) == 1, f'Should have 1 anonymized collection (ansible.builtin), got {len(anonymized_collections)}'
    builtin_collection = anonymized_collections[0]
    assert builtin_collection['collection_source'] == 'Custom', 'ansible.builtin collection should be Custom (not in collections.json)'
    # 6 jobs total
    assert builtin_collection['jobs_total'] == 6, 'ansible.builtin collection should have 6 jobs'
    assert 'ansible_versions' in builtin_collection, 'Each collection_stat should have ansible_versions field'
    assert isinstance(builtin_collection['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(builtin_collection['ansible_versions']) > 0, 'ansible_versions should not be empty'
    for version in builtin_collection['ansible_versions']:
        assert _is_valid_version(version), f'Version should contain numbers and dots, got {version}'
    assert builtin_collection['unique_hosts_total'] == 2, 'ansible.builtin collection should have 2 hosts'
    # 6 jobs × 2 hosts = 12 successful tasks
    assert builtin_collection['task_ok_total'] == 12, 'ansible.builtin collection should have 12 successful tasks'
    # 6 jobs × 2 hosts = 12 processed events
    assert builtin_collection['processed_events_total'] == 12, 'ansible.builtin collection should have 12 processed events (6 jobs × 2 hosts)'
    assert builtin_collection['collection_name'] == 'Custom', (
        f'Anonymized collection name should be "Custom", got {builtin_collection["collection_name"]}'
    )


def _validate_jobs_by_launch_type_values(json_data):
    """Validate jobs_by_launch_type actual values."""
    print('--- Validating jobs_by_launch_type data values ---')
    assert len(json_data['jobs_by_launch_type']) >= 1, 'Should have at least 1 launch_type group'
    launch_type_entry = json_data['jobs_by_launch_type'][0]
    assert 'launch_type' in launch_type_entry, 'Should have launch_type field'
    assert launch_type_entry['jobs_total'] >= 1, 'Should have at least 1 job in launch_type group'


def _validate_jobs_by_ansible_version_values(json_data):
    """Validate jobs_by_ansible_version actual values."""
    print('--- Validating jobs_by_ansible_version data values ---')
    assert len(json_data['jobs_by_ansible_version']) >= 1, 'Should have at least 1 ansible_version group'
    ansible_version_entry = json_data['jobs_by_ansible_version'][0]
    assert 'ansible_version' in ansible_version_entry, 'Should have ansible_version field'
    assert ansible_version_entry['ansible_version'] is not None, 'ansible_version should not be None'
    if ansible_version_entry['ansible_version'] != 'None':
        assert _is_valid_version(ansible_version_entry['ansible_version']), (
            f'ansible_version should contain numbers and dots, got {ansible_version_entry["ansible_version"]}'
        )
    assert ansible_version_entry['jobs_total'] >= 1, 'Should have at least 1 job in ansible_version group'
    assert 'launch_type_manual_total' not in ansible_version_entry
    assert 'launch_type_scheduled_total' not in ansible_version_entry


def _validate_job_statistics_match(json_data, statistics):
    """Validate new job statistics match sum from jobs_by_job_type."""
    print('--- Validating job statistics match jobs_by_job_type sums ---')
    if not json_data['jobs_by_job_type']:
        return

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


def _validate_totals_match(json_data, statistics):
    """Verify totals match between all groupings."""
    total_jobs_by_job_type = sum(j.get('jobs_total', 0) for j in json_data['jobs_by_job_type'])
    total_jobs_by_launch_type = sum(j.get('jobs_total', 0) for j in json_data['jobs_by_launch_type'])
    total_jobs_by_ansible_version = sum(j.get('jobs_total', 0) for j in json_data['jobs_by_ansible_version'])
    assert total_jobs_by_job_type == total_jobs_by_launch_type == total_jobs_by_ansible_version == statistics['rollup_period_jobs_total'], (
        f'Total jobs should match: jobs_by_job_type={total_jobs_by_job_type}, '
        f'jobs_by_launch_type={total_jobs_by_launch_type}, jobs_by_ansible_version={total_jobs_by_ansible_version}, '
        f'statistics={statistics["rollup_period_jobs_total"]}'
    )


def _validate_cross_section_consistency(json_data, statistics):
    """Validate cross-section data consistency."""
    print('--- Validating cross-section data consistency ---')
    for module_stat in json_data['module_stats']:
        assert module_stat['unique_hosts_total'] <= statistics['rollup_period_unique_hosts_automated_total'], (
            f'Module {module_stat["module_name"][:50]} hosts should not exceed total automated hosts'
        )


def _validate_credentials(json_data):
    """Validate credentials data values."""
    print('--- Validating credentials data values ---')
    assert 'rollup_period_credential_types' in json_data, 'Should have rollup_period_credential_types at top level'
    credential_types = json_data['rollup_period_credential_types']
    assert isinstance(credential_types, list), 'rollup_period_credential_types should be a list'
    assert 'Amazon Web Services' in credential_types
    assert 'Machine' in credential_types
    assert 'Network' in credential_types
    assert 'Vault' in credential_types
    assert len(credential_types) == 4, f'Should have 4 unique credential types, got {len(credential_types)}'
    assert credential_types == sorted(credential_types), 'credential_types should be sorted'


def _validate_table_metadata_structure(json_data):
    """Validate table_metadata structure."""
    print('--- Validating table_metadata structure ---')
    assert 'table_metadata' in json_data, 'Should have table_metadata at top level'
    table_metadata = json_data['table_metadata']

    # table_metadata should always be a dictionary (can be empty if no data)
    assert isinstance(table_metadata, dict), 'table_metadata should be a dictionary'

    # If table_metadata has data, validate the structure
    # Keys should follow pattern: {table_name}_{field_name}
    # where field_name is one of: estimated_row_count, total_size_bytes, table_size_bytes, indexes_size_bytes
    if table_metadata:
        expected_field_suffixes = ['estimated_row_count', 'total_size_bytes', 'table_size_bytes', 'indexes_size_bytes']

        # Group keys by table name (extract table name from key)
        table_names = set()
        for key in table_metadata.keys():
            # Key format: {table_name}_{field_name}
            # Find the last underscore to split table name from field name
            parts = key.rsplit('_', 1)
            if len(parts) == 2:
                table_name = parts[0]
                field_suffix = parts[1]
                if field_suffix in expected_field_suffixes:
                    table_names.add(table_name)

        # For each table, verify all expected fields exist
        for table_name in table_names:
            for field_suffix in expected_field_suffixes:
                key = f'{table_name}_{field_suffix}'
                assert key in table_metadata, f'Should have {key} in table_metadata'
                assert isinstance(table_metadata[key], int), f'{key} should be an integer'


def _validate_table_metadata_values(json_data):
    """Validate table_metadata actual values (only structure, not specific values)."""
    print('--- Validating table_metadata data values ---')
    table_metadata = json_data.get('table_metadata', {})

    # Values will vary, so we only validate structure here
    # The structure validation is already done in _validate_table_metadata_structure
    # This function is kept for consistency but doesn't validate specific values
    if not table_metadata:
        return

    # Just verify that if there's data, it has the expected structure
    # (already validated in _validate_table_metadata_structure)
    assert isinstance(table_metadata, dict), 'table_metadata should be a dictionary'


def _validate_jobs_by_controller_version(json_data, statistics):
    """Validate jobs_by_controller_version: 1 item, correct stats, correct controller_version."""
    print('--- Validating jobs_by_controller_version data values ---')
    ctrl_summary_list = json_data['jobs_by_controller_version']
    assert isinstance(ctrl_summary_list, list), 'jobs_by_controller_version should be a list'
    assert len(ctrl_summary_list) == 1, 'jobs_by_controller_version should contain exactly 1 item'

    ctrl_summary = ctrl_summary_list[0]

    # controller_version should be the first (smallest) version from the sorted controller_versions list
    controller_versions = json_data.get('controller_versions', [])
    expected_controller_version = controller_versions[0] if controller_versions else None
    assert ctrl_summary.get('controller_version') == expected_controller_version, (
        f'Expected controller_version {expected_controller_version!r}, got {ctrl_summary.get("controller_version")!r}'
    )

    # Totals must match the overall statistics (summary covers all jobs)
    assert ctrl_summary['jobs_total'] == statistics['rollup_period_jobs_total'], (
        f'jobs_total should match statistics: expected {statistics["rollup_period_jobs_total"]}, got {ctrl_summary["jobs_total"]}'
    )
    assert ctrl_summary['jobs_failed_total'] == statistics['rollup_period_jobs_failed'], (
        f'jobs_failed_total should match statistics: expected {statistics["rollup_period_jobs_failed"]}, got {ctrl_summary["jobs_failed_total"]}'
    )
    assert ctrl_summary['jobs_successful_total'] == statistics['rollup_period_jobs_successful'], (
        f'jobs_successful_total should match statistics: '
        f'expected {statistics["rollup_period_jobs_successful"]}, got {ctrl_summary["jobs_successful_total"]}'
    )

    # Duration and waiting totals must also match the known multi-hour values
    assert ctrl_summary['jobs_duration_total_seconds'] == pytest.approx(720.0, rel=1e-6), (
        f'jobs_duration_total_seconds should be 720s (390+330), got {ctrl_summary["jobs_duration_total_seconds"]}'
    )
    assert ctrl_summary['job_duration_minimum_seconds'] == pytest.approx(80.0, rel=1e-6)
    assert ctrl_summary['job_duration_maximum_seconds'] == pytest.approx(180.0, rel=1e-6)
    assert ctrl_summary['job_waiting_time_total_seconds'] == pytest.approx(120.0, rel=1e-6), (
        f'job_waiting_time_total_seconds should be 120s (60+60), got {ctrl_summary["job_waiting_time_total_seconds"]}'
    )

    # Required fields
    for field in [
        'jobs_total',
        'jobs_failed_total',
        'jobs_successful_total',
        'jobs_never_started_total',
        'templates_total',
        'inventories_total',
        'jobs_duration_total_seconds',
        'job_duration_maximum_seconds',
        'job_duration_minimum_seconds',
        'job_waiting_time_total_seconds',
        'job_waiting_time_maximum_seconds',
        'job_waiting_time_minimum_seconds',
        'ansible_versions',
    ]:
        assert field in ctrl_summary, f'Should have {field} in jobs_by_controller_version item'
    assert isinstance(ctrl_summary['ansible_versions'], list), 'ansible_versions should be a list'
    assert len(ctrl_summary['ansible_versions']) > 0, 'ansible_versions should not be empty'


def _validate_feature_flags(json_data):
    """Validate feature_flags structure and values."""
    print('--- Validating feature_flags data values ---')
    assert 'feature_flags' in json_data, 'Should have feature_flags at top level'
    feature_flags = json_data['feature_flags']
    assert isinstance(feature_flags, list), 'feature_flags should be a list'

    # All entries must be strings (flag names)
    for flag in feature_flags:
        assert isinstance(flag, str), f'Each feature flag should be a string, got {type(flag)}'
        assert flag.startswith('FEATURE_'), f'Feature flag name should start with FEATURE_, got {flag}'

    # Based on test data seeded by dab_feature_flags.sql, two flags are enabled
    expected_flags = ['FEATURE_ANALYTICS_ENABLED', 'FEATURE_INDIRECT_NODE_COUNTING_ENABLED']
    assert len(feature_flags) == len(expected_flags), f'Should have {len(expected_flags)} feature flags, got {len(feature_flags)}'
    assert set(feature_flags) == set(expected_flags), f'Feature flags should match expected set. Expected: {expected_flags}, Got: {feature_flags}'

    # Disabled flag must not appear
    assert 'FEATURE_SOME_DISABLED_FLAG' not in feature_flags, 'Disabled flag FEATURE_SOME_DISABLED_FLAG should not be in the rollup'


def _validate_controller_versions(json_data):
    """Validate controller_versions structure and values."""
    print('--- Validating controller_versions data values ---')
    assert 'controller_versions' in json_data, 'Should have controller_versions at top level'
    controller_versions = json_data['controller_versions']
    assert isinstance(controller_versions, list), 'controller_versions should be a list'

    # Validate that all versions are valid version strings
    for version in controller_versions:
        assert isinstance(version, str), f'Each controller version should be a string, got {type(version)}'
        assert _is_valid_version(version), f'Controller version should contain numbers and dots, got {version}'

    # Validate that versions are sorted (as per controller_version_service collector)
    assert controller_versions == sorted(controller_versions), 'controller_versions should be sorted in ascending order'

    # Based on test data, we expect specific versions
    expected_versions = ['1.0', '23.5.0', '24.1.0', '24.2.0', '4.7.2']
    assert len(controller_versions) == len(expected_versions), (
        f'Should have {len(expected_versions)} controller versions, got {len(controller_versions)}'
    )
    assert set(controller_versions) == set(expected_versions), (
        f'Controller versions should match expected set. Expected: {expected_versions}, Got: {controller_versions}'
    )


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


def _collect_time_series_data(collector_func, collector_name, time_intervals, db):
    """Collect data from a time-series collector for multiple intervals."""
    dataframes = []
    for since, until in time_intervals:
        try:
            df = collector_func(db=db, since=since, until=until).gather()
            dataframes.append(df if df is not None else pd.DataFrame())
        except Exception as e:
            raise RuntimeError(f'Error collecting {collector_name} for interval {since} to {until}') from e
    return dataframes


def _collect_snapshot_data(collector_func, collector_name, db):
    """Collect data from a snapshot collector."""
    try:
        df = collector_func(db=db).gather()
        return [df] if df is not None else [pd.DataFrame()]
    except Exception as e:
        raise RuntimeError(f'Error collecting {collector_name}') from e


def _collect_data_from_collectors(collectors, time_intervals, db):
    """Collect data from all collectors for the given time intervals."""
    results: dict[str, list[pd.DataFrame]] = {}

    for collector_name, collector_info in collectors.items():
        print(f'Collecting {collector_name}...')

        # Allow individual collectors to override the default DB connection.
        collector_db = collector_info.get('db', db)

        if collector_info['needs_since_until']:
            dataframes = _collect_time_series_data(collector_info['func'], collector_name, time_intervals, collector_db)
        else:
            dataframes = _collect_snapshot_data(collector_info['func'], collector_name, collector_db)

        results[collector_name] = dataframes
        print(f'  Collected {len(dataframes)} dataframe(s)')

    return results


def _prepare_input_data(results, collector_to_input_key):
    """Prepare input_data dict from collected results."""
    input_data = {}

    for collector_name, dataframes in results.items():
        input_key = collector_to_input_key.get(collector_name)
        if input_key:
            # Filter out None dataframes, but keep empty dataframes (they're valid)
            valid_dataframes = [df for df in dataframes if df is not None]
            if valid_dataframes:
                input_data[input_key] = valid_dataframes
            else:
                # If no valid dataframes, pass a single empty dataframe
                input_data[input_key] = [pd.DataFrame()]

    return input_data


def _save_json_output(json_data, since, until):
    """Save JSON data to the expected output path."""
    json_path = f'./out/rollups/{since.year}/{since.month}/{since.day}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d")}.json'

    # create the dir
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        json.dump(json_data, f, indent=4)


def _validate_indirect_managed_nodes(json_data, statistics):
    """Validate indirect managed node count and confirm IDs are not in the output."""
    print('--- Validating indirect_managed_nodes data values ---')
    assert isinstance(statistics['rollup_period_indirect_managed_nodes_all_total'], int), (
        'rollup_period_indirect_managed_nodes_all_total should be an integer'
    )
    # 10:00h window: host_ids 1, 2 (2 unique)
    # 11:00h window: host_ids 2, 3 (2 is a duplicate, 3 is new)
    # Unique across both windows: 1, 2, 3 = 3
    assert statistics['rollup_period_indirect_managed_nodes_all_total'] == 3, (
        f'Expected 3 unique indirect managed nodes (host_ids 1,2,3 deduplicated across windows), '
        f'got {statistics["rollup_period_indirect_managed_nodes_all_total"]}'
    )
    assert 'indirect_managed_nodes' not in json_data, 'Host IDs must not be included in the final JSON payload (privacy requirement)'


def _validate_all_data(json_data, statistics):
    """Run all validation checks on the json_data."""
    # Validate structure
    _validate_top_level_structure(json_data)
    _validate_statistics_structure(statistics)
    _validate_statistics_data_types(statistics)
    _validate_arrays_structure(json_data)
    _validate_module_stats_structure(json_data)
    _validate_collection_stats_structure(json_data)
    _validate_jobs_by_job_type_structure(json_data)
    _validate_jobs_by_launch_type_structure(json_data)
    _validate_jobs_by_ansible_version_structure(json_data)

    # Validate actual data values and relationships
    print('\n--- Validating statistics data values ---')
    assert statistics['rollup_period_modules_total'] == 2, 'Should have 2 modules (ansible.builtin.yum and a10.acos_axapi.a10_slb_virtual_server)'
    assert statistics['rollup_period_unique_hosts_automated_total'] == 2, 'Should have 2 hosts automated'
    assert len(json_data['module_stats']) == 2, 'Should have 2 module stats'
    assert len(json_data['collection_stats']) == 2, 'Should have 2 collection stats (ansible.builtin and a10.acos_axapi)'

    # Note: module_stats and collection_stats validations will need updates for 6 jobs total
    # (3 from 10:00 hour + 3 from 11:00 hour)
    _validate_module_stats_values_multi_hour(json_data)
    _validate_collection_stats_values_multi_hour(json_data)
    _validate_role_stats_and_jobs_by_installed_collections_versions(json_data)

    print('--- Validating playbooks_total ---')
    assert statistics['rollup_period_playbooks_total'] == 1, 'Should have 1 total playbook'

    print('--- Validating execution_environments data values ---')
    assert statistics['rollup_period_execution_environments_total'] == 2, 'Should have 2 total execution environments'
    assert statistics['rollup_period_EE_default_total'] == 1, 'Should have 1 default execution environment'
    assert statistics['rollup_period_EE_custom_total'] == 1, 'Should have 1 custom execution environment'
    assert (
        statistics['rollup_period_execution_environments_total']
        == statistics['rollup_period_EE_default_total'] + statistics['rollup_period_EE_custom_total']
    ), 'Total EE should equal default + custom'

    _validate_jobs_values_multi_hour(json_data, statistics)
    _validate_job_host_summary_values_multi_hour(json_data, statistics)
    _validate_jobs_by_launch_type_values(json_data)
    _validate_jobs_by_ansible_version_values(json_data)
    _validate_totals_match(json_data, statistics)
    _validate_job_statistics_match(json_data, statistics)
    _validate_cross_section_consistency(json_data, statistics)
    _validate_credentials(json_data)
    _validate_table_metadata_structure(json_data)
    _validate_table_metadata_values(json_data)
    _validate_controller_versions(json_data)
    _validate_jobs_by_controller_version(json_data, statistics)
    _validate_feature_flags(json_data)
    _validate_indirect_managed_nodes(json_data, statistics)

    print('✅ All data value assertions passed!')


def test_from_gather_to_json(cleanup_glob):
    """
    Test collecting data from collectors for two hourly intervals (10:00-11:00 and 11:00-12:00)
    and computing anonymized rollup from raw data.
    """
    # Define collectors similar to run_no_events.py
    COLLECTORS = {
        'unified_jobs': {
            'func': unified_jobs,
            'needs_since_until': True,
        },
        'job_host_summary_service': {
            'func': job_host_summary_service,
            'needs_since_until': True,
        },
        'credentials_service': {
            'func': credentials_service,
            'needs_since_until': True,
        },
        'main_jobevent_service': {
            'func': main_jobevent_service,
            'needs_since_until': True,
        },
        'execution_environments': {
            'func': execution_environments,
            'needs_since_until': False,  # snapshot collector
        },
        'table_metadata': {
            'func': table_metadata,
            'needs_since_until': False,  # snapshot collector
        },
        'controller_version_service': {
            'func': controller_version_service,
            'needs_since_until': False,  # snapshot collector
        },
        'feature_flags_service': {
            'func': feature_flags_service,
            'needs_since_until': False,  # snapshot collector
        },
        'main_indirectmanagednodeaudit': {
            'func': main_indirectmanagednodeaudit,
            'needs_since_until': True,
        },
    }

    # Define two hourly intervals: 10:00-11:00 and 11:00-12:00
    time_intervals = [
        (utcdt('2025-06-13T10:00:00'), utcdt('2025-06-13T11:00:00')),
        (utcdt('2025-06-13T11:00:00'), utcdt('2025-06-13T12:00:00')),
    ]

    # Map collector names to input_data keys expected by compute_anonymized_rollup_from_raw_data
    collector_to_input_key = {
        'unified_jobs': 'unified_jobs',
        'job_host_summary_service': 'job_host_summary',
        'credentials_service': 'credentials',
        'main_jobevent_service': 'main_jobevent',
        'execution_environments': 'execution_environments',
        'table_metadata': 'table_metadata',
        'controller_version_service': 'controller_version',
        'feature_flags_service': 'feature_flags',
        'main_indirectmanagednodeaudit': 'indirect_managed_nodes',
    }

    # Collect data from all collectors
    results = _collect_data_from_collectors(COLLECTORS, time_intervals, connection)

    # Prepare input_data dict
    input_data = _prepare_input_data(results, collector_to_input_key)

    # Compute anonymized rollup from raw data
    salt = 'salt'
    print('Computing anonymized rollup from collected data...')
    json_data = compute_anonymized_rollup_from_raw_data(input_data, salt)
    print('✓ Anonymized rollup computed successfully')

    # Save JSON output
    since = utcdt('2025-06-13T10:00:00')
    until = utcdt('2025-06-13T12:00:00')
    _save_json_output(json_data, since, until)

    # Validate all data
    statistics = json_data['statistics']
    _validate_all_data(json_data, statistics)
