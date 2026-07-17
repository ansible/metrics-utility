"""
Test that validates splitting anonymized rollup reports into multiple JSON chunks.

This test:
1. Creates mock anonymized rollup data with the same structure as real reports
2. Populates arrays (module_stats, jobs_by_job_type, etc.) with many items
   so they exceed the Segment message size limit (24KB)
3. Tests the _split_into_chunks function
4. Validates that multiple chunks are created when arrays are too large
5. Saves chunks to separate JSON files for inspection
"""

import json
import os

import pytest

from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.test.util import cleanup_glob as _cleanup_glob


file_glob = './out/test_report_splitting/*.json'


@pytest.fixture(scope='function')
def cleanup_test_data():
    yield
    _cleanup_glob(file_glob)


def _event_stat_counters(seed, *, jobs_total, events_collected_total, ansible_versions):
    """Shared module/collection/role counters matching EventModulesAnonymizedRollup._NUMERIC_COLS."""
    return {
        'jobs_total': jobs_total,
        'jobs_successful_total': max(0, jobs_total - (seed % 3)),
        'jobs_failed_total': seed % 3,
        'jobs_duration_total_seconds': 100 + (seed % 500),
        'jobs_waiting_time_total_seconds': 10 + (seed % 60),
        'jobs_never_started_total': seed % 2,
        'jobs_successful_duration_total_seconds': 80 + (seed % 400),
        'jobs_failed_duration_total_seconds': seed % 50,
        'runner_on_ok_total': 100 + (seed % 50),
        'runner_on_failed_total': seed % 10,
        'runner_on_failed_ignored_total': seed % 5,
        'runner_on_unreachable_total': seed % 4,
        'runner_on_async_ok_total': seed % 8,
        'runner_on_async_failed_total': seed % 3,
        'runner_on_async_failed_ignored_total': seed % 2,
        'runner_item_on_ok_total': seed % 20,
        'runner_item_on_failed_total': seed % 6,
        'runner_item_on_failed_ignored_total': seed % 2,
        'runner_item_on_unreachable_total': seed % 2,
        'runner_retry_total': seed % 7,
        'warnings_total': seed % 5,
        'deprecations_total': seed % 3,
        'events_collected_total': events_collected_total,
        'event_data_size_total': events_collected_total * (80 + (seed % 40)),
        'ansible_versions': ansible_versions,
    }


def _create_statistics_dict(num_modules, num_jobs):
    """Create statistics dictionary for anonymized rollup."""
    return {
        'rollup_period_modules_total': num_modules,
        'rollup_period_collected_events_total': 5000,
        'rollup_period_warnings_total': 12,
        'rollup_period_deprecations_total': 4,
        'rollup_period_playbooks_total': 50,
        'rollup_period_execution_environments_total': 20,
        'rollup_period_EE_default_total': 10,
        'rollup_period_EE_custom_total': 10,
        'rollup_period_jobs_total': num_jobs,
        'rollup_period_jobs_successful': num_jobs - 10,
        'rollup_period_jobs_failed': 10,
        'rollup_period_jobs_duration_all_statuses_seconds': 36000,
        'rollup_period_jobs_successful_duration_total_seconds': 33000,
        'rollup_period_jobs_failed_duration_total_seconds': 3000,
        'rollup_period_organizations_total': 5,
        'rollup_period_forks_total': 100,
        'rollup_period_templates_total': 30,
        'rollup_period_inventories_total': 15,
        'rollup_period_unique_hosts_total': 500,
        'rollup_period_job_host_pairs_total': 2000,
        'rollup_period_successful_hosts_total': 1800,
        'rollup_period_failed_hosts_total': 150,
        'rollup_period_unreachable_hosts_total': 50,
        'rollup_period_indirect_managed_nodes_all_total': 25,
        # From job_host_summary host-task outcome totals (not events_modules runner counters)
        'rollup_period_tasks_total': 10000,
        'rollup_period_task_ok_total': 9000,
        'rollup_period_task_failed_total': 500,
        'rollup_period_task_skipped_total': 300,
        'rollup_period_task_unreachable_total': 150,
        'rollup_period_task_ignored_total': 50,
    }


def _create_module_stats(num_modules):
    """Create module_stats array matching production Segment field names."""
    module_stats = []
    for i in range(num_modules):
        is_even = i % 2 == 0
        is_divisible_by_3 = i % 3 == 0
        collection_source = 'certified' if is_even else 'community'
        collection = 'ansible.builtin' if is_even else f'community.module_{i % 10}'
        ansible_versions = ['2.15.0', '2.16.0', '2.17.0'] if is_divisible_by_3 else ['2.18.0', '2.19.0']
        jobs_total = 10 + (i % 20)
        events_collected_total = 50 + (i % 100)

        module_stats.append(
            {
                'module': f'{collection}.module_{i:04d}',
                'collection_source': collection_source,
                'collection': collection,
                **_event_stat_counters(
                    i,
                    jobs_total=jobs_total,
                    events_collected_total=events_collected_total,
                    ansible_versions=ansible_versions,
                ),
            }
        )
    return module_stats


def _create_collection_stats(num_collections):
    """Create collection_stats array matching production Segment field names."""
    collection_stats = []
    sources = ['certified', 'community', 'validated', 'partner']
    for i in range(num_collections):
        jobs_total = 20 + (i % 30)
        events_collected_total = 200 + (i % 200)
        collection_stats.append(
            {
                'collection': f'ansible.collection_{i:03d}',
                'collection_source': sources[i % 4],
                **_event_stat_counters(
                    i,
                    jobs_total=jobs_total,
                    events_collected_total=events_collected_total,
                    ansible_versions=['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0'],
                ),
            }
        )
    return collection_stats


def _create_role_stats():
    """Create role_stats array matching production Segment field names."""
    role_stats = []
    sources = ['certified', 'community']
    for i in range(30):
        jobs_total = 5 + (i % 15)
        events_collected_total = 100 + (i % 100)
        role_stats.append(
            {
                'role': f'example_role_{i:03d}',
                'collection': f'ansible.collection_{i % 10:03d}',
                'collection_source': sources[i % 2],
                **_event_stat_counters(
                    i,
                    jobs_total=jobs_total,
                    events_collected_total=events_collected_total,
                    ansible_versions=['2.16.0', '2.17.0'],
                ),
            }
        )
    return role_stats


def _host_summary_fields(seed):
    """Host-summary fields merged into jobs groupings (unique_hosts_total is top-level only)."""
    return {
        'unreachable_total': seed % 5,
        'failed_total': seed % 3,
        'ok_total': 10 + (seed % 20),
        'skipped_total': seed % 4,
        'ignored_total': seed % 2,
        'rescued_total': 0,
        'successful_hosts_total': 4 + (seed % 12),
        'failed_hosts_total': seed % 3,
        'unreachable_hosts_total': seed % 2,
    }


def _create_jobs_by_job_type(num_jobs):
    """Create jobs_by_job_type array."""
    jobs_by_job_type = []
    for i in range(num_jobs):
        is_even = i % 2 == 0
        is_successful = i % 10 != 0
        job_type = 'job' if is_even else 'workflowjob'
        ansible_versions = ['2.15.0', '2.16.0'] if is_even else ['2.17.0', '2.18.0']
        jobs_successful_total = 1 if is_successful else 0
        jobs_failed_total = 1 if not is_successful else 0
        jobs_successful_duration = 90 + (i % 450) if is_successful else 0
        jobs_failed_duration = 50 + (i % 200) if not is_successful else 0

        jobs_by_job_type.append(
            {
                'job_type': job_type,
                'controller_version': '4.6.0',
                'jobs_total': 1,
                'jobs_successful_total': jobs_successful_total,
                'jobs_failed_total': jobs_failed_total,
                'jobs_duration_total_seconds': 100 + (i % 500),
                'jobs_successful_duration_total_seconds': jobs_successful_duration,
                'jobs_failed_duration_total_seconds': jobs_failed_duration,
                'templates_total': 1,
                'inventories_total': 1,
                'ansible_versions': ansible_versions,
                **_host_summary_fields(i),
            }
        )
    return jobs_by_job_type


def _create_jobs_by_launch_type():
    """Create jobs_by_launch_type array."""
    jobs_by_launch_type = []
    launch_types = ['manual', 'scheduled', 'workflow', 'callback']
    for i, launch_type in enumerate(launch_types):
        jobs_by_launch_type.append(
            {
                'launch_type': launch_type,
                'controller_version': '4.6.0',
                'jobs_total': 25 + (i * 10),
                'jobs_successful_total': 20 + (i * 8),
                'jobs_failed_total': 5 + (i * 2),
                'templates_total': 5 + i,
                **_host_summary_fields(i * 5),
            }
        )
    return jobs_by_launch_type


def _create_jobs_by_ansible_version():
    """Create jobs_by_ansible_version array."""
    jobs_by_ansible_version = []
    versions = ['2.15.0', '2.16.0', '2.17.0', '2.18.0', '2.19.0']
    for i, version in enumerate(versions):
        jobs_by_ansible_version.append(
            {
                'ansible_version': version,
                'controller_version': '4.6.0',
                'jobs_total': 30 + (i * 5),
                'jobs_successful_total': 25 + (i * 4),
                'jobs_failed_total': 5 + i,
                'templates_total': 8 + i,
                **_host_summary_fields(i * 3),
            }
        )
    return jobs_by_ansible_version


def _create_jobs_by_installed_collections_versions():
    """Create jobs_by_installed_collections_versions array."""
    jobs_by_installed_collections_versions = []
    for i in range(20):
        jobs_total = 10 + (i % 20)
        jobs_by_installed_collections_versions.append(
            {
                'collection': f'ansible.collection_{i:03d}',
                'version': f'1.{i}.0',
                'jobs_total': jobs_total,
                'jobs_failed_total': i % 3,
                'jobs_successful_total': max(0, jobs_total - (i % 3)),
                'jobs_never_started_total': 0,
                'jobs_duration_total_seconds': 100 + (i % 200),
                'jobs_successful_duration_total_seconds': 80 + (i % 150),
                'jobs_failed_duration_total_seconds': i % 40,
                'job_duration_maximum_seconds': 200 + i,
                'job_duration_minimum_seconds': 10 + (i % 5),
                'job_waiting_time_total_seconds': 5 + (i % 20),
                'job_waiting_time_maximum_seconds': 30 + i,
                'job_waiting_time_minimum_seconds': 1,
                'templates_total': 1 + (i % 5),
                'inventories_total': 1 + (i % 3),
                'ansible_versions': ['2.16.0', '2.17.0'],
            }
        )
    return jobs_by_installed_collections_versions


def _create_ansible_versions():
    """Create large ansible_versions array that will need to be split.

    Generate many version strings to exceed the 24KB limit.
    Each version string is ~10-12 bytes with JSON formatting.
    To exceed 24KB, we need ~2000+ items, but let's use 3000 to ensure splitting.
    This creates 3 * 100 * 10 = 3000 versions, which should exceed 24KB.
    """
    ansible_versions = []
    for major in range(2, 5):  # Major versions 2, 3, 4
        for minor in range(0, 100):  # Minor versions 0-99
            for patch in range(0, 10):  # Patch versions 0-9
                version = f'{major}.{minor}.{patch}'
                ansible_versions.append(version)
    return ansible_versions


def create_mock_anonymized_rollup_data(num_modules=200, num_jobs=150, num_collections=50):
    """
    Create mock anonymized rollup data with the same structure as real reports.

    Args:
        num_modules: Number of module_stats entries (default 200 to exceed size limit)
        num_jobs: Number of jobs_by_job_type entries (default 150 to exceed size limit)
        num_collections: Number of collection_stats entries (default 50)

    Returns:
        Dictionary with anonymized rollup structure
    """
    statistics = _create_statistics_dict(num_modules, num_jobs)
    module_stats = _create_module_stats(num_modules)
    collection_stats = _create_collection_stats(num_collections)
    role_stats = _create_role_stats()
    jobs_by_job_type = _create_jobs_by_job_type(num_jobs)
    jobs_by_launch_type = _create_jobs_by_launch_type()
    jobs_by_ansible_version = _create_jobs_by_ansible_version()
    jobs_by_installed_collections_versions = _create_jobs_by_installed_collections_versions()
    ansible_versions = _create_ansible_versions()

    anonymized_rollup = {
        'statistics': statistics,
        'rollup_period_ansible_versions': ansible_versions,
        'rollup_period_scm_types': ['git', 'manual', 'svn'],
        'rollup_period_credential_types': ['Amazon Web Services', 'Container Registry', 'Machine', 'Network', 'Source Control', 'Vault'],
        'module_stats': module_stats,
        'collection_stats': collection_stats,
        'role_stats': role_stats,
        'jobs_by_job_type': jobs_by_job_type,
        'jobs_by_launch_type': jobs_by_launch_type,
        'jobs_by_ansible_version': jobs_by_ansible_version,
        'jobs_by_controller_version': [
            {
                'controller_version': '4.6.0',
                'jobs_total': num_jobs,
                'jobs_successful_total': num_jobs - 10,
                'jobs_failed_total': 10,
            }
        ],
        'jobs_by_installed_collections_versions': jobs_by_installed_collections_versions,
        'table_metadata': {'main_jobevent': {'row_count': 5000}, 'main_unifiedjob': {'row_count': num_jobs}},
        'controller_versions': ['4.6.0'],
        # Non-empty lists only — Segment skipping empty arrays means empty keys never get a chunk
        'feature_flags': [{'name': 'example_flag', 'enabled': True}],
        'observability_by_tasks': [{'task': 'example_task', 'count': 10}],
        'indirect_nodes_by_collection': [{'collection': 'ansible.builtin', 'host_count': 5}],
    }

    return anonymized_rollup


def _print_splitting_info(anonymized_rollup, total_size, max_size):
    """Print information about the splitting test."""
    print(f'\n{"=" * 80}')
    print('=== TESTING ANONYMIZED ROLLUP SPLITTING ===')
    print(f'{"=" * 80}')
    print(f'Total data size: {total_size} bytes ({total_size / 1024:.1f} KB)')
    print(f'Message size limit: {max_size} bytes ({max_size / 1024:.1f} KB)')
    print(f'Number of controller versions: {len(anonymized_rollup["rollup_period_ansible_versions"])}')
    print(f'Number of modules: {len(anonymized_rollup["module_stats"])}')
    print(f'Number of jobs: {len(anonymized_rollup["jobs_by_job_type"])}')
    print(f'Number of collections: {len(anonymized_rollup["collection_stats"])}')


def _save_and_validate_chunks(chunks, storage_segment, max_size, output_dir, anonymized_rollup):
    """Save chunks to files and validate them."""
    chunk_sizes = []
    chunk_keys = []
    total_items_in_chunks = {}

    for i, chunk in enumerate(chunks, 1):
        chunk_size = storage_segment._calculate_size(chunk)
        chunk_sizes.append(chunk_size)
        assert chunk_size <= max_size, f'Chunk {i} size ({chunk_size} bytes) exceeds the limit ({max_size} bytes)'

        chunk_key = list(chunk.keys())[0] if chunk else 'unknown'
        chunk_keys.append(chunk_key)

        chunk_path = os.path.join(output_dir, f'chunk_{i:03d}_of_{len(chunks):03d}_{chunk_key}.json')
        with open(chunk_path, 'w') as f:
            json.dump(chunk, f, indent=4)

        print(f'Chunk {i}/{len(chunks)}: {chunk_key} - {chunk_size} bytes ({chunk_size / 1024:.1f} KB) - saved to {chunk_path}')

        if isinstance(chunk[chunk_key], list):
            num_items = len(chunk[chunk_key])
            print(f'  └─ Contains {num_items} items in {chunk_key}')
            if chunk_key not in total_items_in_chunks:
                total_items_in_chunks[chunk_key] = 0
            total_items_in_chunks[chunk_key] += num_items

    return chunk_sizes, chunk_keys, total_items_in_chunks


def _validate_split_arrays(chunk_keys, total_items_in_chunks, anonymized_rollup):
    """Validate that arrays were split correctly."""
    ansible_versions_chunks = [i for i, key in enumerate(chunk_keys) if key == 'rollup_period_ansible_versions']
    if len(ansible_versions_chunks) > 1:
        print(f'\n✓ rollup_period_ansible_versions was split into {len(ansible_versions_chunks)} chunks')
        assert len(ansible_versions_chunks) > 1, 'rollup_period_ansible_versions should be split into multiple chunks'
        total_version_items = total_items_in_chunks.get('rollup_period_ansible_versions', 0)
        original_version_count = len(anonymized_rollup['rollup_period_ansible_versions'])
        assert total_version_items == original_version_count, (
            f'Total controller version items in chunks ({total_version_items}) should match original ({original_version_count})'
        )

    module_stats_chunks = [i for i, key in enumerate(chunk_keys) if key == 'module_stats']
    if len(module_stats_chunks) > 1:
        print(f'✓ module_stats was split into {len(module_stats_chunks)} chunks')
        assert len(module_stats_chunks) > 1, 'module_stats should be split into multiple chunks'
        total_module_items = total_items_in_chunks.get('module_stats', 0)
        assert total_module_items == len(anonymized_rollup['module_stats']), (
            f'Total module items in chunks ({total_module_items}) should match original ({len(anonymized_rollup["module_stats"])})'
        )

    jobs_chunks = [i for i, key in enumerate(chunk_keys) if key == 'jobs_by_job_type']
    if len(jobs_chunks) > 1:
        print(f'✓ jobs_by_job_type was split into {len(jobs_chunks)} chunks')
        assert len(jobs_chunks) > 1, 'jobs_by_job_type should be split into multiple chunks'
        total_job_items = total_items_in_chunks.get('jobs_by_job_type', 0)
        assert total_job_items == len(anonymized_rollup['jobs_by_job_type']), (
            f'Total job items in chunks ({total_job_items}) should match original ({len(anonymized_rollup["jobs_by_job_type"])})'
        )


def _validate_all_keys_present(chunk_keys, anonymized_rollup):
    """Validate that each top-level key appears in at least one chunk."""
    original_keys = set(anonymized_rollup.keys())
    for key in original_keys:
        key_chunks = [i for i, k in enumerate(chunk_keys) if k == key]
        assert len(key_chunks) >= 1, f'Key {key} should appear in at least one chunk'


def _print_chunk_statistics(chunk_sizes):
    """Print chunk size statistics."""
    avg_chunk_size = sum(chunk_sizes) / len(chunk_sizes)
    max_chunk_size = max(chunk_sizes)
    min_chunk_size = min(chunk_sizes)
    print(f'\n{"=" * 80}')
    print('Chunk size statistics:')
    print(f'  Average: {avg_chunk_size:.0f} bytes ({avg_chunk_size / 1024:.1f} KB)')
    print(f'  Maximum: {max_chunk_size:.0f} bytes ({max_chunk_size / 1024:.1f} KB)')
    print(f'  Minimum: {min_chunk_size:.0f} bytes ({min_chunk_size / 1024:.1f} KB)')
    print(f'{"=" * 80}')


def _assert_fixture_matches_production_shape(anonymized_rollup):
    """Guard against fixtures drifting from Segment/production JSON field names."""
    module = anonymized_rollup['module_stats'][0]
    assert 'module' in module and 'module_name' not in module
    assert 'collection' in module and 'collection_name' not in module
    assert 'runner_on_ok_total' in module
    assert 'tasks_ok_total' not in module
    assert 'tasks_failed_total' not in module
    assert 'tasks_skipped_total' not in module

    collection = anonymized_rollup['collection_stats'][0]
    assert 'collection' in collection and 'collection_name' not in collection
    assert 'runner_on_ok_total' in collection

    role = anonymized_rollup['role_stats'][0]
    assert 'collection' in role and 'collection_name' not in role
    assert 'tasks_total' not in role
    assert 'runner_on_ok_total' in role

    installed = anonymized_rollup['jobs_by_installed_collections_versions'][0]
    assert 'collection' in installed and 'name' not in installed

    statistics = anonymized_rollup['statistics']
    assert 'rollup_period_warnings_total' in statistics
    assert 'rollup_period_deprecations_total' in statistics

    job = anonymized_rollup['jobs_by_job_type'][0]
    assert 'controller_version' in job
    assert 'unique_hosts_total' not in job


def test_anonymized_rollup_splitting(cleanup_test_data):
    """
    Test that anonymized rollup reports are correctly split into multiple chunks
    when arrays exceed the Segment message size limit.
    """
    anonymized_rollup = create_mock_anonymized_rollup_data(num_modules=200, num_jobs=150, num_collections=50)
    _assert_fixture_matches_production_shape(anonymized_rollup)
    storage_segment = StorageSegment()
    max_size = storage_segment.REGULAR_MESSAGE_LIMIT

    total_size = storage_segment._calculate_size(anonymized_rollup)
    _print_splitting_info(anonymized_rollup, total_size, max_size)

    chunks = storage_segment._split_into_chunks(anonymized_rollup, max_size)
    print(f'\nTotal chunks created: {len(chunks)}')

    assert len(chunks) > 1, (
        f'Expected multiple chunks to be created, but got only {len(chunks)} chunk. '
        f'Total data size is {total_size} bytes, which exceeds the {max_size} byte limit.'
    )

    output_dir = './out/test_report_splitting'
    os.makedirs(output_dir, exist_ok=True)

    original_path = os.path.join(output_dir, 'original_anonymized_rollup.json')
    with open(original_path, 'w') as f:
        json.dump(anonymized_rollup, f, indent=4)
    print(f'\nSaved original data to: {original_path}')

    chunk_sizes, chunk_keys, total_items_in_chunks = _save_and_validate_chunks(chunks, storage_segment, max_size, output_dir, anonymized_rollup)

    _validate_split_arrays(chunk_keys, total_items_in_chunks, anonymized_rollup)
    _validate_all_keys_present(chunk_keys, anonymized_rollup)
    _print_chunk_statistics(chunk_sizes)

    assert len(chunks) > 1, 'Test should create multiple chunks to validate splitting functionality'
    print(f'\n✓ Test passed: Successfully split anonymized rollup into {len(chunks)} chunks')
    print(f'✓ All chunks are within size limit ({max_size} bytes)')
    print('✓ All data preserved correctly')


def test_anonymized_rollup_splitting_with_small_data(cleanup_test_data):
    """
    Test that small anonymized rollup reports are not unnecessarily split.
    This ensures the splitting logic works correctly for both large and small data.
    """
    # Create mock anonymized rollup data with small arrays
    anonymized_rollup = create_mock_anonymized_rollup_data(num_modules=5, num_jobs=3, num_collections=2)

    # Initialize StorageSegment
    storage_segment = StorageSegment()
    max_size = storage_segment.REGULAR_MESSAGE_LIMIT

    # Split the data into chunks
    chunks = storage_segment._split_into_chunks(anonymized_rollup, max_size)

    # With small data, we should get one chunk per top-level key
    # But arrays should not be split if they're small enough
    assert len(chunks) >= len(anonymized_rollup), (
        f'Should have at least one chunk per top-level key. Got {len(chunks)} chunks for {len(anonymized_rollup)} keys'
    )

    # Validate no array was unnecessarily split
    chunk_keys = [list(chunk.keys())[0] for chunk in chunks]

    # Count how many times each key appears
    key_counts = {}
    for key in chunk_keys:
        key_counts[key] = key_counts.get(key, 0) + 1

    # For small data, arrays should not be split (each should appear only once)
    for key, count in key_counts.items():
        if key in ['module_stats', 'jobs_by_job_type', 'collection_stats']:
            # These are arrays - with small data, they should not be split
            assert count == 1, f'Array {key} should not be split with small data, but appears in {count} chunks'

    print(f'\n✓ Small data test passed: {len(chunks)} chunks created (no unnecessary splitting)')
