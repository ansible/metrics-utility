import csv
import glob
import os

from unittest.mock import patch

import pytest

from metrics_utility.gather.collection import Collection
from metrics_utility.test.base.functional.helpers import read_tarball
from metrics_utility.test.util import run_gather_ext, run_gather_int


# environment for run_gather_ext
env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

# mock uuid
uuid = '00000000-0000-0000-0000-000000000000'

# where to find the tar.gz
file_glob = f'./metrics_utility/test/test_data/data/*/*/*/{uuid}-*.tar.gz'
file_paths = f'./metrics_utility/test/test_data/data/2025/06/13/{uuid}-*.tar.gz'

# expected CSV content (header + rows)
test_lines = [
    'id,created,modified,host_name,host_remote_id,ansible_host_variable,'
    'ansible_connection_variable,changed,dark,failures,ok,processed,skipped,'
    'failed,ignored,rescued,job_created,job_remote_id,job_template_remote_id,'
    'job_template_name,inventory_remote_id,inventory_name,organization_remote_id,'
    'organization_name,project_remote_id,project_name',
    '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,1,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,1,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '3,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,2,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '4,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,2,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '5,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,1,0,0,0,t,0,0,'
    '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,1,0,0,0,t,0,0,'
    '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '7,2025-06-13 11:00:00+00,2025-06-13 11:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 11:00:00+00,4,1,default_unified_job_11_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '8,2025-06-13 11:00:00+00,2025-06-13 11:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 11:00:00+00,4,1,default_unified_job_11_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '9,2025-06-13 11:00:00+00,2025-06-13 11:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 11:00:00+00,5,1,default_unified_job_11_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '10,2025-06-13 11:00:00+00,2025-06-13 11:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 11:00:00+00,5,1,default_unified_job_11_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '11,2025-06-13 11:00:00+00,2025-06-13 11:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 11:00:00+00,6,1,default_unified_job_11_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '12,2025-06-13 11:00:00+00,2025-06-13 11:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 11:00:00+00,6,1,default_unified_job_11_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
]

# derive expected header and rows
expected_header = test_lines[0].split(',')
expected_rows = [line.split(',') for line in test_lines[1:]]

# identify column names to skip asserting (unstable IDs)
skip_columns = {
    'id',
    'host_remote_id',
    'job_remote_id',
    'job_template_remote_id',
    'inventory_remote_id',
    'organization_remote_id',
    'project_remote_id',
}


def find_csv_in_tarballs(tarball_glob, csv_suffix):
    for file_path in glob.glob(tarball_glob):
        files = read_tarball(file_path)
        match = next((name for name in files if name.endswith(csv_suffix)), None)
        if match is not None:
            return files[match]
    return None


@pytest.fixture
def cleanup_glob():
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command(cleanup_glob):
    """Build xlsx report using build command and test CSV contents."""
    run_gather_ext(env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14', '--force'])

    data = find_csv_in_tarballs(file_paths, 'job_host_summary.csv')
    assert data is not None, 'job_host_summary.csv not found in any tarballs.'

    rows = list(csv.reader(data.decode('utf-8').splitlines()))

    header = rows[0]
    assert header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {header}'

    actual_data = rows[1:]
    assert len(actual_data) == len(expected_rows), f'\nRow count mismatch: expected {len(expected_rows)}, got {len(actual_data)}'

    for i, (expected_row, actual_row) in enumerate(zip(expected_rows, actual_data), start=1):
        for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
            col_name = header[idx]
            if col_name in skip_columns:
                continue

            assert exp_cell == act_cell, (
                f'\nData mismatch on row {i + 1}, column "{col_name}" (index {idx}):\nExpected: {exp_cell!r}\nActual:   {act_cell!r}'
            )


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_disabled_by_env_var(cleanup_glob):
    """Test that job_host_summary.csv is not generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is set to 'true'."""
    disabled_env_vars = {**env_vars, 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true'}

    rg = run_gather_ext(disabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Skipping job_host_summary' in rg.stderr

    assert find_csv_in_tarballs(file_paths, 'job_host_summary.csv') is None, (
        'job_host_summary.csv should not be generated when collector is disabled.'
    )


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_enabled_explicitly(cleanup_glob):
    """Test that job_host_summary.csv is generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is explicitly set to 'false'."""
    enabled_env_vars = {**env_vars, 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'false'}

    run_gather_ext(enabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    assert find_csv_in_tarballs(file_paths, 'job_host_summary.csv') is not None, (
        'job_host_summary.csv should be generated when collector is explicitly enabled.'
    )


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_case_insensitive_disable(cleanup_glob):
    """Test that the environment variable check is case insensitive for 'true' values."""
    for test_value in ['TRUE', 'True', 'tRuE']:
        disabled_env_vars = {**env_vars, 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': test_value}

        run_gather_ext(disabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

        assert find_csv_in_tarballs(file_paths, 'job_host_summary.csv') is None, (
            f'job_host_summary.csv should not be generated when collector is disabled with value "{test_value}".'
        )

        for file in glob.glob(file_glob):
            os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_invalid_values_still_enabled(cleanup_glob):
    """Test that job_host_summary.csv is still generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is set to invalid values."""
    for test_value in ['yes', 'no', '0', 'enabled', 'disabled', 'random_text', '']:
        test_env_vars = {**env_vars, 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': test_value}

        run_gather_ext(test_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

        assert find_csv_in_tarballs(file_paths, 'job_host_summary.csv') is not None, (
            f'job_host_summary.csv should be generated when collector has invalid disable value "{test_value}".'
        )

        for file in glob.glob(file_glob):
            os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_main_host_collection_trailing_comma(cleanup_glob):
    main_host_collection(cleanup_glob, collectors='main_jobevent,main_host', trailing_comma=True)
    main_host_collection(cleanup_glob, collectors='main_jobevent', trailing_comma=True)
    main_host_collection(cleanup_glob, collectors='main_host', trailing_comma=True)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_main_host_collection_no_trailing_comma(cleanup_glob):
    main_host_collection(cleanup_glob, collectors='main_jobevent,main_host', trailing_comma=False)
    main_host_collection(cleanup_glob, collectors='main_jobevent', trailing_comma=False)
    main_host_collection(cleanup_glob, collectors='main_host', trailing_comma=False)


def main_host_collection(cleanup_glob, collectors='main_jobevent,main_host', trailing_comma=False):
    """Test that main_host table collection runs without error and all collections have 'ok' status."""
    # Enable main_host collection by adding it to optional collectors
    env_vars_with_main_host = env_vars.copy()
    env_vars_with_main_host['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = collectors

    if trailing_comma:
        env_vars_with_main_host['METRICS_UTILITY_OPTIONAL_COLLECTORS'] += ','

    # Track collections and their statuses
    collection_statuses = {}

    # Mock the Collection.gather method to capture success/failure status
    original_collection_gather = Collection.gather

    def mock_collection_gather(self):
        """Mock collection gather to capture statuses."""
        # Call the original method
        result = original_collection_gather(self)

        # Capture the status
        collection_name = getattr(self, 'filename', 'unknown')
        collection_statuses[collection_name] = self.gathering_successful

        return result

    with patch.object(Collection, 'gather', mock_collection_gather):
        # Run the gather command
        run_gather_int(
            env_vars_with_main_host,
            {
                'ship': True,
                'since': '2025-06-12',
                'until': '2025-06-14',
            },
        )

    # Check collection statuses
    print('\nCollection statuses:')
    expected_collections = {'job_host_summary.csv', 'main_jobevent.csv', 'main_host.csv'}
    errors_found = []

    for collection_name, status in collection_statuses.items():
        status_str = 'ok' if status else 'failed'
        print(f'  {collection_name}: {status_str}')

        if not status:
            errors_found.append(f"Collection '{collection_name}' failed")

    # Check if there were any errors
    if errors_found:
        assert False, 'Found errors in collections:\n' + '\n'.join(errors_found)

    # Check if all expected collections were seen
    collected_names = set(collection_statuses.keys())
    missing_collections = expected_collections - collected_names

    # Note: Some collections might have different names or be in subdirectories
    # Let's check for partial matches
    for expected in list(missing_collections):
        for collected in collected_names:
            if expected in collected or collected.endswith(expected):
                missing_collections.remove(expected)
                break

    if missing_collections:
        assert False, f'Expected collections were not found: {", ".join(missing_collections)}. Found: {", ".join(collected_names)}'
