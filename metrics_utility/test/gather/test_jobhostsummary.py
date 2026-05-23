import csv
import glob

from unittest.mock import patch

import pytest

from metrics_utility.gather.collection import Collection
from metrics_utility.test.gather.support.helpers import read_tarball
from metrics_utility.test.util import run_gather_ext, run_gather_int


uuid = '00000000-0000-0000-0000-000000000000'

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


def make_env(ship_path):
    return {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }


def make_glob(ship_path):
    return f'{ship_path}/data/*/*/*/{uuid}-*.tar.gz'


def make_paths(ship_path):
    return f'{ship_path}/data/2025/06/13/{uuid}-*.tar.gz'


def find_csv_in_tarballs(tarball_glob, csv_suffix):
    for file_path in glob.glob(tarball_glob):
        files = read_tarball(file_path)
        match = next((name for name in files if name.endswith(csv_suffix)), None)
        if match is not None:
            return files[match]
    return None


def test_command(ship_path):
    """Build xlsx report using build command and test CSV contents."""
    run_gather_ext(make_env(ship_path), ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    data = find_csv_in_tarballs(make_paths(ship_path), 'job_host_summary.csv')
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


def test_job_host_summary_disabled_by_env_var(ship_path):
    """Test that job_host_summary.csv is not generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is set to 'true'."""
    disabled_env_vars = {**make_env(ship_path), 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true'}

    rg = run_gather_ext(disabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Disabled job_host_summary' in rg.stderr

    assert find_csv_in_tarballs(make_paths(ship_path), 'job_host_summary.csv') is None, (
        'job_host_summary.csv should not be generated when collector is disabled.'
    )


def test_job_host_summary_enabled_explicitly(ship_path):
    """Test that job_host_summary.csv is generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is explicitly set to 'false'."""
    enabled_env_vars = {**make_env(ship_path), 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'false'}

    run_gather_ext(enabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    assert find_csv_in_tarballs(make_paths(ship_path), 'job_host_summary.csv') is not None, (
        'job_host_summary.csv should be generated when collector is explicitly enabled.'
    )


def test_job_host_summary_case_insensitive_disable(ship_path):
    """Test that the environment variable check is case insensitive for 'true' values."""
    for test_value in ['TRUE', 'True', 'tRuE']:
        disabled_env_vars = {**make_env(ship_path), 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': test_value}

        run_gather_ext(disabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

        assert find_csv_in_tarballs(make_paths(ship_path), 'job_host_summary.csv') is None, (
            f'job_host_summary.csv should not be generated when collector is disabled with value "{test_value}".'
        )


def test_job_host_summary_invalid_values_still_enabled(ship_path):
    """Test that job_host_summary.csv is still generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is set to invalid values."""
    for test_value in ['yes', 'no', '0', 'enabled', 'disabled', 'random_text', '']:
        test_env_vars = {**make_env(ship_path), 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': test_value}

        run_gather_ext(test_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

        assert find_csv_in_tarballs(make_paths(ship_path), 'job_host_summary.csv') is not None, (
            f'job_host_summary.csv should be generated when collector has invalid disable value "{test_value}".'
        )


def test_main_host_collection_trailing_comma(ship_path):
    main_host_collection(ship_path, collectors='main_jobevent,main_host', trailing_comma=True)
    main_host_collection(ship_path, collectors='main_jobevent', trailing_comma=True)
    main_host_collection(ship_path, collectors='main_host', trailing_comma=True)


def test_main_host_collection_no_trailing_comma(ship_path):
    main_host_collection(ship_path, collectors='main_jobevent,main_host', trailing_comma=False)
    main_host_collection(ship_path, collectors='main_jobevent', trailing_comma=False)
    main_host_collection(ship_path, collectors='main_host', trailing_comma=False)


def main_host_collection(ship_path, collectors='main_jobevent,main_host', trailing_comma=False):
    """Test that main_host table collection runs without error and all collections have 'ok' status."""
    env_vars_with_main_host = make_env(ship_path)
    env_vars_with_main_host['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = collectors

    if trailing_comma:
        env_vars_with_main_host['METRICS_UTILITY_OPTIONAL_COLLECTORS'] += ','

    collection_statuses = {}

    original_collection_gather = Collection.gather

    def mock_collection_gather(self):
        """Mock collection gather to capture statuses."""
        result = original_collection_gather(self)

        collection_name = getattr(self, 'filename', 'unknown')
        if not self.disabled:
            collection_statuses[collection_name] = self.gathering_successful

        return result

    with patch.object(Collection, 'gather', mock_collection_gather):
        run_gather_int(
            env_vars_with_main_host,
            {
                'ship': True,
                'since': '2025-06-12',
                'until': '2025-06-14',
            },
        )

    collector_list = [c.strip() for c in collectors.split(',') if c.strip()]
    expected_collections = {'job_host_summary.csv'}
    for c in collector_list:
        expected_collections.add(f'{c}.csv')
    errors_found = []

    for collection_name, status in collection_statuses.items():
        if not status:
            errors_found.append(f"Collection '{collection_name}' failed")

    if errors_found:
        pytest.fail('Found errors in collections:\n' + '\n'.join(errors_found))

    collected_names = set(collection_statuses.keys())
    missing_collections = expected_collections - collected_names

    for expected in list(missing_collections):
        for collected in collected_names:
            if expected in collected or collected.endswith(expected):
                missing_collections.remove(expected)
                break

    if missing_collections:
        pytest.fail(f'Expected collections were not found: {", ".join(missing_collections)}. Found: {", ".join(collected_names)}')
