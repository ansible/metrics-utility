import csv
import glob
import os
import tarfile

from unittest.mock import patch

import pytest

from metrics_utility.base.collection import Collection
from metrics_utility.test.util import run_gather_ext, run_gather_int


def safe_tarfile_member_check(member):
    """
    Check if a tar member is safe to extract (no path traversal).

    This function prevents 'tar slip' or 'zip slip' attacks by validating
    that tar members don't contain dangerous paths that could extract files
    outside the intended directory.

    SonarQube compliance: Addresses security hotspot for archive expansion.
    """
    # Reject device files and FIFOs which could be dangerous
    if member.isdev() or member.isfifo():
        return False
    # Reject paths with directory traversal patterns
    if '..' in member.name or member.name.startswith('/'):
        return False
    return True


class SafeTarFile:
    """
    A context manager for safely opening and reading tar files.

    This class ensures that tar file operations are safe from path traversal
    attacks by filtering out dangerous members during opening.

    SonarQube compliance: Provides safe archive handling.
    """

    def __init__(self, file_path, mode='r:gz'):
        self.file_path = file_path
        self.mode = mode
        self.tar = None

    def __enter__(self):
        # Open the tar file - suppressed security warning as we immediately filter members below
        self.tar = tarfile.open(self.file_path, self.mode)

        # Filter members to only include safe ones
        original_members = self.tar.getmembers()
        safe_members = [m for m in original_members if safe_tarfile_member_check(m)]

        # Replace the members list with filtered safe members
        self.tar.members = safe_members

        return self.tar

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.tar:
            self.tar.close()


# environment for run_gather_ext
env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
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
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,1,'
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


@pytest.fixture
def cleanup_glob():
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command(cleanup_glob):
    """Build xlsx report using build command and test CSV contents."""
    # run the gather command
    run_gather_ext(env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14', '--force'])

    jobhost_found = False

    # locate the generated tarball(s)
    for file_path in glob.glob(file_paths):
        with SafeTarFile(file_path) as tar:
            # look for the CSV inside (members are already filtered for safety)
            try:
                member = next(m for m in tar.getmembers() if m.name.endswith('job_host_summary.csv'))
            except StopIteration:
                continue

            jobhost_found = True
            f = tar.extractfile(member)
            assert f is not None, 'Could not extract job_host_summary.csv'

            # read CSV rows
            text = f.read().decode('utf-8').splitlines()
            reader = csv.reader(text)
            rows = list(reader)

            # check header exactly
            header = rows[0]
            assert header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {header}'

            # check number of data rows
            actual_data = rows[1:]
            assert len(actual_data) == len(expected_rows), f'\nRow count mismatch: expected {len(expected_rows)}, got {len(actual_data)}'

            # compare each cell, skipping unstable ID columns
            for i, (expected_row, actual_row) in enumerate(zip(expected_rows, actual_data), start=1):
                for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
                    col_name = header[idx]
                    if col_name in skip_columns:
                        # skip unstable ID
                        continue

                    assert exp_cell == act_cell, (
                        f'\nData mismatch on row {i + 1}, column "{col_name}" (index {idx}):\nExpected: {exp_cell!r}\nActual:   {act_cell!r}'
                    )
            break

    if not jobhost_found:
        pytest.fail('job_host_summary.csv not found in any tarballs.')


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_disabled_by_env_var(cleanup_glob):
    """Test that job_host_summary.csv is not generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is set to 'true'."""

    # Create environment variables with collector disabled
    disabled_env_vars = env_vars.copy()
    disabled_env_vars['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'

    # run the gather command with disabled collector
    run_gather_ext(disabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    jobhost_found = False

    # locate the generated tarball(s)
    for file_path in glob.glob(file_paths):
        with SafeTarFile(file_path) as tar:
            # look for the CSV inside - it should NOT be present (members are already filtered for safety)
            try:
                next(m for m in tar.getmembers() if m.name.endswith('job_host_summary.csv'))
                jobhost_found = True
            except StopIteration:
                # This is expected when collector is disabled
                continue

    if jobhost_found:
        pytest.fail('job_host_summary.csv should not be generated when collector is disabled.')


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_enabled_explicitly(cleanup_glob):
    """Test that job_host_summary.csv is generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is explicitly set to 'false'."""

    # Create environment variables with collector explicitly enabled
    enabled_env_vars = env_vars.copy()
    enabled_env_vars['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'false'

    # run the gather command with explicitly enabled collector
    run_gather_ext(enabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    jobhost_found = False

    # locate the generated tarball(s)
    for file_path in glob.glob(file_paths):
        with SafeTarFile(file_path) as tar:
            # look for the CSV inside - it should be present (members are already filtered for safety)
            try:
                next(m for m in tar.getmembers() if m.name.endswith('job_host_summary.csv'))
                jobhost_found = True
                break
            except StopIteration:
                continue

    if not jobhost_found:
        pytest.fail('job_host_summary.csv should be generated when collector is explicitly enabled.')


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_case_insensitive_disable(cleanup_glob):
    """Test that the environment variable check is case insensitive for 'true' values."""

    test_cases = ['TRUE', 'True', 'tRuE']

    for test_value in test_cases:
        # Create environment variables with collector disabled using different cases
        disabled_env_vars = env_vars.copy()
        disabled_env_vars['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = test_value

        # run the gather command with disabled collector
        run_gather_ext(disabled_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

        jobhost_found = False

        # locate the generated tarball(s)
        for file_path in glob.glob(file_paths):
            with SafeTarFile(file_path) as tar:
                # look for the CSV inside - it should NOT be present (members are already filtered for safety)
                try:
                    next(m for m in tar.getmembers() if m.name.endswith('job_host_summary.csv'))
                    jobhost_found = True
                except StopIteration:
                    # This is expected when collector is disabled
                    continue

        if jobhost_found:
            pytest.fail(f'job_host_summary.csv should not be generated when collector is disabled with value "{test_value}".')

        # Clean up files for next iteration
        for file in glob.glob(file_glob):
            os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_invalid_values_still_enabled(cleanup_glob):
    """Test that job_host_summary.csv is still generated when METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR is set to invalid values."""

    invalid_values = ['yes', 'no', '1', '0', 'enabled', 'disabled', 'random_text', '']

    for test_value in invalid_values:
        # Create environment variables with collector set to invalid value
        test_env_vars = env_vars.copy()
        test_env_vars['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = test_value

        # run the gather command
        run_gather_ext(test_env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

        jobhost_found = False

        # locate the generated tarball(s)
        for file_path in glob.glob(file_paths):
            with SafeTarFile(file_path) as tar:
                # look for the CSV inside - it should be present since invalid values don't disable (members are already filtered for safety)
                try:
                    next(m for m in tar.getmembers() if m.name.endswith('job_host_summary.csv'))
                    jobhost_found = True
                    break
                except StopIteration:
                    continue

        if not jobhost_found:
            pytest.fail(f'job_host_summary.csv should be generated when collector has invalid disable value "{test_value}".')

        # Clean up files for next iteration
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

    def mock_collection_gather(self, path):
        """Mock collection gather to capture statuses."""
        # Call the original method
        result = original_collection_gather(self, path)

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
