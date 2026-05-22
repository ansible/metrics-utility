import csv
import glob
import os

import pytest

from metrics_utility.test.gather.support.helpers import read_tarball
from metrics_utility.test.util import _print_comparison, run_gather_ext


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './out',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

# where to find the tar.gz (match jobhostsummary test layout)
uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./out/*/{uuid}-*.tar.gz'
file_paths = f'./out/data/2025/06/13/{uuid}-*.tar.gz'


def validate_csv_in_tarballs(file_paths, csv_filename, expected_lines, skip_columns_names):
    """Open tarballs under file_paths, find csv_filename, and validate its rows.

    expected_lines: list of strings where first is header, rest rows
    skip_columns_names: iterable of column names to skip comparison
    """
    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    expected_header = expected_rows[0]
    expected_data = expected_rows[1:]

    for file_path in glob.glob(file_paths):
        files = read_tarball(file_path)
        match = next((name for name in files if name.endswith(csv_filename)), None)
        if match is None:
            continue

        text = files[match].decode('utf-8').splitlines()

        _print_comparison(text, expected_lines)

        reader = csv.reader(text)
        rows = list(reader)

        header = rows[0]
        assert header == expected_header, f'\nHeader mismatch for {csv_filename}:\nExpected: {expected_header}\nActual:   {header}'

        actual_data = rows[1:]
        assert len(actual_data) == len(expected_data), (
            f'\nRow count mismatch in {csv_filename}: expected {len(expected_data)}, got {len(actual_data)}'
        )

        skip_columns = set(skip_columns_names)
        for i, (expected_row, actual_row) in enumerate(zip(expected_data, actual_data), start=1):
            for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
                col_name = header[idx]
                if col_name in skip_columns:
                    continue
                assert exp_cell == act_cell, (
                    f'\nData mismatch in {csv_filename} on row {i + 1}, column {col_name!r} '
                    f'(index {idx}):\n'
                    f'Expected: {exp_cell!r}\n'
                    f'Actual:   {act_cell!r}'
                )

        return

    pytest.fail(f'{csv_filename} not found in any tarballs.')


@pytest.fixture
def cleanup_glob():
    for file in glob.glob(file_glob):
        os.remove(file)
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


execution_environments_lines = [
    'id,created,modified,description,image,managed,created_by_id,credential_id,modified_by_id,organization_id,name,pull',
    '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    'Python 3.11 environment with common ML libraries,'
    'registry.example.com/envs/python-ml:3.11,t,,,,,'
    'Python ML Environment,always',
    '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    'Node.js 20 environment for backend services,'
    'registry.example.com/envs/node-backend:20,f,,,,,'
    'Node Backend Environment,missing',
]

execution_environments_skip_columns = [
    'id',
    'created_by_id',
    'credential_id',
    'modified_by_id',
    'organization_id',
]


def test_execution_environments_command(cleanup_glob):
    """Build and validate execution_environments.csv contents in the generated tarball."""
    # prepare env

    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'execution_environments'

    # run the gather command
    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-12', '--until=2025-06-14'])

    validate_csv_in_tarballs(file_paths, 'execution_environments.csv', execution_environments_lines, execution_environments_skip_columns)
