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

uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./out/*/{uuid}-*.tar.gz'
file_paths = f'./out/data/2025/06/*/{uuid}-*.tar.gz'


def validate_csv_in_tarballs(file_paths, csv_filename, expected_lines, skip_columns_names):
    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    expected_header = expected_rows[0]
    expected_data = expected_rows[1:]

    actual_rows = []
    for file_path in sorted(glob.glob(file_paths)):
        files = read_tarball(file_path)
        match = next((name for name in files if name.endswith(csv_filename)), None)
        if match is None:
            continue

        text = files[match].decode('utf-8').splitlines()
        reader = csv.reader(text)
        rows = list(reader)
        header = rows[0]
        assert header == expected_header, f'\nHeader mismatch for {csv_filename}:\nExpected: {expected_header}\nActual:   {header}'
        actual_rows.extend(rows[1:])

    assert len(actual_rows) > 0, f'{csv_filename} not found in any tarballs under {file_paths}'

    _print_comparison(
        [','.join(expected_header)] + [','.join(r) for r in actual_rows],
        expected_lines,
    )

    assert len(actual_rows) == len(expected_data), f'\nRow count mismatch in {csv_filename}: expected {len(expected_data)}, got {len(actual_rows)}'

    skip_columns = set(skip_columns_names)
    actual_sorted = sorted(actual_rows, key=lambda r: r[0])
    expected_sorted = sorted(expected_data, key=lambda r: r[0])

    for i, (expected_row, actual_row) in enumerate(zip(expected_sorted, actual_sorted), start=1):
        for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
            col_name = expected_header[idx]
            if col_name in skip_columns:
                continue
            assert exp_cell == act_cell, (
                f'\nData mismatch in {csv_filename} on row {i + 1}, column {col_name!r} '
                f'(index {idx}):\n'
                f'Expected: {exp_cell!r}\n'
                f'Actual:   {act_cell!r}'
            )


@pytest.fixture
def cleanup_glob():
    for file in glob.glob(file_glob):
        os.remove(file)
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


# Each hostmetric row joins with 3 main_host entries (one per inventory),
# producing 3 rows per hostmetric hostname.
_hm_header = (
    'hostname,host_id,first_automation,last_automation,automated_counter,'
    'deleted_counter,last_deleted,deleted,ansible_product_serial,'
    'ansible_machine_id,ansible_host_variable,ansible_connection_variable'
)
_hm_rows = [
    'default_host_hostmetric_1_2025-06-13,0,2025-06-01 08:00:00+00,2025-06-10 14:30:00+00,12,0,,f,,,,,',
    'default_host_hostmetric_2_2025-06-13,0,2025-06-28 09:15:00+00,2025-06-12 16:00:00+00,5,1,2025-06-20 10:00:00+00,t,,,,,',
    'default_host_hostmetric_3_2025-06-13,0,2025-06-03 12:00:00+00,2025-06-11 13:45:00+00,7,0,,f,,,,,',
    'default_host_hostmetric_4_2025-06-13,0,2025-06-02 07:30:00+00,2025-06-09 15:30:00+00,10,0,,f,,,,,',
    'default_host_hostmetric_5_2025-06-13,0,2025-06-30 10:00:00+00,2025-06-08 11:00:00+00,3,2,2025-06-15 12:00:00+00,t,,,,,',
    'default_host_hostmetric_6_2025-06-13,0,2025-06-01 06:45:00+00,2025-06-06 13:15:00+00,6,1,,t,,,,,',
    'default_host_hostmetric_7_2025-06-13,0,2025-06-04 10:30:00+00,2025-06-10 12:30:00+00,8,0,,f,,,,,',
    'default_host_hostmetric_8_2025-06-13,0,2025-06-29 09:45:00+00,2025-06-07 14:00:00+00,4,1,2025-06-13 09:30:00+00,t,,,,,',
    'default_host_hostmetric_9_2025-06-13,0,2025-06-05 08:30:00+00,2025-06-10 16:00:00+00,9,0,,f,,,,,',
]
main_hostmetric_lines = [_hm_header] + _hm_rows * 3

main_hostmetric_skip_columns = [
    'host_id',
    'ansible_product_serial',
    'ansible_machine_id',
    'ansible_host_variable',
    'ansible_connection_variable',
]


def test_main_hostmetric_command(cleanup_glob):
    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'main_hostmetric'

    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-06', '--until=2025-06-13'])

    validate_csv_in_tarballs(file_paths, 'main_hostmetric.csv', main_hostmetric_lines, main_hostmetric_skip_columns)
