import csv
import glob
import os
import tarfile

from unittest.mock import patch

import pytest

from metrics_utility.base.collection import Collection
from metrics_utility.test.util import run_gather_ext, run_gather_int
from metrics_utility.test.gather.test_jobhostsummary_gather import SafeTarFile

env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './out',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

jobs_lines = [
    "id,polymorphic_ctype_id,model,organization_id,organization_name,execution_environment_image,inventory_id,inventory_name,created,name,unified_job_template_id,launch_type,schedule_id,execution_node,controller_node,cancel_flag,status,failed,started,finished,elapsed,job_explanation,instance_group_id,installed_collections,ansible_version,forks",
    "1,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,controller1,f,pending,f,,2025-06-13 10:00:00+00,0,,,{},2.9.10,0",
    "2,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,controller1,f,pending,f,,2025-06-13 10:00:00+00,0,,,{},2.9.10,0",
    "3,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,controller1,f,pending,f,,2025-06-13 10:00:00+00,0,,,{},2.9.10,0"
]

# we have to skip columns containing ids because they can change
json_lines_skip_ids_columns = [
    'id',
    'polymorphic_ctype_id',
    'organization_id',
    'inventory_id',
    'unified_job_template_id',
    'schedule_id',
    'instance_group_id',
]

# where to find the tar.gz (match jobhostsummary test layout)
uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./out/data/2025/06/*/{uuid}-*.tar.gz'
file_paths = f'./out/data/2025/06/13/{uuid}-*.tar.gz'


@pytest.fixture
def cleanup_glob():
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_unified_jobs_table_command(cleanup_glob):
    """Build and validate unified_jobs_table.csv contents in the generated tarball."""
    # prepare env
    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'unified_jobs_table'

    # run the gather command
    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-12', '--until=2025-06-14'])

    jobs_found = False

    # locate the generated tarball(s)
    for file_path in glob.glob(file_paths):
        with SafeTarFile(file_path) as tar:
            # look for the CSV inside (members are already filtered for safety)
            try:
                member = next(m for m in tar.getmembers() if m.name.endswith('unified_jobs_table.csv'))
            except StopIteration:
                continue

            jobs_found = True
            f = tar.extractfile(member)
            assert f is not None, 'Could not extract unified_jobs_table.csv'

            # read CSV rows
            text = f.read().decode('utf-8').splitlines()
            reader = csv.reader(text)
            rows = list(reader)

            # expected header and rows
            expected_header = jobs_lines[0].split(',')
            expected_rows = [line.split(',') for line in jobs_lines[1:]]

            # check header exactly
            header = rows[0]
            assert header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {header}'

            # check number of data rows
            actual_data = rows[1:]
            assert len(actual_data) == len(expected_rows), f'\nRow count mismatch: expected {len(expected_rows)}, got {len(actual_data)}'

            # compare each cell, skipping unstable ID columns
            skip_columns = set(json_lines_skip_ids_columns)
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

    if not jobs_found:
        pytest.fail('unified_jobs_table.csv not found in any tarballs.')








