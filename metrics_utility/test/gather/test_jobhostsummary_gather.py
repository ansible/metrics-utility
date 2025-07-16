import csv
import glob
import json
import os
import tarfile

import pytest

from metrics_utility.test.util import run_gather_ext


# environment for run_gather_ext
env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

# mock uuid
uuid = '00000000-0000-0000-0000-000000000000'

# where to find the tar.gz
file_glob = f'./metrics_utility/test/test_data/data/2025/06/*/{uuid}-*.tar.gz'
file_paths = f'./metrics_utility/test/test_data/data/2025/06/13/{uuid}-*.tar.gz'

# expected CSV content (header + rows)
test_lines = [
    'id,created,modified,host_name,host_remote_id,ansible_host_variable,'
    'ansible_connection_variable,changed,dark,failures,ok,processed,skipped,'
    'failed,ignored,rescued,job_created,job_remote_id,job_template_remote_id,'
    'job_template_name,inventory_remote_id,inventory_name,organization_remote_id,'
    'organization_name,project_remote_id,project_name',
    '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,0,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,1,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,0,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,1,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '3,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,0,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,2,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '4,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,0,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,2,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '5,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,1,'
    'default_ansible_host,default_ansible_connection,0,0,0,0,0,0,f,0,0,'
    '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,1,'
    'default_inventory_2025-06-13,1,default_org_2025-06-13,1,'
    'default_unified_job_template_2025-06-13',
    '6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,2,'
    'default_ansible_host,default_ansible_connection,0,0,0,0,0,0,f,0,0,'
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
    run_gather_ext(env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    jobhost_found = False

    # locate the generated tarball(s)
    for file_path in glob.glob(file_paths):
        with tarfile.open(file_path, 'r:gz') as tar:
            # look for the CSV inside
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
def test_main_host_collection(cleanup_glob):
    """Test main_host table collection using the updated query with helper functions."""
    # Enable main_host collection by adding it to optional collectors
    env_vars_with_main_host = env_vars.copy()
    env_vars_with_main_host['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'main_jobevent,main_host'

    # run the gather command
    run_gather_ext(env_vars_with_main_host, ['--ship', '--since=2025-07-13', '--until=2025-07-14'])

    main_host_found = False

    # Expected CSV structure based on the main_host query output
    expected_header = [
        'host_name',
        'host_id',
        'inventory_remote_id',
        'inventory_name',
        'organization_remote_id',
        'organization_name',
        'last_automation',
        'ansible_host_variable',
        'canonical_facts',
        'facts',
    ]

    # locate the generated tarball(s)
    # Adjust the glob pattern for the date range we're using
    file_pattern = f'./metrics_utility/test/test_data/data/2025/07/13/{uuid}-*.tar.gz'
    for file_path in glob.glob(file_pattern):
        with tarfile.open(file_path, 'r:gz') as tar:
            # look for the CSV inside
            try:
                member = next(m for m in tar.getmembers() if m.name.endswith('main_host.csv'))
            except StopIteration:
                continue

            main_host_found = True
            f = tar.extractfile(member)
            assert f is not None, 'Could not extract main_host.csv'

            # read CSV rows
            text = f.read().decode('utf-8').splitlines()
            reader = csv.reader(text)
            rows = list(reader)

            # check header exactly
            header = rows[0]
            assert header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {header}'

            # check that we have data rows
            actual_data = rows[1:]
            assert len(actual_data) > 0, 'No data rows found in main_host.csv'

            # Validate first row has expected structure
            if len(actual_data) > 0:
                first_row = actual_data[0]
                assert len(first_row) == len(expected_header), f'Row column count mismatch: expected {len(expected_header)}, got {len(first_row)}'

                # Validate canonical_facts and facts are JSON strings
                canonical_facts_idx = header.index('canonical_facts')
                facts_idx = header.index('facts')

                try:
                    canonical_facts = json.loads(first_row[canonical_facts_idx])
                    assert isinstance(canonical_facts, dict), 'canonical_facts should be a JSON object'
                    # Check expected fields in canonical_facts
                    expected_canonical_fields = {'ansible_product_serial', 'ansible_machine_id', 'ansible_host', 'host_name', 'ansible_port'}
                    assert set(canonical_facts.keys()) == expected_canonical_fields, f'Unexpected canonical_facts fields: {canonical_facts.keys()}'
                except json.JSONDecodeError:
                    pytest.fail(f'canonical_facts is not valid JSON: {first_row[canonical_facts_idx]}')

                try:
                    facts = json.loads(first_row[facts_idx])
                    assert isinstance(facts, dict), 'facts should be a JSON object'
                    assert 'ansible_connection_variable' in facts, 'facts should contain ansible_connection_variable'
                    assert 'ansible_virtualization_type' in facts, 'facts should contain ansible_virtualization_type'
                except json.JSONDecodeError:
                    pytest.fail(f'facts is not valid JSON: {first_row[facts_idx]}')

            print(f'Successfully collected {len(actual_data)} rows from main_host table')
            break

    if not main_host_found:
        pytest.fail('main_host.csv not found in any tarballs.')
