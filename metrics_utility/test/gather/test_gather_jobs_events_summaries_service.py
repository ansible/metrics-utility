import csv
from pathlib import Path
import glob

import pytest

from django.db import connection

from metrics_utility.library.collectors.controller.credentials_service import credentials_service
from metrics_utility.library.collectors.controller.job_host_summary_service import job_host_summary_service
from metrics_utility.library.collectors.controller.main_jobevent_service import main_jobevent_service
from metrics_utility.library.collectors.controller.unified_jobs import unified_jobs
from metrics_utility.test.gather.test_jobhostsummary_gather import SafeTarFile
from metrics_utility.test.util import cleanup_glob as _cleanup_glob
from metrics_utility.test.util import run_gather_ext, utcdt


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
    # Use csv.reader for expected lines to properly handle quoted fields with commas
    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    expected_header = expected_rows[0]
    expected_data = expected_rows[1:]

    found = False
    for file_path in glob.glob(file_paths):
        with SafeTarFile(file_path) as tar:
            try:
                member = next(m for m in tar.getmembers() if m.name.endswith(csv_filename))
            except StopIteration:
                continue

            found = True
            f = tar.extractfile(member)
            assert f is not None, f'Could not extract {csv_filename}'

            text = f.read().decode('utf-8').splitlines()

            print('original --------------------------------')
            # print(text)
            for line in text:
                print(line)
            print('--------------------------------\n\n')

            print('expected --------------------------------')
            for line in expected_lines:
                print(line)
            print('--------------------------------\n\n')

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

            break

    if not found:
        pytest.fail(f'{csv_filename} not found in any tarballs.')


def _parse_expected_csv(expected_lines):
    """Parse expected CSV lines into header and data rows."""
    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    return expected_rows[0], expected_rows[1:]


def _read_dataframe(df):
    # Convert boolean columns from True/False to t/f
    # Convert float columns that are actually integers to Int64
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == 'bool':
            df_copy[col] = df_copy[col].map({True: 't', False: 'f'})
        elif df_copy[col].dtype in ['float64', 'float32']:
            # If all non-null values are whole numbers, convert to nullable int
            non_null_values = df_copy[col].dropna()
            if len(non_null_values) > 0 and (non_null_values == non_null_values.astype(int)).all():
                df_copy[col] = df_copy[col].astype('Int64')

    text = df_copy.to_csv(index=False).splitlines()
    reader = csv.reader(text)
    rows = list(reader)
    return rows[0], rows[1:], text


def _print_comparison(actual_text, expected_lines):
    """Print actual and expected CSV content for debugging."""
    print('original --------------------------------')
    for line in actual_text:
        print(line)
    print('--------------------------------\n\n')

    print('expected --------------------------------')
    for line in expected_lines:
        print(line)
    print('--------------------------------\n\n')


def _get_sort_key(row, header_row):
    """Create sort key from available columns: job_id, host_id, event, or first column."""
    key_parts = []
    sort_columns = ['job_id', 'host_id', 'event']

    for col_name in sort_columns:
        if col_name in header_row:
            idx = header_row.index(col_name)
            key_parts.append(row[idx] if idx < len(row) else '')

    # Fallback: use first column if no standard columns found
    if not key_parts and row:
        key_parts.append(row[0])

    return tuple(key_parts or ('',))


def _validate_header(actual_header, expected_header):
    """Validate that CSV headers match."""
    assert actual_header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {actual_header}'


def _validate_row_count(actual_data, expected_data):
    """Validate that row counts match."""
    assert len(actual_data) == len(expected_data), f'\nRow count mismatch: expected {len(expected_data)}, got {len(actual_data)}'


def _validate_rows(actual_data_sorted, expected_data_sorted, header, skip_columns_names):
    """Validate that all rows match, skipping specified columns."""
    skip_columns = set(skip_columns_names)
    for i, (expected_row, actual_row) in enumerate(zip(expected_data_sorted, actual_data_sorted), start=1):
        for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
            col_name = header[idx]
            if col_name in skip_columns:
                continue
            assert exp_cell == act_cell, (
                f'\nData mismatch on row {i + 1}, column {col_name!r} (index {idx}):\nExpected: {exp_cell!r}\nActual:   {act_cell!r}'
            )


def validate_dataframe(df, expected_lines, skip_columns_names):
    """Validate DataFrame

    df: pandas DataFrame to validate
    expected_lines: list of strings where first is header, rest rows
    skip_columns_names: iterable of column names to skip comparison
    """
    expected_header, expected_data = _parse_expected_csv(expected_lines)
    header, actual_data, text = _read_dataframe(df)

    _print_comparison(text, expected_lines)
    _validate_header(header, expected_header)
    _validate_row_count(actual_data, expected_data)

    # Sort both actual and expected data for consistent comparison
    actual_data_sorted = sorted(actual_data, key=lambda r: _get_sort_key(r, header))
    expected_data_sorted = sorted(expected_data, key=lambda r: _get_sort_key(r, header))

    _validate_rows(actual_data_sorted, expected_data_sorted, header, skip_columns_names)


@pytest.fixture
def cleanup_glob():
    yield
    _cleanup_glob(file_glob)


jobs_lines = [
    (
        'id,polymorphic_ctype_id,model,organization_id,organization_name,'
        'execution_environment_image,inventory_id,inventory_name,execution_environment_id,created,'
        'name,unified_job_template_id,launch_type,schedule_id,execution_node,'
        'controller_node,cancel_flag,status,failed,started,finished,elapsed,'
        'job_explanation,instance_group_id,installed_collections,ansible_version,forks,'
        'job_template_name,scm_type'
    ),
    (
        '1,,job,2,default_org_2025-06-13,registry.example.com/envs/python-ml:3.11,4,default_inventory_2025-06-13,,'
        '2025-06-13 10:00:00+00:00,default_unified_job_2025-06-13,1,manual,,auto,'
        'controller1,f,pending,f,,2025-06-13 10:02:10+00:00,120.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}}",2.9.10,5,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '2,,job,2,default_org_2025-06-13,registry.example.com/envs/python-ml:3.11,4,default_inventory_2025-06-13,,'
        '2025-06-13 10:00:00+00:00,default_unified_job_2025-06-13,1,scheduled,,auto,'
        'controller1,f,pending,f,2025-06-13 10:00:20+00:00,2025-06-13 10:03:20+00:00,180.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}}",2.9.10,10,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '3,,job,2,default_org_2025-06-13,registry.example.com/envs/node-backend:20,4,default_inventory_2025-06-13,,'
        '2025-06-13 10:00:00+00:00,default_unified_job_2025-06-13,1,workflow,,auto,'
        'controller1,f,failed,t,2025-06-13 10:00:30+00:00,2025-06-13 10:02:00+00:00,90.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}, '
        '""redhat.rhel_system_roles"": {""version"": ""1.23.0""}}",2.9.10,20,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '4,,job,2,default_org_2025-06-13,registry.example.com/envs/node-backend:20,4,default_inventory_2025-06-13,,'
        '2025-06-13 11:00:00+00:00,default_unified_job_11_2025-06-13,1,manual,,auto,'
        'controller1,f,pending,f,2025-06-13 11:00:10+00:00,2025-06-13 11:01:50+00:00,100.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}, '
        '""redhat.rhel_system_roles"": {""version"": ""1.23.0""}}",2.9.10,8,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '5,,job,2,default_org_2025-06-13,registry.example.com/envs/node-backend:20,4,default_inventory_2025-06-13,,'
        '2025-06-13 11:00:00+00:00,default_unified_job_11_2025-06-13,1,scheduled,,auto,'
        'controller1,f,pending,f,2025-06-13 11:00:20+00:00,2025-06-13 11:02:50+00:00,150.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}, '
        '""redhat.rhel_system_roles"": {""version"": ""1.23.0""}}",2.9.10,15,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '6,,job,2,default_org_2025-06-13,registry.example.com/envs/python-ml:3.11,4,default_inventory_2025-06-13,,'
        '2025-06-13 11:00:00+00:00,default_unified_job_11_2025-06-13,1,workflow,,auto,'
        'controller1,f,pending,f,2025-06-13 11:00:30+00:00,2025-06-13 11:01:50+00:00,80.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}}",2.9.10,25,'
        'default_unified_job_template_2025-06-13,git'
    ),
]

# we have to skip columns containing ids because they can change
json_lines_skip_ids_columns = [
    'id',
    'polymorphic_ctype_id',
    'organization_id',
    'inventory_id',
    'execution_environment_id',
    'unified_job_template_id',
    'schedule_id',
    'instance_group_id',
]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_unified_jobs_command(cleanup_glob):
    """Build and validate unified_jobs output from new library collector."""
    since = utcdt('2025-06-12')
    until = utcdt('2025-06-14')

    # Run the new collector directly
    collector_instance = unified_jobs(db=connection, since=since, until=until)
    df = collector_instance.gather()

    assert df is not None, 'unified_jobs returned None'

    # Validate DataFrame content
    validate_dataframe(df, jobs_lines, json_lines_skip_ids_columns)


jobs_host_summary_service_lines = [
    (
        'id,created,modified,host_name,host_remote_id,changed,dark,failures,ok,processed,skipped,'
        'failed,ignored,rescued,job_created,job_remote_id,job_template_remote_id,'
        'job_template_name,ansible_version,launch_type,inventory_remote_id,inventory_name,organization_remote_id,'
        'organization_name,project_remote_id,project_name,model'
    ),
    (
        '1,2025-06-13 10:00:00+00:00,2025-06-13 10:00:00+00:00,default_host_1_2025-06-13,'
        '31,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00:00,1,1,default_unified_job_template_2025-06-13,2.9.10,manual,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '2,2025-06-13 10:00:00+00:00,2025-06-13 10:00:00+00:00,default_host_2_2025-06-13,'
        '32,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00:00,1,1,default_unified_job_template_2025-06-13,2.9.10,manual,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '3,2025-06-13 10:00:00+00:00,2025-06-13 10:00:00+00:00,default_host_1_2025-06-13,'
        '31,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00:00,2,1,default_unified_job_template_2025-06-13,2.9.10,scheduled,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '4,2025-06-13 10:00:00+00:00,2025-06-13 10:00:00+00:00,default_host_2_2025-06-13,'
        '32,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00:00,2,1,default_unified_job_template_2025-06-13,2.9.10,scheduled,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '5,2025-06-13 10:00:00+00:00,2025-06-13 10:00:00+00:00,default_host_1_2025-06-13,'
        '31,0,0,1,0,0,0,t,0,0,'
        '2025-06-13 10:00:00+00:00,3,1,default_unified_job_template_2025-06-13,2.9.10,workflow,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '6,2025-06-13 10:00:00+00:00,2025-06-13 10:00:00+00:00,default_host_2_2025-06-13,'
        '32,0,0,1,0,0,0,t,0,0,'
        '2025-06-13 10:00:00+00:00,3,1,default_unified_job_template_2025-06-13,2.9.10,workflow,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '7,2025-06-13 11:00:00+00:00,2025-06-13 11:00:00+00:00,default_host_1_2025-06-13,'
        '31,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 11:00:00+00:00,4,1,default_unified_job_template_2025-06-13,2.9.10,manual,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '8,2025-06-13 11:00:00+00:00,2025-06-13 11:00:00+00:00,default_host_2_2025-06-13,'
        '32,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 11:00:00+00:00,4,1,default_unified_job_template_2025-06-13,2.9.10,manual,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '9,2025-06-13 11:00:00+00:00,2025-06-13 11:00:00+00:00,default_host_1_2025-06-13,'
        '31,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 11:00:00+00:00,5,1,default_unified_job_template_2025-06-13,2.9.10,scheduled,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '10,2025-06-13 11:00:00+00:00,2025-06-13 11:00:00+00:00,default_host_2_2025-06-13,'
        '32,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 11:00:00+00:00,5,1,default_unified_job_template_2025-06-13,2.9.10,scheduled,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '11,2025-06-13 11:00:00+00:00,2025-06-13 11:00:00+00:00,default_host_1_2025-06-13,'
        '31,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 11:00:00+00:00,6,1,default_unified_job_template_2025-06-13,2.9.10,workflow,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '12,2025-06-13 11:00:00+00:00,2025-06-13 11:00:00+00:00,default_host_2_2025-06-13,'
        '32,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 11:00:00+00:00,6,1,default_unified_job_template_2025-06-13,2.9.10,workflow,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
]


jobs_host_summary_service_skip_columns = [
    'id',
    'host_remote_id',
    'job_remote_id',
    'job_template_remote_id',
    'inventory_remote_id',
    'organization_remote_id',
    'project_remote_id',
]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_service_command(cleanup_glob):
    """Build and validate job_host_summary_service output from new library collector."""
    since = utcdt('2025-06-12')
    until = utcdt('2025-06-14')

    # Run the new collector directly
    collector_instance = job_host_summary_service(db=connection, since=since, until=until)
    df = collector_instance.gather()

    assert df is not None, 'job_host_summary_service returned None'

    # Validate DataFrame content
    validate_dataframe(df, jobs_host_summary_service_lines, jobs_host_summary_service_skip_columns)


main_jobevent_service_lines = (
    Path(__file__).resolve().parent.joinpath('fixtures', 'main_jobevent_service_expected.csv').read_text().splitlines()
)


main_jobevent_service_skip_columns = [
    'id',
    'job_remote_id',
    'host_remote_id',
    'uuid',
    'parent_uuid',
    'task_uuid',
    'event_data_length',
]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_main_jobevent_service_command(cleanup_glob):
    """Build and validate main_jobevent_service output from new library collector."""
    since = utcdt('2025-06-12')
    until = utcdt('2025-06-14')

    # Run the new collector directly
    collector_instance = main_jobevent_service(db=connection, since=since, until=until)
    df = collector_instance.gather()

    assert df is not None, 'main_jobevent_service returned None'

    # Validate DataFrame content
    validate_dataframe(df, main_jobevent_service_lines, main_jobevent_service_skip_columns)


def test_main_jobevent_service_row_limit(caplog):
    """Integration test: row_limit caps the number of events fetched from the real DB."""
    import logging

    since = utcdt('2025-06-12')
    until = utcdt('2025-06-14')

    # The fixture contains 64 events; a limit of 2 must cap the result.
    collector_instance = main_jobevent_service(db=connection, since=since, until=until, row_limit=2)
    with caplog.at_level(logging.INFO, logger='metrics_utility.logger'):
        df = collector_instance.gather()

    assert df is not None, 'main_jobevent_service returned None'
    assert len(df) == 2, f'Expected exactly 2 rows with row_limit=2, got {len(df)}'

    # Schema must be intact even when truncated
    expected_columns = [
        'id',
        'created',
        'modified',
        'job_created',
        'job_finished',
        'ansible_version',
        'uuid',
        'parent_uuid',
        'event',
        'task_action',
        'resolved_action',
        'resolved_role',
        'duration',
        'start',
        'end',
        'task_uuid',
        'ignore_errors',
        'failed',
        'changed',
        'playbook',
        'play',
        'task',
        'role',
        'job_remote_id',
        'job_id',
        'host_remote_id',
        'host_id',
        'host_name',
        'warnings',
        'deprecations',
        'event_data_length',
        'job_failed',
        'job_started',
    ]
    assert list(df.columns) == expected_columns, f'Unexpected columns: {list(df.columns)}'

    # Truncation info must be emitted when limit is reached
    info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
    assert any('row limit reached' in str(m) for m in info_messages), 'Expected a row-limit info log, but none was found'


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


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_execution_environments_command(cleanup_glob):
    """Build and validate execution_environments.csv contents in the generated tarball."""
    # prepare env

    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'execution_environments'

    # run the gather command
    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-12', '--until=2025-06-14'])

    validate_csv_in_tarballs(file_paths, 'execution_environments.csv', execution_environments_lines, execution_environments_skip_columns)


credentials_service_lines = [
    'credential_type',
    'Amazon Web Services',
    'Machine',
    'Network',
    'Vault',
]

credentials_service_skip_columns = []


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_credentials_service_command(cleanup_glob):
    """Build and validate credentials_service output from new library collector."""
    since = utcdt('2025-06-12')
    until = utcdt('2025-06-14')

    # Run the new collector directly
    collector_instance = credentials_service(db=connection, since=since, until=until)
    df = collector_instance.gather()

    assert df is not None, 'credentials_service returned None'

    # Validate DataFrame content
    validate_dataframe(df, credentials_service_lines, credentials_service_skip_columns)

    # Verify that custom credential types (managed=false) are NOT included
    assert 'My Custom Credential Type' not in df['credential_type'].values, (
        'Custom credential type "My Custom Credential Type" should be filtered out by managed=true filter, but it was found in the output'
    )
