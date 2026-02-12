import csv
import glob
import os
import tempfile

from datetime import datetime, timezone

import pytest

from django.db import connection

from metrics_utility.library.collectors.controller.credentials_service import credentials_service
from metrics_utility.library.collectors.controller.job_host_summary_service import job_host_summary_service
from metrics_utility.library.collectors.controller.main_jobevent_service import main_jobevent_service
from metrics_utility.library.collectors.controller.unified_jobs import unified_jobs
from metrics_utility.test.gather.test_jobhostsummary_gather import SafeTarFile
from metrics_utility.test.util import run_gather_ext


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


def validate_csv_file(csv_file_path, expected_lines, skip_columns_names):
    """Validate CSV file directly.

    expected_lines: list of strings where first is header, rest rows
    skip_columns_names: iterable of column names to skip comparison
    """
    # Use csv.reader for expected lines to properly handle quoted fields with commas
    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    expected_header = expected_rows[0]
    expected_data = expected_rows[1:]

    with open(csv_file_path, 'r') as f:
        text = f.read().splitlines()

    print('original --------------------------------')
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
    assert header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {header}'

    actual_data = rows[1:]
    assert len(actual_data) == len(expected_data), f'\nRow count mismatch: expected {len(expected_data)}, got {len(actual_data)}'

    # Sort both actual and expected data for consistent comparison
    # Use available columns: job_id, host_id (if present), event (if present)
    def get_sort_key(row, header_row):
        """Create sort key from available columns."""
        key_parts = []
        # Always try job_id if present
        if 'job_id' in header_row:
            idx = header_row.index('job_id')
            key_parts.append(row[idx] if idx < len(row) else '')
        # Try host_id if present
        if 'host_id' in header_row:
            idx = header_row.index('host_id')
            key_parts.append(row[idx] if idx < len(row) else '')
        # Try event if present
        if 'event' in header_row:
            idx = header_row.index('event')
            key_parts.append(row[idx] if idx < len(row) else '')
        # Fallback: use first column if no standard columns found
        if not key_parts and row:
            key_parts.append(row[0] if row else '')
        return tuple(key_parts or ('',))
    
    actual_data_sorted = sorted(actual_data, key=lambda r: get_sort_key(r, header))
    expected_data_sorted = sorted(expected_data, key=lambda r: get_sort_key(r, header))

    skip_columns = set(skip_columns_names)
    for i, (expected_row, actual_row) in enumerate(zip(expected_data_sorted, actual_data_sorted), start=1):
        for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
            col_name = header[idx]
            if col_name in skip_columns:
                continue
            assert exp_cell == act_cell, (
                f'\nData mismatch on row {i + 1}, column {col_name!r} (index {idx}):\nExpected: {exp_cell!r}\nActual:   {act_cell!r}'
            )


@pytest.fixture
def cleanup_glob():
    for file in glob.glob(file_glob):
        os.remove(file)
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


jobs_lines = [
    (
        'id,polymorphic_ctype_id,model,organization_id,organization_name,'
        'execution_environment_image,inventory_id,inventory_name,created,'
        'name,unified_job_template_id,launch_type,schedule_id,execution_node,'
        'controller_node,cancel_flag,status,failed,started,finished,elapsed,'
        'job_explanation,instance_group_id,installed_collections,ansible_version,forks,'
        'job_template_name,scm_type'
    ),
    (
        '1,,job,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,'
        '2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,'
        'controller1,f,pending,f,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,0.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}}",2.9.10,5,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '2,,job,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,'
        '2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,scheduled,,auto,'
        'controller1,f,pending,f,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,0.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}}",2.9.10,10,'
        'default_unified_job_template_2025-06-13,git'
    ),
    (
        '3,,job,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,'
        '2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,workflow,,auto,'
        'controller1,f,pending,f,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,0.000,,,'
        '"{""a10.acos_axapi"": {""version"": ""1.0.0""}, '
        '""ansible.builtin"": {""version"": ""2.9.10""}}",2.9.10,20,'
        'default_unified_job_template_2025-06-13,git'
    ),
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


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_unified_jobs_command(cleanup_glob):
    """Build and validate unified_jobs output from new library collector."""
    since = datetime(2025, 6, 12, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, tzinfo=timezone.utc)

    # Run the new collector directly
    with tempfile.TemporaryDirectory() as tmpdir:
        collector_instance = unified_jobs(db=connection, since=since, until=until, output_dir=tmpdir)
        csv_files = collector_instance.gather()

        # Find the unified_jobs CSV file (generated as unified_jobs_table.csv)
        csv_file = None
        for f in csv_files:
            if 'unified_jobs_table.csv' in f or f.endswith('unified_jobs_table.csv'):
                csv_file = f
                break

        assert csv_file is not None, f'unified_jobs CSV not found in {csv_files}'

        # Validate CSV content
        validate_csv_file(csv_file, jobs_lines, json_lines_skip_ids_columns)


jobs_host_summary_service_lines = [
    (
        'id,created,modified,host_name,host_remote_id,ansible_host_variable,'
        'ansible_connection_variable,changed,dark,failures,ok,processed,skipped,'
        'failed,ignored,rescued,job_created,job_remote_id,job_template_remote_id,'
        'job_template_name,ansible_version,launch_type,inventory_remote_id,inventory_name,organization_remote_id,'
        'organization_name,project_remote_id,project_name,model'
    ),
    (
        '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,'
        '31,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,1,1,default_unified_job_template_2025-06-13,2.9.10,manual,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,'
        '32,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,1,1,default_unified_job_template_2025-06-13,2.9.10,manual,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '3,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,'
        '31,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,2,1,default_unified_job_template_2025-06-13,2.9.10,scheduled,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '4,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,'
        '32,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,2,1,default_unified_job_template_2025-06-13,2.9.10,scheduled,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '5,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,'
        '31,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,3,1,default_unified_job_template_2025-06-13,2.9.10,workflow,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13,job'
    ),
    (
        '6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,'
        '32,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,3,1,default_unified_job_template_2025-06-13,2.9.10,workflow,4,'
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
    since = datetime(2025, 6, 12, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, tzinfo=timezone.utc)

    # Run the new collector directly
    with tempfile.TemporaryDirectory() as tmpdir:
        collector_instance = job_host_summary_service(db=connection, since=since, until=until, output_dir=tmpdir)
        csv_files = collector_instance.gather()

        # Find the job_host_summary_service CSV file (generated as main_jobhostsummary_table.csv)
        csv_file = None
        for f in csv_files:
            if 'main_jobhostsummary_table.csv' in f or f.endswith('main_jobhostsummary_table.csv'):
                csv_file = f
                break

        assert csv_file is not None, f'job_host_summary_service CSV not found in {csv_files}'

        # Validate CSV content
        validate_csv_file(csv_file, jobs_host_summary_service_lines, jobs_host_summary_service_skip_columns)


main_jobevent_service_lines = [
    'id,created,modified,job_created,job_finished,ansible_version,uuid,parent_uuid,event,'
    'task_action,resolved_action,resolved_role,duration,start,end,task_uuid,ignore_errors,failed,'
    'changed,playbook,play,task,role,job_remote_id,job_id,host_remote_id,host_id,'
    'host_name,warnings,deprecations,playbook_on_stats,job_failed,job_started',
    '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,ansible.builtin.yum,,,,,,1_default_host_1_2025-06-13_1,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,1,1,31,31,'
    'default_host_1_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,a10.acos_axapi.a10_slb_virtual_server,,,,,,1_default_host_1_2025-06-13_2,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,1,1,31,31,'
    'default_host_1_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '3,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,ansible.builtin.yum,,,,,,1_default_host_2_2025-06-13_1,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,1,1,32,32,'
    'default_host_2_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '4,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,a10.acos_axapi.a10_slb_virtual_server,,,,,,1_default_host_2_2025-06-13_2,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,1,1,32,32,'
    'default_host_2_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '5,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,ansible.builtin.yum,,,,,,2_default_host_1_2025-06-13_1,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,2,2,31,31,'
    'default_host_1_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,a10.acos_axapi.a10_slb_virtual_server,,,,,,2_default_host_1_2025-06-13_2,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,2,2,31,31,'
    'default_host_1_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '7,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,ansible.builtin.yum,,,,,,2_default_host_2_2025-06-13_1,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,2,2,32,32,'
    'default_host_2_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '8,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,a10.acos_axapi.a10_slb_virtual_server,,,,,,2_default_host_2_2025-06-13_2,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,2,2,32,32,'
    'default_host_2_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '9,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,ansible.builtin.yum,,,,,,3_default_host_1_2025-06-13_1,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,3,3,31,31,'
    'default_host_1_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '10,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,a10.acos_axapi.a10_slb_virtual_server,,,,,,3_default_host_1_2025-06-13_2,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,3,3,31,31,'
    'default_host_1_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '11,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,ansible.builtin.yum,,,,,,3_default_host_2_2025-06-13_1,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,3,3,32,32,'
    'default_host_2_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '12,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    'UUID,,runner_on_ok,a10.acos_axapi.a10_slb_virtual_server,,,,,,3_default_host_2_2025-06-13_2,f,f,f,'
    'default_playbook.yml,default_play,default_task,default_role,3,3,32,32,'
    'default_host_2_2025-06-13,,,,f,2025-06-13 10:00:00+00',
    '13,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    '13aac8b6-038d-4cbe-af99-67276d80d01b,,warning,,,,,,,,f,f,f,,,'
    ',,1,1,,,,,,,f,2025-06-13 10:00:00+00',
    '14,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    '8cdfc02a-8b52-4fe9-883a-1d6608f68c3f,,warning,,,,,,,,f,f,f,,,'
    ',,2,2,,,,,,,f,2025-06-13 10:00:00+00',
    '15,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    '2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,2.9.10,'
    '150d1d0c-dccb-4940-83ee-4d75c2f22493,,deprecated,,,,,,,,f,f,f,,,'
    ',,3,3,,,,,,,f,2025-06-13 10:00:00+00',
]


main_jobevent_service_skip_columns = [
    'id',
    'job_remote_id',
    'host_remote_id',
    'uuid',
    'parent_uuid',
    'task_uuid',
]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_main_jobevent_service_command(cleanup_glob):
    """Build and validate main_jobevent_service output from new library collector."""
    since = datetime(2025, 6, 12, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, tzinfo=timezone.utc)

    # Run the new collector directly
    with tempfile.TemporaryDirectory() as tmpdir:
        collector_instance = main_jobevent_service(db=connection, since=since, until=until, output_dir=tmpdir)
        csv_files = collector_instance.gather()

        # Find the main_jobevent_service CSV file (generated as main_jobevent_table.csv)
        csv_file = None
        for f in csv_files:
            if 'main_jobevent_table.csv' in f or f.endswith('main_jobevent_table.csv'):
                csv_file = f
                break

        assert csv_file is not None, f'main_jobevent_service CSV not found in {csv_files}'

        # Validate CSV content
        validate_csv_file(csv_file, main_jobevent_service_lines, main_jobevent_service_skip_columns)


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
    'credential_type,job_id,model',
    'Amazon Web Services,1,job',
    'Machine,1,job',
    'Machine,2,job',
    'Vault,2,job',
    'Amazon Web Services,3,job',
    'Machine,3,job',
    'Network,3,job',
]

credentials_service_skip_columns = [
    'job_id',
]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_credentials_service_command(cleanup_glob):
    """Build and validate credentials_service output from new library collector."""
    since = datetime(2025, 6, 12, tzinfo=timezone.utc)
    until = datetime(2025, 6, 14, tzinfo=timezone.utc)

    # Run the new collector directly
    with tempfile.TemporaryDirectory() as tmpdir:
        collector_instance = credentials_service(db=connection, since=since, until=until, output_dir=tmpdir)
        csv_files = collector_instance.gather()

        # Find the credentials_service CSV file (generated as credentials_table.csv)
        csv_file = None
        for f in csv_files:
            if 'credentials_table.csv' in f or f.endswith('credentials_table.csv'):
                csv_file = f
                break

        assert csv_file is not None, f'credentials_service CSV not found in {csv_files}'

        # Validate CSV content
        validate_csv_file(csv_file, credentials_service_lines, credentials_service_skip_columns)
