import csv
import glob
import os

import pytest

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
    expected_header = expected_lines[0].split(',')
    expected_rows = [line.split(',') for line in expected_lines[1:]]

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
            assert len(actual_data) == len(expected_rows), (
                f'\nRow count mismatch in {csv_filename}: expected {len(expected_rows)}, got {len(actual_data)}'
            )

            skip_columns = set(skip_columns_names)
            for i, (expected_row, actual_row) in enumerate(zip(expected_rows, actual_data), start=1):
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
        'job_explanation,instance_group_id,installed_collections,ansible_version,forks'
    ),
    (
        '1,,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,'
        '2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,'
        'controller1,f,pending,f,,2025-06-13 10:00:00+00,0.000,,,{},2.9.10,0'
    ),
    (
        '2,,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,'
        '2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,'
        'controller1,f,pending,f,,2025-06-13 10:00:00+00,0.000,,,{},2.9.10,0'
    ),
    (
        '3,,,2,default_org_2025-06-13,,4,default_inventory_2025-06-13,'
        '2025-06-13 10:00:00+00,default_unified_job_2025-06-13,1,manual,,auto,'
        'controller1,f,pending,f,,2025-06-13 10:00:00+00,0.000,,,{},2.9.10,0'
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
    """Build and validate unified_jobs_table.csv contents in the generated tarball."""
    # prepare env
    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'unified_jobs'

    # run the gather command
    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-12', '--until=2025-06-14'])

    # validate CSV inside generated tarball(s)
    validate_csv_in_tarballs(file_paths, 'unified_jobs.csv', jobs_lines, json_lines_skip_ids_columns)


jobs_host_summary_service_lines = [
    (
        'id,created,modified,host_name,host_remote_id,ansible_host_variable,'
        'ansible_connection_variable,changed,dark,failures,ok,processed,skipped,'
        'failed,ignored,rescued,job_created,job_remote_id,job_template_remote_id,'
        'job_template_name,inventory_remote_id,inventory_name,organization_remote_id,'
        'organization_name,project_remote_id,project_name'
    ),
    (
        '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,'
        '31,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,1,1,default_unified_job_2025-06-13,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13'
    ),
    (
        '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,'
        '32,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,1,1,default_unified_job_2025-06-13,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13'
    ),
    (
        '3,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,'
        '31,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,2,1,default_unified_job_2025-06-13,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13'
    ),
    (
        '4,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,'
        '32,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,2,1,default_unified_job_2025-06-13,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13'
    ),
    (
        '5,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_1_2025-06-13,'
        '31,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13'
    ),
    (
        '6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,default_host_2_2025-06-13,'
        '32,default_ansible_host,default_ansible_connection,0,0,0,1,0,0,f,0,0,'
        '2025-06-13 10:00:00+00,3,1,default_unified_job_2025-06-13,4,'
        'default_inventory_2025-06-13,2,default_org_2025-06-13,1,'
        'default_unified_job_template_2025-06-13'
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
    """Build and validate jobs_host_summary_service.csv contents in the generated tarball."""
    # prepare env

    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'job_host_summary_service'

    # run the gather command
    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-12', '--until=2025-06-14'])

    # validate CSV inside generated tarball(s)
    validate_csv_in_tarballs(file_paths, 'job_host_summary_service.csv', jobs_host_summary_service_lines, jobs_host_summary_service_skip_columns)



main_jobevent_service_lines = [
    "id,created,modified,job_created,finished,uuid,parent_uuid,event,"
    "task_action,resolved_action,resolved_role,duration,start,end,failed,"
    "changed,playbook,play,task,role,job_remote_id,host_remote_id,"
    "host_name,warnings,deprecations,playbook_on_stats",

    "1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "d34ed5d9-fce8-427b-94e3-ed46f81e0071,,runner_on_start,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,1,31,"
    "default_host_1_2025-06-13,,,",

    "2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "765ea843-a379-4c6e-a89a-df8c37b3cfec,,runner_on_ok,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,1,31,"
    "default_host_1_2025-06-13,,,",

    "3,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "984de595-b7df-4107-83cf-101defdef80f,,runner_on_start,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,1,32,"
    "default_host_2_2025-06-13,,,",

    "4,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "07c717dd-4e7b-43b0-9bb9-257ada77cb19,,runner_on_ok,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,1,32,"
    "default_host_2_2025-06-13,,,",

    "5,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "0f1745e3-b611-4a21-9684-5e6cb6f6307f,,runner_on_start,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,2,31,"
    "default_host_1_2025-06-13,,,",

    "6,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "3b83b139-fd0a-4ede-9a58-d0a6c50faa9d,,runner_on_ok,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,2,31,"
    "default_host_1_2025-06-13,,,",

    "7,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "3edfc47b-09fe-4cb0-89d1-6c7ed6bcbc17,,runner_on_start,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,2,32,"
    "default_host_2_2025-06-13,,,",

    "8,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "8f3fbb4a-c0c0-44d2-a2a3-d188ce40b27b,,runner_on_ok,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,2,32,"
    "default_host_2_2025-06-13,,,",

    "9,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "72b20910-9623-43fd-935e-ef78623ae60c,,runner_on_start,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,3,31,"
    "default_host_1_2025-06-13,,,",

    "10,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "d0e307d0-c787-4bb4-b93c-05b97423b395,,runner_on_ok,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,3,31,"
    "default_host_1_2025-06-13,,,",

    "11,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "51858aba-7fd9-4f40-b2e1-1fe93cc78d8b,,runner_on_start,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,3,32,"
    "default_host_2_2025-06-13,,,",

    "12,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,"
    "ed8c6f8b-4ac9-4247-8a6c-3f4c2d96b157,,runner_on_ok,,,,,,,f,f,"
    "default_playbook.yml,default_play,default_task,default_role,3,32,"
    "default_host_2_2025-06-13,,,",
]

main_jobevent_service_skip_columns = [
    'id',
    'job_remote_id',
    'host_remote_id',
]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_main_jobevent_service_command(cleanup_glob):
    """Build and validate main_jobevent_service.csv contents in the generated tarball."""
    # prepare env

    test_env = env_vars.copy()
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'main_jobevent_service'

    # run the gather command
    run_gather_ext(test_env, ['--ship', '--force', '--since=2025-06-12', '--until=2025-06-14'])

    # validate CSV inside generated tarball(s)
    validate_csv_in_tarballs(file_paths, 'main_jobevent_service.csv', main_jobevent_service_lines, main_jobevent_service_skip_columns)

