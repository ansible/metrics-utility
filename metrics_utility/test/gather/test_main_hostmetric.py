from metrics_utility.test.gather.support.helpers import validate_csv_in_tarballs
from metrics_utility.test.util import run_gather_ext


uuid = '00000000-0000-0000-0000-000000000000'


def make_env(ship_path):
    return {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }


def make_paths(ship_path):
    return f'{ship_path}/data/2025/06/*/{uuid}-*.tar.gz'


main_hostmetric_lines = [
    (
        'hostname,host_id,first_automation,last_automation,automated_counter,'
        'deleted_counter,last_deleted,deleted,ansible_product_serial,'
        'ansible_machine_id,ansible_host_variable,ansible_connection_variable'
    ),
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

main_hostmetric_skip_columns = [
    'host_id',
    'ansible_product_serial',
    'ansible_machine_id',
    'ansible_host_variable',
    'ansible_connection_variable',
]


def test_main_hostmetric_command(ship_path):
    test_env = make_env(ship_path)
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'main_hostmetric'

    run_gather_ext(test_env, ['--ship', '--since=2025-06-06', '--until=2025-06-13'])

    validate_csv_in_tarballs(make_paths(ship_path), 'main_hostmetric.csv', main_hostmetric_lines, main_hostmetric_skip_columns)
