from datetime import datetime

import openpyxl
import pytest

from conftest import validate_cell, validate_sheet_columns, validate_sheet_tab_names
from dateutil.relativedelta import relativedelta

from metrics_utility.test.util import run_build_int


# Get current date and time
now = datetime.now()
diff_months = 2000

# Extract current date components with leading zeros
current_month = f'{now.month:02}'
current_day = f'{now.day:02}'
current_year = str(now.year)

start = now - relativedelta(months=diff_months)

# Extract start date components with leading zeros
start_year = str(start.year)
start_month = f'{start.month:02}'
start_day = f'{start.day:02}'


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'controller_db',
    'METRICS_UTILITY_REPORT_TYPE': 'RENEWAL_GUIDANCE',
}

file_path = (
    f'./metrics_utility/test/test_data/reports/{current_year}/{current_month}/'
    f'RENEWAL_GUIDANCE-{start_year}-{start_month}-{start_day}--{current_year}-{current_month}-{current_day}.xlsx'
)


def convert_datetime_strings(obj, fmt='%Y-%m-%d %H:%M:%S'):
    """
    Recursively traverses a nested structure and converts any string matching
    the datetime format into a datetime.datetime object.
    """
    if isinstance(obj, dict):
        return {k: convert_datetime_strings(v, fmt) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_strings(item, fmt) for item in obj]
    elif isinstance(obj, str):
        try:
            return datetime.strptime(obj, fmt)
        except ValueError:
            return obj  # Not a datetime string
    else:
        return obj  # Return other types as-is


expected_sheets = {
    'Managed nodes': [
        {
            'Host name': [
                'default_host_hostmetric_1_2025-06-13',
                'default_host_hostmetric_3_2025-06-13',
                'default_host_hostmetric_4_2025-06-13',
                'default_host_hostmetric_7_2025-06-13',
                'default_host_hostmetric_9_2025-06-13',
            ]
        },
        {
            'First\nautomation': [
                '2025-06-01 08:00:00',
                '2025-06-03 12:00:00',
                '2025-06-02 07:30:00',
                '2025-06-04 10:30:00',
                '2025-06-05 08:30:00',
            ]
        },
        {
            'Last\nautomation': [
                '2025-06-10 14:30:00',
                '2025-06-11 13:45:00',
                '2025-06-09 15:30:00',
                '2025-06-10 12:30:00',
                '2025-06-10 16:00:00',
            ]
        },
        {
            'Number of\nAutomations': [
                36,
                21,
                30,
                24,
                27,
            ]
        },
        {
            'Number of days\nbetween first_automation\nand last_automation': [
                9,
                8,
                7,
                6,
                5,
            ]
        },
        {
            'Number of\nDeletions': [
                0,
                0,
                0,
                0,
                0,
            ]
        },
        {
            'Last\ndeleted': [
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {
            'HostMetric\nrecord count': [
                1,
                1,
                1,
                1,
                1,
            ]
        },
        {
            'HostMetric active\nrecord count': [
                1,
                1,
                1,
                1,
                1,
            ]
        },
        {
            'HostMetric deleted\nrecord count': [
                0,
                0,
                0,
                0,
                0,
            ]
        },
        {
            'Host names': [
                'default_host_hostmetric_1_2025-06-13',
                'default_host_hostmetric_3_2025-06-13',
                'default_host_hostmetric_4_2025-06-13',
                'default_host_hostmetric_7_2025-06-13',
                'default_host_hostmetric_9_2025-06-13',
            ]
        },
        {
            'Variables ansible_host': [
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {
            'Serial Numbers': [
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {
            'Machine UUIDs': [
                None,
                None,
                None,
                None,
                None,
            ]
        },
    ],
    'Deleted Managed nodes': [
        {
            'Host name': [
                'default_host_hostmetric_2_2025-06-13',
                'default_host_hostmetric_5_2025-06-13',
                'default_host_hostmetric_6_2025-06-13',
                'default_host_hostmetric_8_2025-06-13',
            ]
        },
        {
            'First\nautomation': [
                '2025-06-28 09:15:00',
                '2025-06-30 10:00:00',
                '2025-06-01 06:45:00',
                '2025-06-29 09:45:00',
            ]
        },
        {
            'Last\nautomation': [
                '2025-06-12 16:00:00',
                '2025-06-08 11:00:00',
                '2025-06-06 13:15:00',
                '2025-06-07 14:00:00',
            ]
        },
        {
            'Number of\nAutomations': [
                15,
                9,
                18,
                12,
            ]
        },
        {
            'Number of days\nbetween first_automation\nand last_automation': [
                0,
                0,
                5,
                0,
            ]
        },
        {
            'Number of\nDeletions': [
                3,
                6,
                3,
                3,
            ]
        },
        {
            'Last\ndeleted': [
                '2025-06-20 10:00:00',
                '2025-06-15 12:00:00',
                None,
                '2025-06-13 09:30:00',
            ]
        },
        {
            'HostMetric\nrecord count': [
                1,
                1,
                1,
                1,
            ]
        },
        {
            'HostMetric active\nrecord count': [
                0,
                0,
                0,
                0,
            ]
        },
        {
            'HostMetric deleted\nrecord count': [
                1,
                1,
                1,
                1,
            ]
        },
        {
            'Host names': [
                'default_host_hostmetric_2_2025-06-13',
                'default_host_hostmetric_5_2025-06-13',
                'default_host_hostmetric_6_2025-06-13',
                'default_host_hostmetric_8_2025-06-13',
            ]
        },
        {
            'Variables ansible_host': [
                None,
                None,
                None,
                None,
            ]
        },
        {
            'Serial Numbers': [
                None,
                None,
                None,
                None,
            ]
        },
        {
            'Machine UUIDs': [
                None,
                None,
                None,
                None,
            ]
        },
    ],
}


expected_sheets = convert_datetime_strings(expected_sheets)


@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_import(cleanup):
    """Build xlsx report using build command and test its contents."""

    run_build_int(
        env_vars,
        {
            'since': f'{diff_months}mo',
            'force': True,
        },
    )
    validate_sheet_columns(file_path, expected_sheets, 6)

    validate_sheet_tab_names(file_path, expected_sheets, ['Usage Reporting'])

    wb = openpyxl.load_workbook(file_path)
    try:
        sh = 'Usage Reporting'
        validate_cell(wb, sh, 'A1', 'Renewal guidance')
        validate_cell(wb, sh, 'A2', 'Report Period (YYYY-MM-DD, YYYY-MM-DD)')
        validate_cell(wb, sh, 'A3', 'Description')
        validate_cell(wb, sh, 'A4', 'Automated hosts')
        validate_cell(wb, sh, 'A5', 'Deleted Automated hosts')

        validate_cell(wb, sh, 'B1', None)
        validate_cell(wb, sh, 'B2', f'{start_year}-{start_month}-{start_day}, {current_year}-{current_month}-{current_day}')
        validate_cell(wb, sh, 'B3', 'Quantity')
        validate_cell(wb, sh, 'B4', 5)
        validate_cell(wb, sh, 'B5', 4)

        validate_cell(wb, sh, 'D1', now.strftime('Updated: %b %d, %Y'))

    finally:
        wb.close()
