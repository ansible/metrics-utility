import openpyxl
import pandas
import pytest

from conftest import transform_sheet
from pandas import NaT, Timestamp

from metrics_utility.test.util import run_build_int


env_vars = {
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'jobs,managed_nodes,usage_by_organizations,managed_nodes_by_organizations',
}

file_path = './metrics_utility/test/test_data/reports/2025/03/CCSPv2-2025-03-01--2025-03-02.xlsx'


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_command(cleanup):
    """Build xlsx report using build command and test its contents."""

    run_build_int(
        env_vars,
        {
            'since': '2025-03-01',
            'until': '2025-03-02',
            'force': True,
        },
    )

    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_jobs(file_path)
        validate_managed_nodes(file_path)
        validate_usage_by_organization(file_path)
        validate_dynamic_sheets(file_path)

    finally:
        workbook.close()


def validate_jobs(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Jobs')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'First run': Timestamp('2025-03-01 10:12:55.043000'),
            'Job runs': 4,
            'Job template name': 'Demo Job Template',
            'Last run': Timestamp('2025-03-01 10:13:29.590000'),
            'Non-unique managed nodes automated': 4,
            'Number of task runs': 8,
            'Organization name': 'Default',
            'Unique managed nodes automated': 1,
        },
        1: {
            'First run': Timestamp('2025-03-01 10:13:02.854000'),
            'Job runs': 5,
            'Job template name': 'Test Job Template 1',
            'Last run': Timestamp('2025-03-01 13:36:04.823000'),
            'Non-unique managed nodes automated': 18,
            'Number of task runs': 36,
            'Organization name': 'Test Org 1',
            'Unique managed nodes automated': 5,
        },
        2: {
            'First run': Timestamp('2025-03-01 10:13:05.201000'),
            'Job runs': 3,
            'Job template name': 'Test Job Template 2',
            'Last run': Timestamp('2025-03-01 13:36:09.661000'),
            'Non-unique managed nodes automated': 22,
            'Number of task runs': 44,
            'Organization name': 'Test Org 2',
            'Unique managed nodes automated': 7,
        },
    }


def validate_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 2,
            'Default': Timestamp('2025-03-01 10:13:34.244000'),
            'First automation': Timestamp('2025-03-01 10:12:59.005000'),
            'Host name': 'localhost',
            'Job runs': 9,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 18,
            'Test Org 1': Timestamp('2025-03-01 13:36:08.627000'),
            'Test Org 2': NaT,
        },
        1: {
            'Automated by organizations': 2,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Host name': 'manually_created_host_1',
            'Job runs': 2,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 4,
            'Test Org 1': Timestamp('2025-03-01 13:36:08.627000'),
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        2: {
            'Automated by organizations': 2,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_4',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 10,
            'Test Org 1': Timestamp('2025-03-01 13:36:08.627000'),
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        3: {
            'Automated by organizations': 1,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_5',
            'Job runs': 6,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 12,
            'Test Org 1': NaT,
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        4: {
            'Automated by organizations': 1,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_new_1',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
            'Test Org 1': NaT,
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        5: {
            'Automated by organizations': 2,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'test_host_1',
            'Job runs': 8,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 16,
            'Test Org 1': Timestamp('2025-03-01 13:36:08.627000'),
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        6: {
            'Automated by organizations': 1,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_2',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
            'Test Org 1': NaT,
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        7: {
            'Automated by organizations': 1,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_3',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
            'Test Org 1': NaT,
            'Test Org 2': Timestamp('2025-03-01 13:36:13.420000'),
        },
        8: {
            'Automated by organizations': 1,
            'Default': NaT,
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'test_host_42',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 10,
            'Test Org 1': Timestamp('2025-03-01 13:36:08.627000'),
            'Test Org 2': NaT,
        },
    }


def validate_usage_by_organization(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by organizations')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Job runs': 4,
            'Non-unique managed nodes automated': 4,
            'Number of task runs': 8,
            'Organization name': 'Default',
            'Unique managed nodes automated': 1,
        },
        1: {
            'Job runs': 5,
            'Non-unique managed nodes automated': 18,
            'Number of task runs': 36,
            'Organization name': 'Test Org 1',
            'Unique managed nodes automated': 5,
        },
        2: {
            'Job runs': 3,
            'Non-unique managed nodes automated': 22,
            'Number of task runs': 44,
            'Organization name': 'Test Org 2',
            'Unique managed nodes automated': 7,
        },
    }


def validate_dynamic_sheets(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Default')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'First automation': Timestamp('2025-03-01 10:12:59.005000'),
            'Host name': 'localhost',
            'Job runs': 4,
            'Last automation': Timestamp('2025-03-01 10:13:34.244000'),
            'Number of task runs': 8,
        },
    }

    sheet = pandas.read_excel(file_path, sheet_name='Test Org 1')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'localhost',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 10,
        },
        1: {
            'First automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Host name': 'manually_created_host_1',
            'Job runs': 1,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 2,
        },
        2: {
            'First automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Host name': 'real_host_4',
            'Job runs': 2,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 4,
        },
        3: {
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'test_host_1',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 10,
        },
        4: {
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'test_host_42',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 10,
        },
    }

    sheet = pandas.read_excel(file_path, sheet_name='Test Org 2')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'First automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Host name': 'manually_created_host_1',
            'Job runs': 1,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 2,
        },
        1: {
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_4',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        2: {
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_5',
            'Job runs': 6,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 12,
        },
        3: {
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_new_1',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        4: {
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_1',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        5: {
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_2',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        6: {
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_3',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
    }
