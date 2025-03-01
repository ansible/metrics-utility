from conftest import transform_sheet, temporary_env
from datetime import datetime
from metrics_utility.management.commands.build_report import Command

import openpyxl
import pandas

from pandas import Timestamp

import pytest

env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'managed_nodes,indirectly_managed_nodes,usage_by_organizations',
}

file_path = './metrics_utility/test/test_data/reports/2025/02/CCSPv2-2025-02-25--2025-02-25.xlsx'

date_today = datetime.now().strftime('%b %d, %Y')


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

    # Running a command python way, so we can work with debugger in the code
    with temporary_env(env_vars):
        options = {
            'since': '2025-02-25',
            'until': '2025-02-25',
            'ephemeral': None,
            'force': True,
            'verbose': False,
        }

        # Instantiate your command
        command = Command()

        # Call the handle() method directly with the options.
        command.handle(**options)

    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_managed_nodes(file_path)
        validate_indirect_managed_nodes(file_path)
        validate_usage_by_organization(file_path)

    finally:
        workbook.close()


def validate_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 08:35:52.345000'),
            'Host name': 'host_1',
            'Job runs': 4,
            'Last automation': Timestamp('2025-02-25 08:39:08.049000'),
            'Number of task runs': 8,
        },
        1: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 08:35:52.345000'),
            'Host name': 'host_2',
            'Job runs': 2,
            'Last automation': Timestamp('2025-02-25 08:39:08.049000'),
            'Number of task runs': 4,
        },
        2: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 08:35:52.345000'),
            'Host name': 'localhost',
            'Job runs': 19,
            'Last automation': Timestamp('2025-02-25 12:27:58.985000'),
            'Number of task runs': 33,
        },
    }


def validate_indirect_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Indirectly Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 09:33:11.557000'),
            'Host name': 'indirect_host_1',
            'Job runs': 7,
            'Last automation': Timestamp('2025-02-25 10:48:56.984000'),
            'Number of task runs': 7,
        },
        1: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 10:48:57.035000'),
            'Host name': 'indirect_host_2',
            'Job runs': 5,
            'Last automation': Timestamp('2025-02-25 13:42:53.114000'),
            'Number of task runs': 5,
        },
    }


def validate_usage_by_organization(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by organizations')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Job runs': 19,
            'Non-unique indirect managed nodes automated': 12,
            'Non-unique managed nodes automated': 25,
            'Number of task runs': 57,
            'Organization name': 'Default',
            'Unique indirect managed nodes automated': 2,
            'Unique managed nodes automated': 3,
        },
    }
