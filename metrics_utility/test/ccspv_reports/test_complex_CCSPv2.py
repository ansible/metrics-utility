from conftest import transform_sheet, temporary_env
from datetime import datetime
from metrics_utility.management.commands.build_report import Command

import openpyxl

import pandas
import pytest

from pandas import Timestamp


env_vars = {
    'METRICS_UTILITY_PRICE_PER_NODE': '11.55',
    'METRICS_UTILITY_REPORT_RHN_LOGIN': 'test_login',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME': 'Customer A',
    'METRICS_UTILITY_REPORT_END_USER_STATE': 'TX',
    'METRICS_UTILITY_REPORT_SKU_DESCRIPTION': 'EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)',
    'METRICS_UTILITY_REPORT_H1_HEADING': 'CCSP NA Direct Reporting Template',
    'METRICS_UTILITY_REPORT_END_USER_CITY': 'Springfield',
    'METRICS_UTILITY_REPORT_PO_NUMBER': '123',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_END_USER_COUNTRY': 'US',
    'METRICS_UTILITY_REPORT_COMPANY_NAME': 'Partner A',
    'METRICS_UTILITY_REPORT_SKU': 'MCT3752MO',
    'METRICS_UTILITY_REPORT_EMAIL': 'email@email.com',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'ccsp_summary,managed_nodes,usage_by_organizations,'
    'usage_by_collections,usage_by_roles,usage_by_modules',
}

file_path = './metrics_utility/test/test_data/reports/2025/02/CCSPv2-2025-02-13--2025-02-13.xlsx'

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

    with temporary_env(env_vars):
        options = {
            'since': '2025-02-13',
            'until': '2025-02-13',
            'ephemeral': None,
            'force': True,
            'verbose': False,
        }

        # Instantiate your command
        command = Command()

        # Call the handle() method directly with the options.
        command.handle(**options)

    try:
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_managed_nodes(file_path)
        validate_usage_by_organization(file_path)
        validate_usage_by_collections(file_path)
        validate_usage_by_roles(file_path)
        validate_usage_by_modules(file_path)

    finally:
        workbook.close()


def validate_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 2,
            'First automation': Timestamp('2025-02-13 12:39:15.342000'),
            'Host name': 'host1',
            'Job runs': 8,
            'Last automation': Timestamp('2025-02-13 12:49:01.047000'),
            'Number of task runs': 12,
        },
        1: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-13 12:33:50.933000'),
            'Host name': 'localhost',
            'Job runs': 1,
            'Last automation': Timestamp('2025-02-13 12:33:50.933000'),
            'Number of task runs': 2,
        },
        2: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-13 12:33:46.093000'),
            'Host name': 'test_host',
            'Job runs': 4,
            'Last automation': Timestamp('2025-02-13 12:33:50.933000'),
            'Number of task runs': 8,
        },
        3: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-13 12:33:46.093000'),
            'Host name': 'test_host_1',
            'Job runs': 1,
            'Last automation': Timestamp('2025-02-13 12:33:46.093000'),
            'Number of task runs': 2,
        },
    }


def validate_usage_by_organization(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by organizations')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Job runs': 5,
            'Non-unique managed nodes automated': 12,
            'Number of task runs': 20,
            'Organization name': 'Default',
            'Unique managed nodes automated': 4,
        },
        1: {
            'Job runs': 1,
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 4,
            'Organization name': 'org1',
            'Unique managed nodes automated': 1,
        },
    }


def validate_usage_by_collections(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by collections')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Collection name': 'ansible.builtin',
            'Duration of task runs [seconds]': 22.055472,
            'Non-unique managed nodes automated': 8,
            'Number of task runs': 22,
            'Unique managed nodes automated': 4,
        },
        1: {
            'Collection name': 'ansible.builtin2',
            'Duration of task runs [seconds]': 0.802726,
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 2,
            'Unique managed nodes automated': 1,
        },
    }


def validate_usage_by_roles(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by roles')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Duration of task runs [seconds]': 22.055472,
            'Non-unique managed nodes automated': 8,
            'Number of task runs': 22,
            'Role name': 'No role used',
            'Unique managed nodes automated': 4,
        },
        1: {
            'Duration of task runs [seconds]': 0.802726,
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 2,
            'Role name': 'ansible.builtin2.role',
            'Unique managed nodes automated': 1,
        },
    }


def validate_usage_by_modules(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by modules')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Duration of task runs [seconds]': 0.119905,
            'Module name': 'ansible.builtin.debug',
            'Non-unique managed nodes automated': 6,
            'Number of task runs': 9,
            'Unique managed nodes automated': 4,
        },
        1: {
            'Duration of task runs [seconds]': 21.935567,
            'Module name': 'ansible.builtin.gather_facts',
            'Non-unique managed nodes automated': 8,
            'Number of task runs': 13,
            'Unique managed nodes automated': 4,
        },
        2: {
            'Duration of task runs [seconds]': 0.011992,
            'Module name': 'ansible.builtin2.debug',
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 1,
            'Unique managed nodes automated': 1,
        },
        3: {
            'Duration of task runs [seconds]': 0.790734,
            'Module name': 'ansible.builtin2.gather_facts',
            'Non-unique managed nodes automated': 1,
            'Number of task runs': 1,
            'Unique managed nodes automated': 1,
        },
    }
