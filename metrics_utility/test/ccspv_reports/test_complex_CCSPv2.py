import openpyxl
import pandas
import pytest

from conftest import transform_sheet
from pandas import Timestamp

from metrics_utility.test.util import run_build_int


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
    'usage_by_collections,usage_by_roles,usage_by_modules,data_collection_status',
    'METRICS_UTILITY_ORGANIZATION_FILTER': 'Default;org1',
}

file_path = './metrics_utility/test/test_data/reports/2025/02/CCSPv2-2025-02-13--2025-02-13.xlsx'


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_command_without_trailing_comma(cleanup):
    base_command(cleanup, env_vars)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_command_with_trailing_comma(cleanup):
    env_vars_with_trailing_comma = env_vars.copy()
    env_vars_with_trailing_comma['METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS'] += ','
    env_vars_with_trailing_comma['METRICS_UTILITY_ORGANIZATION_FILTER'] += ';'
    base_command(cleanup, env_vars_with_trailing_comma)


def base_command(cleanup, env_vars):
    """Build xlsx report using build command and test its contents."""

    run_build_int(
        env_vars,
        {
            'since': '2025-02-13',
            'until': '2025-02-13',
            'force': True,
        },
    )

    try:
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_managed_nodes(file_path)
        validate_usage_by_organization(file_path)
        validate_usage_by_collections(file_path)
        validate_usage_by_roles(file_path)
        validate_usage_by_modules(file_path)
        validate_data_collection_status(file_path)

    finally:
        workbook.close()


def validate_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 2,
            'First automation': Timestamp('2025-02-13 12:41:44.619000'),
            'Host name': 'host1',
            'Job runs': 4,
            'Last automation': Timestamp('2025-02-13 12:49:01.047000'),
            'Number of task runs': 8,
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
            'Job runs': 3,
            'Non-unique managed nodes automated': 8,
            'Number of task runs': 16,
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
            'Non-unique managed nodes automated': 10,
            'Number of task runs': 22,
            'Unique managed nodes automated': 5,
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
            'Non-unique managed nodes automated': 10,
            'Number of task runs': 22,
            'Role name': 'No role used',
            'Unique managed nodes automated': 5,
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
            'Non-unique managed nodes automated': 10,
            'Number of task runs': 13,
            'Unique managed nodes automated': 5,
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


def load_data_collection_status(file_path):
    # find the first empty row - gap between 2 tables
    sheet_both = pandas.read_excel(file_path, sheet_name='Data collection status')
    rows = sheet_both.index[sheet_both.isnull().all(axis=1)][0] + 1

    sheet_missing = pandas.read_excel(file_path, sheet_name='Data collection status', nrows=rows)
    sheet_status = pandas.read_excel(file_path, sheet_name='Data collection status', skiprows=rows + 1)

    return sheet_missing, sheet_status


def validate_data_collection_status(file_path):
    sheet_missing, sheet_status = load_data_collection_status(file_path)

    assert transform_sheet(sheet_missing.to_dict()) == {
        0: {
            'CSV filename': 'job_host_summary.csv',
            'Gap in seconds': 42557.200196,
            'Missing from': Timestamp('2025-02-13 00:00:00'),
            'Missing until': Timestamp('2025-02-13 11:49:17.200000'),
        },
        1: {
            'CSV filename': 'job_host_summary.csv',
            'Gap in seconds': 40242.797774,
            'Missing from': Timestamp('2025-02-13 12:49:17.202000'),
            'Missing until': Timestamp('2025-02-14 00:00:00'),
        },
        2: {
            'CSV filename': 'main_jobevent.csv',
            'Gap in seconds': 42557.200196,
            'Missing from': Timestamp('2025-02-13 00:00:00'),
            'Missing until': Timestamp('2025-02-13 11:49:17.200000'),
        },
        3: {
            'CSV filename': 'main_jobevent.csv',
            'Gap in seconds': 40242.797774,
            'Missing from': Timestamp('2025-02-13 12:49:17.202000'),
            'Missing until': Timestamp('2025-02-14 00:00:00'),
        },
    }

    assert transform_sheet(sheet_status.fillna(value='').to_dict()) == {
        0: {
            'CSV filename': 'job_host_summary.csv',
            'Collection timestamp': Timestamp('2025-02-13 12:49:17.232000'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-02-13 11:49:17.200000'),
            'Filter until': Timestamp('2025-02-13 12:49:17.202000'),
            'Status': 'ok',
            'Time since previous collection': '',
        },
        1: {
            'CSV filename': 'main_jobevent.csv',
            'Collection timestamp': Timestamp('2025-02-13 12:49:17.248000'),
            'Elapsed': 0,
            'Filter since': Timestamp('2025-02-13 11:49:17.200000'),
            'Filter until': Timestamp('2025-02-13 12:49:17.202000'),
            'Status': 'ok',
            'Time since previous collection': '',
        },
    }
