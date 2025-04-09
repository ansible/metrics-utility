from datetime import datetime

import pytest

from conftest import validate_sheet_columns, validate_sheet_tab_names

from metrics_utility.test.util import run_build_ext, run_build_int


env_vars = {
    'METRICS_UTILITY_PRICE_PER_NODE': '11.55',
    'METRICS_UTILITY_REPORT_RHN_LOGIN': 'test_login',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_REPORT_SKU_DESCRIPTION': 'EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)',
    'METRICS_UTILITY_REPORT_H1_HEADING': 'CCSP NA Direct Reporting Template',
    'METRICS_UTILITY_REPORT_PO_NUMBER': '123',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_COMPANY_NAME': 'Partner A',
    'METRICS_UTILITY_REPORT_SKU': 'MCT3752MO',
    'METRICS_UTILITY_REPORT_EMAIL': 'email@email.com',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSP',
}

file_path = './metrics_utility/test/test_data/reports/2024/02/CCSP-2024-02.xlsx'


expected_sheets = {
    'Usage Reporting': [
        {
            'SKU': [
                'CCSP Company Name',
                'CCSP Email',
                'CCSP RHN Login',
                'Report Period (YYYY-MM)',
                'Company Business leader ',
                'Company Procurement leader ',
                'Periodicity',
                None,
                'Monthly payment',
                None,
                None,
                None,
                'SKU',
                'MCT3752MO',
                None,
                'Organization name (i.e. company name)',
                'Default',
                'test organization',
                'Sum of monthly payment',
            ]
        },
        {
            'SKU Description': [
                'Partner A',
                'email@email.com',
                'test_login',
                '2024-02',
                None,
                None,
                'Report is submitted monthly',
                None,
                None,
                None,
                None,
                None,
                'SKU Description',
                'EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)',
                None,
                "Please Mark With An 'X' If The Usage Is Internal. \nOtherwise Leave Blank",
                None,
                None,
                None,
            ]
        },
        {
            '': [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'Red Hat SKU\n Quantity Consumed',
                1,
                2,
                None,
            ]
        },
        {
            'Term': [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'Term',
                'MONTH',
                None,
                'Subscription Fee\n (SKU Unit Price)',
                11.55,
                11.55,
                None,
            ]
        },
        {
            'Unit of Measure': [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'Unit of Measure',
                'MANAGED NODE',
                None,
                'Extended\n Subscription Fees\n (SKU Extended Unit Price)',
                '=C18*D18',
                '=C19*D19',
                '=SUM(E18:E19)',
            ]
        },
        {
            'Currency': [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'Currency',
                'USD',
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {
            'MSRP': [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                'MSRP',
                '11.55',
                None,
                None,
                None,
                None,
                None,
            ]
        },
    ],
    'Managed nodes': [
        {'Host name': ['localhost', 'test host 1', 'test host 2']},
        {'automated by organizations': [1, 1, 1]},
        {'job runs': [2, 2, 2]},
        {'number of task runs': [4, 4, 4]},
        {
            'first automation': [
                datetime(2024, 2, 28, 8, 48, 36, 37000),
                datetime(2024, 2, 28, 8, 48, 41, 638000),
                datetime(2024, 2, 28, 8, 48, 41, 638000),
            ]
        },
        {
            'last automation': [
                datetime(2024, 2, 28, 8, 48, 50, 35000),
                datetime(2024, 2, 28, 8, 48, 58, 766000),
                datetime(2024, 2, 28, 8, 48, 58, 766000),
            ]
        },
    ],
    'Usage by collections': [
        {'Collection name': []},
        {'Unique managed nodes\nautomated': []},
        {'Non-unique managed\nnodes automated': []},
        {'Number of task\nruns': []},
        {'Duration of task\nruns [seconds]': []},
    ],
    'Usage by roles': [
        {'Role name': []},
        {'Unique managed nodes\nautomated': []},
        {'Non-unique managed\nnodes automated': []},
        {'Number of task\nruns': []},
        {'Duration of task\nruns [seconds]': []},
    ],
    'Usage by modules': [
        {'Module name': []},
        {'Unique managed nodes\nautomated': []},
        {'Non-unique managed\nnodes automated': []},
        {'Number of task\nruns': []},
        {'Duration of task\nruns [seconds]': []},
    ],
}


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

    run_build_ext(env_vars, ['--month=2024-02', '--force'])

    validate_sheet_columns(file_path, expected_sheets, 14)
    validate_sheet_tab_names(file_path, expected_sheets)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_import(cleanup):
    # test_command doesn't collect coverage
    run_build_int(
        env_vars,
        {
            'month': '2024-02',
            'force': True,
        },
    )

    validate_sheet_columns(file_path, expected_sheets, 14)
    validate_sheet_tab_names(file_path, expected_sheets)
