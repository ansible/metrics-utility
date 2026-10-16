from datetime import datetime

import pytest

from conftest import validate_sheet_columns, validate_sheet_tab_names

from metrics_utility.test.util import run_build_ext, run_build_int


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
}

file_path = './metrics_utility/test/test_data/reports/2024/02/CCSPv2-2024-02.xlsx'


expected_sheets = {
    'Usage Reporting': [
        {
            'End User Company Name': [
                'CCSP Company Name',
                'CCSP Email',
                'CCSP RHN Login',
                'Report Period (YYYY-MM)',
                'End User Company Name',
                'Customer A',
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {
            "Enter 'X' to Indicate\nInteral Usage": [
                'Partner A',
                'email@email.com',
                'test_login',
                '2024-02',
                "Enter 'X' to indicate\nInteral Usage",
                None,
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {'End User\nCity': [None, None, None, None, 'End User\nCity', 'Springfield', None, None, None, None, None]},
        {'End User\nState/Prov': [None, None, None, None, 'End User\nState/Prov', 'TX', None, None, None, None, None]},
        {'Country Where\nSKU Consumed': [None, None, 'PO Number', None, 'Country Where\nSKU Consumed', 'US', None, None, None, None, None]},
        {'SKU Number': [None, None, '123', None, 'SKU Number', 'MCT3752MO', None, None, None, None, None]},
        {'Quantity': [None, None, None, None, 'Quantity', 3, None, None, None, None, None]},
        {
            'SKU Description': [
                None,
                None,
                None,
                None,
                'SKU Description',
                'EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)',
                None,
                None,
                None,
                None,
                None,
            ]
        },
        {'SKU Unit Price': ['Grand total', None, None, None, 'SKU Unit Price', 11.55, None, None, None, None, None]},
        {
            'SKU Extended Unit\nPrice': [
                '=SUM(J7:J12)',
                None,
                None,
                None,
                'SKU Extended Unit\nPrice',
                '=G7*I7',
                '=G8*I8',
                '=G9*I9',
                '=G10*I10',
                '=G11*I11',
                '=G12*I12',
            ]
        },
        {'Notes': [None, None, None, None, 'Notes', None, None, None, None, None, None]},
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
    'Usage by organizations': [
        {'Organization name': ['Default', 'test organization']},
        {'Job runs': [2, 2]},
        {'Unique managed nodes automated': [1, 2]},
        {'Non-unique managed nodes automated': [2, 4]},
        {'Number of task runs': [4, 8]},
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

    validate_sheet_columns(file_path, expected_sheets, 6)
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
    """Build xlsx report using build command and test its contents."""

    run_build_int(
        env_vars,
        {
            'month': '2024-02',
            'force': True,
        },
    )

    validate_sheet_columns(file_path, expected_sheets, 6)
    validate_sheet_tab_names(file_path, expected_sheets)
