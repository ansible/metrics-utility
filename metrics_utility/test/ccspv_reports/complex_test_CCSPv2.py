from conftest import validate_column
from datetime import datetime
import openpyxl

import subprocess
import sys
import pytest

env_vars = {
    "METRICS_UTILITY_PRICE_PER_NODE": "11.55",
    "METRICS_UTILITY_REPORT_RHN_LOGIN": "test_login",
    "METRICS_UTILITY_SHIP_PATH": "/awx_devel/awx-dev/metrics-utility/metrics_utility/test/test_data",
    "METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME": "Customer A",
    "METRICS_UTILITY_REPORT_END_USER_STATE": "TX",
    "METRICS_UTILITY_REPORT_SKU_DESCRIPTION": "EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)",
    "METRICS_UTILITY_REPORT_H1_HEADING": "CCSP NA Direct Reporting Template",
    "METRICS_UTILITY_REPORT_END_USER_CITY": "Springfield",
    "METRICS_UTILITY_REPORT_PO_NUMBER": "123",
    "METRICS_UTILITY_SHIP_TARGET": "directory",
    "METRICS_UTILITY_REPORT_END_USER_COUNTRY": "US",
    "METRICS_UTILITY_REPORT_COMPANY_NAME": "Partner A",
    "METRICS_UTILITY_REPORT_SKU": "MCT3752MO",
    "METRICS_UTILITY_REPORT_EMAIL": "email@email.com",
    "METRICS_UTILITY_REPORT_TYPE": "CCSPv2",
    "AWX_LOGGING_MODE": "stdout",
}

file_path = "/awx_devel/awx-dev/metrics-utility/metrics_utility/test/test_data/reports/2025/02/CCSPv2-2025-02-13--2025-02-13.xlsx"

date_today = datetime.now().strftime("%b %d, %Y")

expected_sheets = {
    "Usage Reporting": [
        {
    "End User Company Name": [
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
        None
    ]
},
        {
    "Enter 'X' to Indicate\nInteral Usage": [
        'Partner A',
        'email@email.com',
        'test_login',
        '2025-02-13, 2025-02-13',
        "Enter 'X' to indicate\nInteral Usage",
        None,
        None,
        None,
        None,
        None,
        None
    ]
},
        {"End User\nCity": [None, None, None, None, 'End User\nCity', 'Springfield', None, None, None, None, None]},
        {"End User\nState/Prov": [None, None, None, None, 'End User\nState/Prov', 'TX', None, None, None, None, None]},
        {"Country Where\nSKU Consumed": [None, None, 'PO Number', None, 'Country Where\nSKU Consumed', 'US', None, None, None, None, None]},
        {"SKU Number": [None, None, '123', None, 'SKU Number', 'MCT3752MO', None, None, None, None, None]},
        {"Quantity":[None, None, None, None, 'Quantity', 4, None, None, None, None, None]},
        {
    "SKU Description": [
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
        None
    ]
},
        {"SKU Unit Price": ['Grand total', None, None, None, 'SKU Unit Price', 11.55, None, None, None, None, None]},
        {
    "SKU Extended Unit\nPrice": [
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
        '=G12*I12'
    ]
},
        {"Notes": [None, None, None, None, 'Notes', None, None, None, None, None, None]},
    ],
    "Managed nodes": [
        {"Host name": ['host1', 'localhost', 'test_host', 'test_host_1']},
        {"automated by organizations": [2, 1, 1, 1]},
        {'job runs':  [8, 1, 4, 1]},
        {'number of task runs': [12, 2, 8, 2]},
        {
            'first automation': [
                datetime(2025, 2, 13, 12, 39, 15, 342000),
                datetime(2025, 2, 13, 12, 33, 50, 933000),]
        },
        {
            'last automation': []
        },
    ],
    "Usage by organizations": [
        {"Organization name" : ['Default', 'org1']},
        {"Job runs": [ 5, 1]},
        {"Unique managed nodes automated": [4, 1]},
        {"Non-unique managed nodes automated": [12, 2]},
        {"Number of task runs":[20,4]},
    ]
}
@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize("cleanup", [file_path,], indirect=True)
def test_command(cleanup):
    """Build xlsx report using build command and test its contents."""

    python_executable = sys.executable
    result = subprocess.run(
        [python_executable, "manage.py", "build_report", "--since=2025-02-13", "--until=2025-02-13", "--force"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env_vars,
    )

    assert result.returncode == 0

    try:
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_managed_nodes(workbook)


    finally:
        workbook.close()

def validate_managed_nodes(workbook):

    validate_column(workbook, "Managed nodes", 'A', 1, 
            ['Host name',
            'host1',
            'localhost',
            'test_host',
            'test_host_1'])
    
    validate_column(workbook, "Managed nodes", 'B', 1, 
            ['Automated by organizations', 2, 1, 1, 1])

    validate_column(workbook, "Managed nodes", 'C', 1, 
                    ['Job runs', 8, 1, 4, 1])

    validate_column(workbook, "Managed nodes", 'D', 1, 
                    ['Number of task runs', 12, 2, 8, 2])

    validate_column(workbook, "Managed nodes", 'E', 1, 
                    ['First automation', 
                    '2025-02-13 12:39:15', 
                    '2025-02-13 12:33:50', 
                    '2025-02-13 12:33:46', 
                    '2025-02-13 12:33:46'])

    validate_column(workbook, "Managed nodes", 'F', 1, 
                    ['Last automation', 
                    '2025-02-13 12:49:01', 
                    '2025-02-13 12:33:50', 
                    '2025-02-13 12:33:50', 
                    '2025-02-13 12:33:46'])