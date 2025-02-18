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
        validate_usage_by_organization(workbook)
        validate_usage_by_collections(workbook)
        validate_usage_by_roles(workbook)
        validate_usage_by_modules(workbook)

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

def validate_usage_by_organization(workbook):
    sheet_name = "Usage by organizations"

    validate_column(workbook, sheet_name, 'A', 1, ['Organization name', 'Default', 'org1'])
    validate_column(workbook, sheet_name, 'B', 1, ['Job runs', '5', '1'])
    validate_column(workbook, sheet_name, 'C', 1, ['Unique managed nodes automated', '4', '1'])
    validate_column(workbook, sheet_name, 'D', 1, ['Non-unique managed nodes automated', '12', '2'])
    validate_column(workbook, sheet_name, 'E', 1, ['Number of task runs', '20', '4'])

def validate_usage_by_collections(workbook):
    sheet_name = "Usage by collections"

    validate_column(workbook, sheet_name, 'A', 1, ['Collection name', 'ansible.builtin', 'ansible.builtin2'])
    validate_column(workbook, sheet_name, 'B', 1, ['Unique managed nodes automated', '4', '1'])
    validate_column(workbook, sheet_name, 'C', 1, ['Non-unique managed nodes automated', '8', '1'])
    validate_column(workbook, sheet_name, 'D', 1, ['Number of task runs', '22', '2'])
    validate_column(workbook, sheet_name, 'E', 1, ['Duration of task runs [seconds]', '22.055472', '0.802726'])

def validate_usage_by_roles(workbook):
    sheet_name = "Usage by roles"

    validate_column(workbook, sheet_name, 'A', 1, ['Role name', 'No role used', 'ansible.builtin2.role'])
    validate_column(workbook, sheet_name, 'B', 1, ['Unique managed nodes automated', '4', '1'])
    validate_column(workbook, sheet_name, 'C', 1, ['Non-unique managed nodes automated', '8', '1'])
    validate_column(workbook, sheet_name, 'D', 1, ['Number of task runs', '22', '2'])
    validate_column(workbook, sheet_name, 'E', 1, ['Duration of task runs [seconds]', '22.055472', '0.802726'])

def validate_usage_by_modules(workbook):
    sheet_name = "Usage by modules"

    validate_column(workbook, sheet_name, 'A', 1, [
        'Module name',
        'ansible.builtin.debug',
        'ansible.builtin.gather_facts',
        'ansible.builtin2.debug',
        'ansible.builtin2.gather_facts'
    ])

    validate_column(workbook, sheet_name, 'B', 1, [
        'Unique managed nodes automated',
        '4', '4', '1', '1'
    ])

    validate_column(workbook, sheet_name, 'C', 1, [
        'Non-unique managed nodes automated',
        '6', '8', '1', '1'
    ])

    validate_column(workbook, sheet_name, 'D', 1, [
        'Number of task runs',
        '9', '13', '1', '1'
    ])

    validate_column(workbook, sheet_name, 'E', 1, [
        'Duration of task runs [seconds]',
        '0.119905', '21.935567', '0.011992', '0.790734'
    ])

