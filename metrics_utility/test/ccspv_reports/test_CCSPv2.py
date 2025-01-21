import pandas as pd
import os
import subprocess
import sys

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

file_path = (
    "/awx_devel/awx-dev/metrics-utility/metrics_utility/test/test_data/reports/2024/02/CCSPv2-2024-02.xlsx"
)


EXPECTED_SHEETS = {
    "Usage Reporting": [
        "CCSP NA Direct Reporting Template",
        "Unnamed: 1",
        "Unnamed: 2",
        "Unnamed: 3",
        "Unnamed: 4",
        "Unnamed: 5",
        "Unnamed: 6",
        "Updated: Jan 21, 2025",
        "Unnamed: 8",
        "Unnamed: 9",
        "Unnamed: 10",
    ],
    "Managed nodes": [
        "Host name",
        "Automated by\norganizations",
        "Job runs",
        "Number of task\nruns",
        "First\nautomation",
        "Last\nautomation",
    ],
    "Usage by organizations": [
        "Organization name",
        "Job runs",
        "Unique managed nodes\nautomated",
        "Non-unique managed\nnodes automated",
        "Number of task\nruns",
    ],
}

def test_command():
    print("Test running")

    cleanup()

    python_executable = sys.executable
    result = subprocess.run(
        [python_executable, 'manage.py', 'build_report', '--month=2024-02', '--force'],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env_vars,
    )

    assert result.returncode == 0

    for sheet_name, expected_columns in EXPECTED_SHEETS.items():
        validate_sheet_columns(sheet_name=sheet_name, expected_columns=expected_columns)
    validate_sheet_tab_names()
    cleanup()

def validate_sheet_columns(sheet_name, expected_columns):
    """Test the column names for each sheet."""

    def normalize_column(col):
        return col.strip().replace("\n", " ").lower()

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    actual_columns = [normalize_column(col) for col in df.columns.tolist()]
    expected_columns = [normalize_column(col) for col in expected_columns]

    if actual_columns != expected_columns:
        print(f"Mismatch for sheet: {sheet_name}")
        print(f"Actual columns (normalized): {actual_columns}")
        print(f"Expected columns (normalized): {expected_columns}")
        print(f"Mismatched columns: {set(actual_columns) ^ set(expected_columns)}")

    assert (
        actual_columns == expected_columns
    ), f"Column names do not match for sheet: {sheet_name}"


def validate_sheet_tab_names():
    """Test the sheet names in the Excel file."""
    excel_data = pd.ExcelFile(file_path)
    assert excel_data.sheet_names == list(
        EXPECTED_SHEETS.keys()
    ), "Sheet names do not match."

def cleanup():
       if os.path.exists(file_path):
        os.remove(file_path)