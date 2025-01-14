import pandas as pd
import pytest

file_path = "/awx_devel/awx-dev/metrics-utility/shipped_data/billing/reports/2025/01/RENEWAL_GUIDANCE-2024-01-14--2025-01-14.xlsx"


EXPECTED_SHEETS = {
    "Usage Reporting": ['Renewal guidance', 'Unnamed: 1', 'Unnamed: 2', 'Updated: Jan 14, 2025'],
    "Managed nodes": [
        'Host name', 'First\nautomation', 'Last\nautomation', 'Number of\nAutomations',
        'Number of days\nbetween first_automation\nand last_automation', 'Number of\nDeletions',
        'Last\ndeleted', 'HostMetric\nrecord count', 'HostMetric active\nrecord count',
        'HostMetric deleted\nrecord count', 'Host names', 'Variables ansible_host',
        'Serial Numbers', 'Machine UUIDs'
    ],
    "Managed nodes ephemeral": [
        'Host name', 'First\nautomation', 'Last\nautomation', 'Number of\nAutomations',
        'Number of days\nbetween first_automation\nand last_automation', 'Number of\nDeletions',
        'Last\ndeleted', 'HostMetric\nrecord count', 'HostMetric active\nrecord count',
        'HostMetric deleted\nrecord count', 'Host names', 'Variables ansible_host',
        'Serial Numbers', 'Machine UUIDs'
    ],
    "Managed nodes ephemeral usage": [
        'Start of the\nephemeral window', 'End of the\nephemeral window', 'Ephemeral automated hosts'
    ],
    "Deleted Managed nodes": [
        'Host name', 'First\nautomation', 'Last\nautomation', 'Number of\nAutomations',
        'Number of days\nbetween first_automation\nand last_automation', 'Number of\nDeletions',
        'Last\ndeleted', 'HostMetric\nrecord count', 'HostMetric active\nrecord count',
        'HostMetric deleted\nrecord count', 'Host names', 'Variables ansible_host',
        'Serial Numbers', 'Machine UUIDs'
    ]
}

@pytest.mark.parametrize("sheet_name, expected_columns", EXPECTED_SHEETS.items())
def test_sheet_columns(sheet_name, expected_columns):
    """Test the column names for each sheet."""

    df = pd.read_excel(file_path, sheet_name=sheet_name)

    assert df.columns.tolist() == expected_columns, f"Column names do not match for sheet: {sheet_name}"


def test_sheet_tab_names():
    """Test the sheet names in the Excel file."""
    excel_data = pd.ExcelFile(file_path)
    assert excel_data.sheet_names == list(EXPECTED_SHEETS.keys()), "Sheet names do not match."
