import pandas as pd
import pytest

file_path = "/awx_devel/awx-dev/metrics-utility/shipped_data/billing/reports/2025/01/CCSPv2-2025-01.xlsx"

EXPECTED_SHEETS = {
    "Usage Reporting": [
        'CCSP NA Direct Reporting Template', 'Unnamed: 1', 'Unnamed: 2',
        'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6',
        'Updated: Jan 17, 2025', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10'
    ],
    "Managed nodes": [
        'Host name', 'Automated by\norganizations', 'Job runs',
        'Number of task\nruns', 'First\nautomation', 'Last\nautomation'
    ],
    "Usage by organizations": [
        'Organization name', 'Job runs',
        'Unique managed nodes\nautomated', 'Non-unique managed\nnodes automated',
        'Number of task\nruns'
    ],
    "Usage by collections": [
        'Collection name', 'Unique managed nodes\nautomated',
        'Non-unique managed\nnodes automated', 'Number of task\nruns',
        'Duration of task\nruns [seconds]'
    ],
    "Usage by roles": [
        'Role name', 'Unique managed nodes\nautomated',
        'Non-unique managed\nnodes automated', 'Number of task\nruns',
        'Duration of task\nruns [seconds]'
    ],
    "Usage by modules": [
        'Module name', 'Unique managed nodes\nautomated',
        'Non-unique managed\nnodes automated', 'Number of task\nruns',
        'Duration of task\nruns [seconds]'
    ],
}


@pytest.mark.parametrize("sheet_name, expected_columns", EXPECTED_SHEETS.items())
def test_sheet_columns(sheet_name, expected_columns):
    """Test the column names for each sheet."""
    def normalize_column(col):
        return col.strip().replace("\\n", "\n").replace("\n", " ").lower()

    df = pd.read_excel(file_path, sheet_name=sheet_name)
    actual_columns = [normalize_column(col) for col in df.columns.tolist()]
    expected_columns = [normalize_column(col) for col in expected_columns]

    if actual_columns != expected_columns:
        print(f"Mismatch for sheet: {sheet_name}")
        print(f"Actual columns (normalized): {actual_columns}")
        print(f"Expected columns (normalized): {expected_columns}")
        print(f"Mismatched columns: {set(actual_columns) ^ set(expected_columns)}")

    assert actual_columns == expected_columns, f"Column names do not match for sheet: {sheet_name}"




def test_sheet_tab_names():
    """Test the sheet names in the Excel file."""
    excel_data = pd.ExcelFile(file_path)
    assert excel_data.sheet_names == list(EXPECTED_SHEETS.keys()), "Sheet names do not match."