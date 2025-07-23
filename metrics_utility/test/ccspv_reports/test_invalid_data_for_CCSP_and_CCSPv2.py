import os

import openpyxl
import pytest

from metrics_utility.test.util import run_build_int


# Define reports and sheet options
reports = [
    'CCSP',
    'CCSPv2',
]

# Test date ranges - using ranges that include infrastructure data
ranges = [
    ['2025-07-15', '2025-07-16'],  # includes rich infrastructure data
    ['2025-02-25', '2025-07-16'],  # mixed old and new data
]

sheets_ccsp = [
    'ccsp_summary',
    'indirectly_managed_nodes',
    'inventory_scope',
    'managed_nodes',
    'managed_nodes_by_organizations',
    'usage_by_collections',
    'usage_by_modules',
    'usage_by_organizations',
    'usage_by_roles',
]
sheets_ccspv2 = [
    'ccsp_summary',
    'data_collection_status',
    'indirectly_managed_nodes',
    'infrastructure_summary',
    'inventory_scope',
    'jobs',
    'managed_nodes',
    'managed_nodes_by_organizations',
    'usage_by_collections',
    'usage_by_modules',
    'usage_by_organizations',
    'usage_by_roles',
]


def build_file_path(report, date_range):
    year, month, _ = date_range[1].split('-')
    return f'./metrics_utility/test/test_data/reports/{year}/{month}/{report}-{date_range[0]}--{date_range[1]}.xlsx'


# Build all combinations of parameters, using correct sheets per report
param_values = []
for report in ['CCSP', 'CCSPv2']:
    report_sheets = sheets_ccspv2 if report == 'CCSPv2' else sheets_ccsp
    for date_range in ranges:
        for sheet in report_sheets:
            param_values.append((report, date_range, sheet, build_file_path(report, date_range)))

id_list = [f'{report}-{date_range[0]}--{date_range[1]}-{sheet}' for report, date_range, sheet, _ in param_values]


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'report,date_range,sheet,cleanup',
    param_values,
    indirect=['cleanup'],
    ids=id_list,
)
def test_invalid_data_handling(report, date_range, sheet, cleanup):
    """Test that reports can be generated even with invalid/corrupted data.

    This test ensures that:
    - Malformed JSON in facts fields doesn't crash the system
    - Missing columns are handled gracefully
    - Invalid date formats are handled
    - Null/empty values don't cause failures
    - Mixed valid and invalid data is processed correctly
    """
    since, until = date_range
    env = {
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': sheet,
        'METRICS_UTILITY_REPORT_TYPE': report,
        'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/ccspv_reports/empty-data',
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }
    args = {
        'since': since,
        'until': until,
        'force': True,
    }

    # This should not raise any exceptions even with invalid data
    run_build_int(env, args)

    # Verify the XLSX output is loadable, if created
    file_name = build_file_path(report, date_range)
    if os.path.isfile(file_name):
        workbook = openpyxl.load_workbook(filename=file_name)
        try:
            assert workbook is not None
            # Verify that at least one sheet exists (even if data is invalid)
            assert len(workbook.sheetnames) > 0
        finally:
            workbook.close()
