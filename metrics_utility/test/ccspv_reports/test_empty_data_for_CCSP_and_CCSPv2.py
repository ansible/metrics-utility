import os

import openpyxl
import pytest

from metrics_utility.test.util import run_build_int


# Define reports, date ranges, and sheet options
reports = [
    'CCSP',
    'CCSPv2',
]

ranges = [
    ['2025-04-02', '2025-04-02'],  # files with data
    ['2025-04-03', '2025-04-03'],  # no data at all (empty folder)
    ['2025-04-01', '2025-04-01'],  # empty csv files
    ['2025-04-01', '2025-04-03'],  # all of the above
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
    for date_range in [
        ['2025-04-02', '2025-04-02'],
        ['2025-04-03', '2025-04-03'],
        ['2025-04-01', '2025-04-01'],
        ['2025-04-01', '2025-04-03'],
    ]:
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
def test_empty_data(report, date_range, sheet, cleanup):
    since, until = date_range
    env = {
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': sheet,
        'METRICS_UTILITY_REPORT_TYPE': report,
        'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }
    args = {
        'since': since,
        'until': until,
        'force': True,
    }

    run_build_int(env, args)

    # Verify the XLSX output is loadable, if created
    file_name = build_file_path(report, date_range)
    if os.path.isfile(file_name):
        workbook = openpyxl.load_workbook(filename=file_name)
        try:
            assert workbook is not None
        finally:
            workbook.close()
