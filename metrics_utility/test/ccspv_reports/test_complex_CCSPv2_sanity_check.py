import openpyxl
import pandas
import pytest

from conftest import transform_sheet

from metrics_utility.test.util import run_build_int


# This test will run on all data, making sure we're backwards compatible and will generate all
# sheets. Just checking the report generation doesn't fail
env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'ccsp_summary,managed_nodes,indirectly_managed_nodes,'
    'inventory_scope,infrastructure_summary,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules,managed_nodes_by_organizations',
}

file_path = './metrics_utility/test/test_data/reports/2025/07/CCSPv2-2025-02-25--2025-07-16.xlsx'


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

    # Running a command python way, so we can work with debugger in the code
    run_build_int(
        env_vars,
        {
            'since': '2025-02-25',
            'until': '2025-07-16',
            'force': True,
        },
    )

    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)
        assert workbook is not None

        sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
        assert len(transform_sheet(sheet.to_dict())) > 0

    finally:
        workbook.close()
