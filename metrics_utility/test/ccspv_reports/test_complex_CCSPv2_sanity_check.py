import pytest

from conftest import run_report_sanity_check


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'ccsp_summary,managed_nodes,indirectly_managed_nodes,'
    'inventory_scope,infrastructure_summary,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules,managed_nodes_by_organizations',
}

file_path = './metrics_utility/test/test_data/reports/2025/07/CCSPv2-2025-02-25--2025-07-16.xlsx'


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize('cleanup', [file_path], indirect=True)
def test_command(cleanup):
    """Build xlsx report using build command and test its contents."""
    run_report_sanity_check(env_vars, file_path, since='2025-02-25', until='2025-07-16')
