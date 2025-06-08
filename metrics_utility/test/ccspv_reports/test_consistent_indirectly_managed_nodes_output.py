import openpyxl
import pytest

from conftest import transform_sheet
from pandas import Timestamp, read_excel

from metrics_utility.test.util import run_build_int


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
file_path = './metrics_utility/test/test_data/reports/2025/02/CCSPv2-2025-02-25--2025-02-26.xlsx'


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_command(cleanup):
    indirectly_managed_nodes_and_managed_by_orgs_workbook = None
    try:
        ## Loads workbook with METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS which
        ## modifies the reports that get generated.  Primarily, this test is meant
        ## to ensure that the Indirectly Managed nodes sheet is consistent regardless of
        ## which the values associated with METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS.

        # Builds a workbook with indirectly_managed_nodes as the only value for
        # METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS
        options = {
            'since': '2025-02-25',
            'until': '2025-02-26',
            'force': True,
        }

        run_build_int(
            {
                **env_vars,
                'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'indirectly_managed_nodes',
            },
            options,
        )
        indirectly_managed_nodes_workbook = openpyxl.load_workbook(filename=file_path)
        indirectly_managed_nodes_only_sheet = read_excel(file_path, sheet_name='Indirectly Managed nodes')

        # Builds a workbook with indirectly_managed_nodes,managed_nodes_by_organizations as the only value for
        # METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS
        run_build_int(
            {
                **env_vars,
                'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'indirectly_managed_nodes,managed_nodes_by_organizations',
            },
            options,
        )

        indirectly_managed_nodes_and_managed_by_orgs_workbook = openpyxl.load_workbook(filename=file_path)
        indirectly_managed_nodes_managed_by_orgs_sheet = read_excel(file_path, sheet_name='Indirectly Managed nodes')

        # Loops through both Indirectly Managed nodes sheets. One from each workbook.
        for sheet in [indirectly_managed_nodes_only_sheet, indirectly_managed_nodes_managed_by_orgs_sheet]:
            # Asserts that each sheet has the expected column names and data values.
            validate_indirect_managed_nodes(sheet)

    finally:
        # indirectly_managed_nodes_workbook.close()
        indirectly_managed_nodes_and_managed_by_orgs_workbook.close()
        indirectly_managed_nodes_workbook.close()


def validate_indirect_managed_nodes(sheet):
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Host name': 'indirect_host_1',
            'Automated by organizations': 1,
            'Job runs': 7,
            'Number of task runs': 14,
            'First automation': Timestamp('2025-02-25 09:33:11.557000'),
            'Last automation': Timestamp('2025-02-25 10:48:56.984000'),
            'Manage Node Types': '["INDIRECT"]',
            'Canonical Facts': '{"ansible_vmware_moid": ["vm-87211", "vm-87212", "vm-87213"], '
            '"ansible_vmware_bios_uuid": ["420b1367-1e11-c9d7-4d0f-c3b3cba9ae16", '
            '"420b188b-16f2-a839-756d-c627378fdcb2", '
            '"420ba1d2-3793-215c-30f0-5957a405d4e6"], '
            '"ansible_vmware_instance_uuid": '
            '["500b1a63-d55d-bf21-c104-1617888dd7d2", '
            '"500b3d2e-9abe-8ee1-98ea-bf67b591c104", '
            '"500bb935-a219-17d7-8e7e-9296f3af6be2"]}',
            'Events': '["vmware.vmware.guest_info"]',
            'Facts': '{"device_type": ["VM"]}',
        },
        1: {
            'Host name': 'indirect_host_2',
            'Automated by organizations': 1,
            'Job runs': 5,
            'Number of task runs': 10,
            'First automation': Timestamp('2025-02-25 10:48:57.035000'),
            'Last automation': Timestamp('2025-02-25 13:42:53.114000'),
            'Manage Node Types': '["INDIRECT"]',
            'Canonical Facts': '{"ansible_vmware_moid": ["vm-87212", "vm-87213"], '
            '"ansible_vmware_bios_uuid": ["420b1367-1e11-c9d7-4d0f-c3b3cba9ae16", '
            '"420ba1d2-3793-215c-30f0-5957a405d4e6"], '
            '"ansible_vmware_instance_uuid": '
            '["500b1a63-d55d-bf21-c104-1617888dd7d2", '
            '"500b3d2e-9abe-8ee1-98ea-bf67b591c104"]}',
            'Events': '["vmware.vmware.guest_info"]',
            'Facts': '{"device_type": ["VM"]}',
        },
    }
