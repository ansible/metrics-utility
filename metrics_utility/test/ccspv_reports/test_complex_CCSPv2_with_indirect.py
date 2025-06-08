import openpyxl
import pandas
import pytest

from conftest import transform_sheet
from pandas import Timestamp

from metrics_utility.test.util import run_build_int


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'ccsp_summary,managed_nodes,indirectly_managed_nodes,usage_by_organizations',
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
    """Build xlsx report using build command and test its contents."""

    # Running a command python way, so we can work with debugger in the code
    run_build_int(
        env_vars,
        {
            'since': '2025-02-25',
            'until': '2025-02-26',
            'force': True,
        },
    )

    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_usage_reporting(workbook)
        validate_managed_nodes(file_path)
        validate_indirect_managed_nodes(file_path)
        validate_usage_by_organization(file_path)

    finally:
        workbook.close()


def validate_usage_reporting(workbook):
    sheet = workbook['Usage Reporting']

    # We have to count only direct hosts, not indirects
    value = sheet['G7'].value
    assert value == 3


def validate_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 08:35:52.345000'),
            'Host name': 'host_1',
            'Job runs': 4,
            'Last automation': Timestamp('2025-02-25 08:39:08.049000'),
            'Number of task runs': 16,
        },
        1: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 08:35:52.345000'),
            'Host name': 'host_2',
            'Job runs': 2,
            'Last automation': Timestamp('2025-02-25 08:39:08.049000'),
            'Number of task runs': 8,
        },
        2: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-02-25 08:35:52.345000'),
            'Host name': 'localhost',
            'Job runs': 19,
            'Last automation': Timestamp('2025-02-25 12:27:58.985000'),
            'Number of task runs': 66,
        },
    }


def validate_indirect_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Indirectly Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 1,
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
            'First automation': Timestamp('2025-02-25 09:33:11.557000'),
            'Host name': 'indirect_host_1',
            'Job runs': 7,
            'Last automation': Timestamp('2025-02-25 10:48:56.984000'),
            'Manage Node Types': '["INDIRECT"]',
            'Number of task runs': 14,
        },
        1: {
            'Automated by organizations': 1,
            'Canonical Facts': '{"ansible_vmware_moid": ["vm-87212", "vm-87213"], '
            '"ansible_vmware_bios_uuid": ["420b1367-1e11-c9d7-4d0f-c3b3cba9ae16", '
            '"420ba1d2-3793-215c-30f0-5957a405d4e6"], '
            '"ansible_vmware_instance_uuid": '
            '["500b1a63-d55d-bf21-c104-1617888dd7d2", '
            '"500b3d2e-9abe-8ee1-98ea-bf67b591c104"]}',
            'Events': '["vmware.vmware.guest_info"]',
            'Facts': '{"device_type": ["VM"]}',
            'First automation': Timestamp('2025-02-25 10:48:57.035000'),
            'Host name': 'indirect_host_2',
            'Job runs': 5,
            'Last automation': Timestamp('2025-02-25 13:42:53.114000'),
            'Manage Node Types': '["INDIRECT"]',
            'Number of task runs': 10,
        },
    }


def validate_usage_by_organization(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by organizations')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Job runs': 19,
            'Non-unique indirect managed nodes automated': 12,
            'Non-unique managed nodes automated': 25,
            'Number of task runs': 114,
            'Organization name': 'Default',
            'Unique indirect managed nodes automated': 2,
            'Unique managed nodes automated': 3,
        },
    }
