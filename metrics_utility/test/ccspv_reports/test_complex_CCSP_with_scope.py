import openpyxl
import pandas
import pytest

from conftest import transform_sheet
from pandas import Timestamp

from metrics_utility.test.util import run_build_int


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSP',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'ccsp_summary,managed_nodes,inventory_scope,usage_by_collections,usage_by_roles,usage_by_modules',
}

file_path = './metrics_utility/test/test_data/reports/2025/03/CCSP-2025-03-01--2025-03-02.xlsx'


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
            'since': '2025-03-01',
            'until': '2025-03-02',
            'force': True,
        },
    )

    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)

        validate_managed_nodes(file_path)
        validate_inventory_scope(file_path)
        validate_usage_by_collections(file_path)
        validate_usage_by_roles(file_path)
        validate_usage_by_modules(file_path)

    finally:
        workbook.close()


def validate_managed_nodes(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Automated by organizations': 2,
            'First automation': Timestamp('2025-03-01 10:12:59.005000'),
            'Host name': 'localhost',
            'Job runs': 9,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 18,
        },
        1: {
            'Automated by organizations': 2,
            'First automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Host name': 'manually_created_host_1',
            'Job runs': 2,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 4,
        },
        2: {
            'Automated by organizations': 2,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_4',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 10,
        },
        3: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_5',
            'Job runs': 6,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 12,
        },
        4: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'real_host_new_1',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        5: {
            'Automated by organizations': 2,
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'test_host_1',
            'Job runs': 8,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 16,
        },
        6: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_2',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        7: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-03-01 10:13:16.975000'),
            'Host name': 'test_host_3',
            'Job runs': 3,
            'Last automation': Timestamp('2025-03-01 13:36:13.420000'),
            'Number of task runs': 6,
        },
        8: {
            'Automated by organizations': 1,
            'First automation': Timestamp('2025-03-01 10:13:13.303000'),
            'Host name': 'test_host_42',
            'Job runs': 5,
            'Last automation': Timestamp('2025-03-01 13:36:08.627000'),
            'Number of task runs': 10,
        },
    }


def validate_inventory_scope(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Inventory Scope')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Canonical Facts': '{"ansible_machine_id": ["98cd2859149f4fe886b22dd44c7dea41", '
            '"99cd2859149f4fe886b22dd44c7dea41"], "ansible_product_serial": '
            '["s98cd2859149f4fe886b22dd44c7dea41"]}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'localhost',
            'Inventories': '["Demo Inventory", "Test Inventory 1"]',
            'Organizations': '["Default", "Test Org 1"]',
            'Last Automation': Timestamp('2025-03-01 13:36:04.823000'),
        },
        1: {
            'Canonical Facts': '{"ansible_machine_id": ["99cd2859149f4fe886b22dd44c7dea41", '
            '"sscd2859149f4fe886b22dd44c7dea41"], "ansible_product_serial": '
            '["aacd2859149f4fe886b22dd44c7dea41", '
            '"bbcd2859149f4fe886b22dd44c7dea41"]}',
            'Facts': '{"ansible_connection_variable": ["local", "vocal"]}',
            'Host name': 'manually_created_host_1',
            'Inventories': '["Test Inventory 1", "Test Inventory 2"]',
            'Organizations': '["Test Org 1", "Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        2: {
            'Canonical Facts': '{"ansible_machine_id": ["99cd2859149f4fe886b22dd44c7dea41"]}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'real_host_4',
            'Inventories': '["Test Inventory 1", "Test Inventory 2"]',
            'Organizations': '["Test Org 1", "Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        3: {
            'Canonical Facts': '{}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'real_host_5',
            'Inventories': '["Test Inventory 2"]',
            'Organizations': '["Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        4: {
            'Canonical Facts': '{}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'real_host_new_1',
            'Inventories': '["Test Inventory 2"]',
            'Organizations': '["Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        5: {
            'Canonical Facts': '{"ansible_machine_id": ["99cd2859149f4fe886b22dd44c7dea41"]}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'test_host_1',
            'Inventories': '["Test Inventory 1", "Test Inventory 2"]',
            'Organizations': '["Test Org 1", "Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        6: {
            'Canonical Facts': '{}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'test_host_2',
            'Inventories': '["Test Inventory 2"]',
            'Organizations': '["Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        7: {
            'Canonical Facts': '{}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'test_host_3',
            'Inventories': '["Test Inventory 2"]',
            'Organizations': '["Test Org 2"]',
            'Last Automation': Timestamp('2025-03-01 13:36:09.661000'),
        },
        8: {
            'Canonical Facts': '{"ansible_machine_id": ["99cd2859149f4fe886b22dd44c7dea41"]}',
            'Facts': '{"ansible_connection_variable": ["local"]}',
            'Host name': 'test_host_42',
            'Inventories': '["Test Inventory 1"]',
            'Organizations': '["Test Org 1"]',
            'Last Automation': Timestamp('2025-03-01 13:36:04.823000'),
        },
        9: {
            'Canonical Facts': '{}',
            'Facts': '{}',
            'Host name': 'test_unreachable_host',
            'Inventories': '["Test Inventory 1"]',
            'Organizations': '["Test Org 1"]',
            'Last Automation': Timestamp('2025-03-01 13:36:04.823000'),
        },
    }


def validate_usage_by_collections(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by collections')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Collection name': 'ansible.builtin',
            'Duration of task runs [seconds]': 48.662962,
            'Non-unique managed nodes automated': 42,
            'Number of task runs': 90,
            'Unique managed nodes automated': 10,
        },
    }


def validate_usage_by_roles(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by roles')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Duration of task runs [seconds]': 48.662962,
            'Non-unique managed nodes automated': 42,
            'Number of task runs': 90,
            'Role name': 'No role used',
            'Unique managed nodes automated': 10,
        },
    }


def validate_usage_by_modules(file_path):
    sheet = pandas.read_excel(file_path, sheet_name='Usage by modules')
    assert transform_sheet(sheet.to_dict()) == {
        0: {
            'Duration of task runs [seconds]': 0.672445,
            'Module name': 'ansible.builtin.debug',
            'Non-unique managed nodes automated': 40,
            'Number of task runs': 44,
            'Unique managed nodes automated': 9,
        },
        1: {
            'Duration of task runs [seconds]': 47.990517,
            'Module name': 'ansible.builtin.gather_facts',
            'Non-unique managed nodes automated': 42,
            'Number of task runs': 46,
            'Unique managed nodes automated': 10,
        },
    }
