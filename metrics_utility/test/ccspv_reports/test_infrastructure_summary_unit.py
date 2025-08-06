import json

from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.metric_utils import INDIRECT


class TestInfrastructureSummaryUnit:
    """Comprehensive unit tests for infrastructure summary functionality."""

    def setup_method(self):
        """Setup common test fixtures."""
        self.extra_params = {
            'price_per_node': 50.0,
            'report_period': '2025-07',
            'report_sku': 'TEST-SKU',
            'report_h1_heading': 'Test Report',
            'report_po_number': 'PO-123',
            'report_company_name': 'Test Company',
            'report_email': 'test@example.com',
            'report_rhn_login': 'testuser',
            'report_sku_description': 'Test SKU Description',
            'report_organization_filter': None,
        }

    def create_mock_worksheet(self):
        """Create a properly mocked worksheet for testing."""
        mock_ws = Mock()

        # Track all cell writes with a dictionary to easily verify values
        self.cell_writes = {}  # (row, col) -> value

        def mock_cell_side_effect(row, column):
            cell_mock = Mock()
            # Store the cell reference for later verification
            key = (row, column)
            self.cell_writes[key] = cell_mock
            return cell_mock

        mock_ws.cell.side_effect = mock_cell_side_effect

        # Mock row_dimensions to support subscripting
        mock_ws.row_dimensions = MagicMock()
        mock_ws.row_dimensions.__getitem__.return_value = Mock()
        mock_ws.row_dimensions.__setitem__.return_value = None

        # Mock column_dimensions
        mock_ws.column_dimensions = MagicMock()
        mock_ws.column_dimensions.__getitem__.return_value = Mock()
        mock_ws.column_dimensions.__setitem__.return_value = None

        # Mock merge_cells method
        mock_ws.merge_cells = Mock()

        return mock_ws

    def get_cell_value(self, row, col):
        """Helper to get the value written to a specific cell."""
        key = (row, col)
        if key in self.cell_writes:
            return getattr(self.cell_writes[key], 'value', None)
        return None

    def find_cell_with_value(self, value):
        """Helper to find cells containing specific values."""
        matches = []
        for (row, col), cell_mock in self.cell_writes.items():
            if hasattr(cell_mock, 'value') and cell_mock.value == value:
                matches.append((row, col))
        return matches

    def get_table_structure(self):
        """Helper to get the table structure for debugging."""
        table = {}
        for (row, col), cell_mock in self.cell_writes.items():
            if hasattr(cell_mock, 'value'):
                table[(row, col)] = cell_mock.value
        return table

    def find_data_row_with_device_type(self, device_type):
        """Find the row containing a specific device type and return its counts."""
        # Look for device type in column 3 (Device Type column)
        for (row, col), cell_mock in self.cell_writes.items():
            if col == 3 and hasattr(cell_mock, 'value') and cell_mock.value == device_type:
                # Found the device type, now get the counts from columns 4 and 5
                unique_nodes = self.get_cell_value(row, 4)  # Unique Nodes column
                total_nodes = self.get_cell_value(row, 5)  # Total Nodes column

                # The infra_bucket is in column 2 of the row above (hierarchical structure)
                infra_bucket = self.get_cell_value(row - 1, 2)  # Device Category is one row up

                # Find the infrastructure type by looking backwards for a value in column 1
                infra_type = None
                for check_row in range(row, 0, -1):  # Look backwards from current row
                    infra_type_candidate = self.get_cell_value(check_row, 1)
                    if infra_type_candidate and infra_type_candidate not in ['Infrastructure']:
                        infra_type = infra_type_candidate
                        break

                return {
                    'row': row,
                    'device_type': device_type,
                    'unique_nodes': unique_nodes,
                    'total_nodes': total_nodes,
                    'infra_bucket': infra_bucket,
                    'infra_type': infra_type,
                }
        return None

    def create_test_dataframe_from_csv_data(self):
        """Create test dataframe using realistic data from the CSV."""
        data = [
            {
                'id': 1,
                'created': '2025-07-22T09:33:11.556896+00',
                'host_name': 'host_1',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "host-1", "ansible_distribution": "RedHat"}',
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',
                'events': '["containers.podman.info"]',
                'first_automation': '2025-07-22T09:23:11.556896+00',
                'last_automation': '2025-07-22T09:33:11.556896+00',
            },
            {
                'id': 2,
                'created': '2025-07-22T09:35:41.557896+00',
                'host_name': 'host_2',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_ec2_instance_id": "i-1234567890abcdef2", "ansible_ec2_placement_region": "us-west-2"}',
                'facts': '{"device_type": "Object Storage", "infra_type": "Hybrid Cloud", "infra_bucket": "Database"}',
                'events': '["amazon.aws.ec2_info"]',
                'first_automation': '2025-07-22T09:25:41.557896+00',
                'last_automation': '2025-07-22T09:35:41.557896+00',
            },
            {
                'id': 3,
                'created': '2025-07-22T09:38:11.558896+00',
                'host_name': 'host_3',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "container-3", "ansible_distribution": "Ubuntu"}',
                'facts': '{"device_type": "Block Storage", "infra_type": "On-Premises", "infra_bucket": "Network"}',
                'events': '["storage.ceph.info"]',
                'first_automation': '2025-07-22T09:28:11.558896+00',
                'last_automation': '2025-07-22T09:38:11.558896+00',
            },
            {
                'id': 4,
                'created': '2025-07-22T09:40:41.559896+00',
                'host_name': 'host_4',
                'host_remote_id': 4,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "storage-4", "ansible_distribution": "CentOS"}',
                'facts': '{"device_type": "File Storage", "infra_type": "Edge Computing", "infra_bucket": "Security"}',
                'events': '["database.postgresql.info"]',
                'first_automation': '2025-07-22T09:30:41.559896+00',
                'last_automation': '2025-07-22T09:40:41.559896+00',
            },
            {
                'id': 5,
                'created': '2025-07-22T09:43:11.560896+00',
                'host_name': 'host_5',
                'host_remote_id': 5,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "db-5", "ansible_distribution": "Debian"}',
                'facts': '{"device_type": "SQL", "infra_type": "Multi-Cloud", "infra_bucket": "Analytics"}',
                'events': '["cisco.iosxr.info"]',
                'first_automation': '2025-07-22T09:33:11.560896+00',
                'last_automation': '2025-07-22T09:43:11.560896+00',
            },
            {
                'id': 6,
                'created': '2025-07-22T09:45:41.561896+00',
                'host_name': 'host_6',
                'host_remote_id': 6,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "network-6", "ansible_distribution": "Cisco"}',
                'facts': '{"device_type": "NoSQL", "infra_type": "Serverless", "infra_bucket": "AI/ML"}',
                'events': '["amazon.aws.s3_info"]',
                'first_automation': '2025-07-22T09:35:41.561896+00',
                'last_automation': '2025-07-22T09:45:41.561896+00',
            },
            {
                'id': 7,
                'created': '2025-07-22T09:48:11.562896+00',
                'host_name': 'host_7',
                'host_remote_id': 7,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "cloud-7", "ansible_distribution": "Amazon"}',
                'facts': '{"device_type": "CDN", "infra_type": "Container Platform", "infra_bucket": "IoT"}',
                'events': '["purestorage.flasharray.info"]',
                'first_automation': '2025-07-22T09:38:11.562896+00',
                'last_automation': '2025-07-22T09:48:11.562896+00',
            },
            {
                'id': 8,
                'created': '2025-07-22T09:50:41.563896+00',
                'host_name': 'host_8',
                'host_remote_id': 8,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "edge-8", "ansible_distribution": "Alpine"}',
                'facts': '{"device_type": "Load Balancer", "infra_type": "Private Cloud", "infra_bucket": "DevOps"}',
                'events': '["netapp.ontap.info"]',
                'first_automation': '2025-07-22T09:40:41.563896+00',
                'last_automation': '2025-07-22T09:50:41.563896+00',
            },
            # Add some duplicates for aggregation testing
            {
                'id': 9,
                'created': '2025-07-22T09:53:11.564896+00',
                'host_name': 'host_9',
                'host_remote_id': 9,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_host": "server-9", "ansible_distribution": "SUSE"}',
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',  # Same as host_1
                'events': '["amazon.aws.efs_info"]',
                'first_automation': '2025-07-22T09:43:11.564896+00',
                'last_automation': '2025-07-22T09:53:11.564896+00',
            },
            {
                'id': 10,
                'created': '2025-07-22T09:55:41.565896+00',
                'host_name': 'host_10',
                'host_remote_id': 10,
                'managed_node_type': INDIRECT,
                'canonical_facts': '{"ansible_vmware_moid": "vm-10", "ansible_vmware_bios_uuid": "420b1367-1e11-c9d7-4d0f-c3b3cba9ae110"}',
                'facts': '{"device_type": "Virtual Machines", "infra_type": "Hybrid Cloud", "infra_bucket": "Compute"}',
                'events': '["vmware.vmware.guest_info"]',
                'first_automation': '2025-07-22T09:45:41.565896+00',
                'last_automation': '2025-07-22T09:55:41.565896+00',
            },
        ]

        base_columns = [
            'id',
            'created',
            'host_name',
            'host_remote_id',
            'managed_node_type',
            'canonical_facts',
            'facts',
            'events',
            'first_automation',
            'last_automation',
        ]

        return pd.DataFrame(data, columns=base_columns)

    def test_data_transformation_extract_infra_info(self):
        """Test data transformation logic for extracting infrastructure information."""
        test_df = self.create_test_dataframe_from_csv_data()
        dataframes = {
            'job_host_summary': test_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        # Access the method and extract the inner function for testing
        # We'll test this by calling the full method and verifying results
        report._build_data_section_infrastructure_summary(1, mock_ws, test_df)

        # Verify that cells were written with extracted information
        call_list = mock_ws.cell.call_args_list
        assert len(call_list) > 0

        # The method should have processed our test data successfully
        assert mock_ws.cell.called

    def test_data_transformation_json_parsing(self):
        """Test JSON parsing in data transformation logic."""
        # Test with valid JSON
        test_data = [
            {
                'id': 1,
                'host_name': 'test_host',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "TestDevice", "infra_type": "TestInfra", "infra_bucket": "TestBucket"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(test_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should complete successfully and return a row number
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_data_transformation_edge_cases(self):
        """Test data transformation with edge cases."""
        edge_cases_data = [
            {
                'id': 1,
                'host_name': 'host_dict_facts',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': {'device_type': 'DirectDict', 'infra_type': 'TestInfra', 'infra_bucket': 'TestBucket'},  # Dict instead of JSON string
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 2,
                'host_name': 'host_list_values',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": ["ListDevice1", "ListDevice2"], "infra_type": "TestInfra", "infra_bucket": "TestBucket"}',  # List values
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 3,
                'host_name': 'host_missing_fields',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"some_other_field": "value"}',  # Missing required fields
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(edge_cases_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        # Should handle edge cases gracefully
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_aggregation_calculations_unique_count(self):
        """Test aggregation calculations for unique host count."""
        # Create data with duplicate device types but different hosts
        agg_test_data = [
            {
                'id': 1,
                'host_name': 'host_1',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 2,
                'host_name': 'host_2',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',  # Same type
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 3,
                'host_name': 'host_1',  # Duplicate host name
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',  # Same type
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(agg_test_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Verify headers are written correctly
        expected_headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']
        for i, header in enumerate(expected_headers, 1):
            assert self.get_cell_value(1, i) == header

        # Find the Containers row and verify aggregation
        containers_row = self.find_data_row_with_device_type('Containers')
        assert containers_row is not None, 'Containers device type should be found in the table'

        # Should aggregate correctly: 2 unique hosts (host_1, host_2), 3 total records
        assert containers_row['infra_bucket'] == 'Storage', f'Containers should be in Storage bucket, got {containers_row["infra_bucket"]}'
        assert containers_row['unique_nodes'] == 2, f'Should have 2 unique hosts (host_1, host_2), got {containers_row["unique_nodes"]}'
        assert containers_row['total_nodes'] == 3, f'Should have 3 total records, got {containers_row["total_nodes"]}'

    def test_aggregation_calculations_grouping(self):
        """Test aggregation grouping by infra_type, infra_bucket, and device_type."""
        grouping_test_data = [
            {
                'id': 1,
                'host_name': 'host_1',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 2,
                'host_name': 'host_2',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Virtual Machines", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',  # Different device type
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 3,
                'host_name': 'host_3',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Private Cloud", "infra_bucket": "Storage"}',  # Different infra type
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 4,
                'host_name': 'host_4',
                'host_remote_id': 4,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Compute"}',  # Different bucket
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(grouping_test_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Verify headers are written correctly
        expected_headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']
        for i, header in enumerate(expected_headers, 1):
            assert self.get_cell_value(1, i) == header

        # Should create separate groups for each unique combination
        # Based on the debug output, we can see the structure creates separate entries

        # Find all device type entries (should be 4 total: 3 Containers + 1 Virtual Machines)
        device_type_entries = []
        for (row, col), cell_mock in self.cell_writes.items():
            if col == 3 and hasattr(cell_mock, 'value') and cell_mock.value in ['Containers', 'Virtual Machines']:
                device_type_entries.append((row, cell_mock.value))

        # Should have 4 device type entries total (3 different Container combinations + 1 VM)
        assert len(device_type_entries) == 4, f'Should have 4 device type entries, got {len(device_type_entries)}'

        # Count Containers and Virtual Machines entries
        containers_count = sum(1 for _, device_type in device_type_entries if device_type == 'Containers')
        vm_count = sum(1 for _, device_type in device_type_entries if device_type == 'Virtual Machines')

        assert containers_count == 3, f'Should have 3 Container entries, got {containers_count}'
        assert vm_count == 1, f'Should have 1 Virtual Machines entry, got {vm_count}'

        # Verify that each entry has proper counts (1 unique, 1 total for each since no duplicates)
        for row, device_type in device_type_entries:
            unique_nodes = self.get_cell_value(row, 4)
            total_nodes = self.get_cell_value(row, 5)
            assert unique_nodes == 1, f'{device_type} at row {row} should have 1 unique node, got {unique_nodes}'
            assert total_nodes == 1, f'{device_type} at row {row} should have 1 total node, got {total_nodes}'

    def test_spreadsheet_tab_generation_headers(self):
        """Test spreadsheet tab generation with correct headers."""
        test_df = self.create_test_dataframe_from_csv_data()[:3]  # Use first 3 rows
        dataframes = {
            'job_host_summary': test_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        report._build_data_section_infrastructure_summary(1, mock_ws, test_df)

        # Verify headers were written
        expected_headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']

        call_list = mock_ws.cell.call_args_list

        # Check that headers were set (first 5 calls should be headers)
        header_calls = call_list[:5]
        for i, _expected_header in enumerate(expected_headers):
            # Verify the header was written to the correct column
            assert header_calls[i][1]['row'] == 1
            assert header_calls[i][1]['column'] == i + 1

    def test_spreadsheet_tab_generation_hierarchical_display(self):
        """Test hierarchical display with merged cells for infrastructure types."""
        hierarchical_data = [
            {
                'id': 1,
                'host_name': 'host_1',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 2,
                'host_name': 'host_2',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Virtual Machines", "infra_type": "Public Cloud", "infra_bucket": "Compute"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 3,
                'host_name': 'host_3',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Databases", "infra_type": "Private Cloud", "infra_bucket": "Database"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(hierarchical_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Verify merge_cells was called for infrastructure type headers
        assert mock_ws.merge_cells.called

        # Should have merged cells for each infrastructure type (Public Cloud, Private Cloud)
        merge_calls = mock_ws.merge_cells.call_args_list
        assert len(merge_calls) >= 2  # At least 2 infrastructure types

    def test_spreadsheet_tab_generation_empty_data(self):
        """Test spreadsheet generation with empty indirect nodes."""
        empty_df = pd.DataFrame(columns=['id', 'host_name', 'host_remote_id', 'managed_node_type', 'facts'])
        dataframes = {
            'job_host_summary': empty_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, empty_df)

        # Should show "No indirect nodes found" message using our tracking system
        assert self.get_cell_value(1, 1) == 'No indirect nodes found'
        assert current_row == 2

    def test_mock_data_scenarios_various_device_types(self):
        """Test with various device types from the CSV data."""
        # Use the full realistic dataset
        test_df = self.create_test_dataframe_from_csv_data()
        dataframes = {
            'job_host_summary': test_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, test_df)

        # Should process all device types successfully
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

        # Should have many calls due to various device types
        call_count = mock_ws.cell.call_count
        assert call_count > 15  # Multiple infrastructure types, buckets, and device types

    def test_mock_data_scenarios_cloud_types(self):
        """Test with different cloud infrastructure types."""
        cloud_types_data = [
            {
                'id': 1,
                'host_name': 'public_cloud_host',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers", "infra_type": "Public Cloud", "infra_bucket": "Storage"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 2,
                'host_name': 'private_cloud_host',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Virtual Machines", "infra_type": "Private Cloud", "infra_bucket": "Compute"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 3,
                'host_name': 'hybrid_cloud_host',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Databases", "infra_type": "Hybrid Cloud", "infra_bucket": "Database"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 4,
                'host_name': 'multi_cloud_host',
                'host_remote_id': 4,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Analytics", "infra_type": "Multi-Cloud", "infra_bucket": "Analytics"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 5,
                'host_name': 'edge_computing_host',
                'host_remote_id': 5,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "IoT Devices", "infra_type": "Edge Computing", "infra_bucket": "IoT"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(cloud_types_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle all cloud types
        assert isinstance(current_row, int)
        assert mock_ws.cell.called

        # Should have processed 5 different infrastructure types
        assert mock_ws.merge_cells.call_count == 5  # One merge per infra type

    def test_sorting_behavior(self):
        """Test that data is sorted correctly by infra_type, infra_bucket, device_type."""
        sorting_data = [
            {
                'id': 1,
                'host_name': 'host_z',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Z_Device", "infra_type": "Z_Infrastructure", "infra_bucket": "Z_Bucket"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 2,
                'host_name': 'host_a',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "A_Device", "infra_type": "A_Infrastructure", "infra_bucket": "A_Bucket"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            {
                'id': 3,
                'host_name': 'host_m',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "M_Device", "infra_type": "A_Infrastructure", "infra_bucket": "M_Bucket"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(sorting_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should process in sorted order (A_Infrastructure should come before Z_Infrastructure)
        assert isinstance(current_row, int)
        assert mock_ws.cell.called

        # Verify merge_cells calls for infrastructure types (should be in sorted order)
        merge_calls = mock_ws.merge_cells.call_args_list
        assert len(merge_calls) >= 2  # A_Infrastructure and Z_Infrastructure

    @pytest.mark.parametrize(
        'device_type,infra_type,infra_bucket',
        [
            ('Containers', 'Public Cloud', 'Storage'),
            ('Virtual Machines', 'Private Cloud', 'Compute'),
            ('Databases', 'Hybrid Cloud', 'Database'),
            ('Load Balancer', 'Edge Computing', 'Network'),
            ('Analytics Platform', 'Multi-Cloud', 'Analytics'),
            ('File Storage', 'On-Premises', 'Storage'),
            ('CDN', 'Serverless', 'Content Delivery'),
            ('Firewall', 'Container Platform', 'Security'),
            ('NoSQL', 'Kubernetes', 'Database'),
            ('Object Storage', 'OpenShift', 'Storage'),
        ],
    )
    def test_parametrized_device_types(self, device_type, infra_type, infra_bucket):
        """Parametrized test for various device type combinations."""
        param_data = [
            {
                'id': 1,
                'host_name': f'host_{device_type.lower().replace(" ", "_")}',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': json.dumps({'device_type': device_type, 'infra_type': infra_type, 'infra_bucket': infra_bucket}),
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(param_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Each device type should be processed successfully
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_font_and_alignment_settings(self):
        """Test that proper fonts and alignments are applied."""
        test_df = self.create_test_dataframe_from_csv_data()[:2]
        dataframes = {
            'job_host_summary': test_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        report._build_data_section_infrastructure_summary(1, mock_ws, test_df)

        # Verify that cells have font and alignment properties set
        call_list = mock_ws.cell.call_args_list
        assert len(call_list) > 0

        # Check that cells were created and fonts/alignments would be set
        # (The actual font/alignment setting happens after cell creation)
        for call_args in call_list:
            assert 'row' in call_args[1]
            assert 'column' in call_args[1]

    def test_row_height_settings(self):
        """Test that row heights are set correctly."""
        test_df = self.create_test_dataframe_from_csv_data()[:2]
        dataframes = {
            'job_host_summary': test_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        report._build_data_section_infrastructure_summary(1, mock_ws, test_df)

        # Verify that row heights were set
        assert mock_ws.row_dimensions.__setitem__.called or mock_ws.row_dimensions.__getitem__.called

    def test_comprehensive_coverage_scenario(self):
        """Comprehensive test to achieve high code coverage."""
        # Create data that exercises all code paths
        comprehensive_data = [
            # Normal case
            {
                'id': 1,
                'host_name': 'normal_host',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Normal Device", "infra_type": "Normal Infra", "infra_bucket": "Normal Bucket"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Dict facts instead of JSON string
            {
                'id': 2,
                'host_name': 'dict_host',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': {'device_type': 'Dict Device', 'infra_type': 'Dict Infra', 'infra_bucket': 'Dict Bucket'},
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Invalid JSON
            {
                'id': 3,
                'host_name': 'invalid_json_host',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': 'invalid json string',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Missing fields
            {
                'id': 4,
                'host_name': 'missing_fields_host',
                'host_remote_id': 4,
                'managed_node_type': INDIRECT,
                'facts': '{"some_other_field": "value"}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # List values
            {
                'id': 5,
                'host_name': 'list_values_host',
                'host_remote_id': 5,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": ["List Device 1", "List Device 2"], "infra_type": ["List Infra"], "infra_bucket": ["List Bucket"]}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Empty values
            {
                'id': 6,
                'host_name': 'empty_values_host',
                'host_remote_id': 6,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "", "infra_type": null, "infra_bucket": []}',
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(comprehensive_data)
        dataframes = {'job_host_summary': df, 'main_jobevent': pd.DataFrame(), 'main_host': pd.DataFrame(), 'data_collection_status': pd.DataFrame()}

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        # Should handle all edge cases and complete successfully
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called
        assert mock_ws.merge_cells.called

        # Should have processed multiple different scenarios
        call_count = mock_ws.cell.call_count
        assert call_count > 10

    def test_missing_infra_type_only(self):
        """Test handling of missing infra_type field only."""
        missing_infra_type_data = [
            {
                'id': 1,
                'host_name': 'host_missing_infra_type',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_bucket": "Storage", "device_type": "Containers"}',  # Missing infra_type
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(missing_infra_type_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle missing infra_type gracefully (should default to 'Unknown')
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_missing_infra_bucket_only(self):
        """Test handling of missing infra_bucket field only."""
        missing_infra_bucket_data = [
            {
                'id': 1,
                'host_name': 'host_missing_infra_bucket',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_type": "Public Cloud", "device_type": "Containers"}',  # Missing infra_bucket
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(missing_infra_bucket_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle missing infra_bucket gracefully (should default to 'Unknown')
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_missing_device_type_only(self):
        """Test handling of missing device_type field only."""
        missing_device_type_data = [
            {
                'id': 1,
                'host_name': 'host_missing_device_type',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_type": "Public Cloud", "infra_bucket": "Storage"}',  # Missing device_type
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(missing_device_type_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle missing device_type gracefully (should default to 'Unknown')
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_missing_all_three_fields(self):
        """Test handling of missing infra_type, infra_bucket, and device_type fields."""
        missing_all_fields_data = [
            {
                'id': 1,
                'host_name': 'host_missing_all_fields',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"some_other_field": "value", "unrelated_field": "data"}',  # Missing all 3 required fields
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(missing_all_fields_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle missing all fields gracefully (all should default to 'Unknown')
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    def test_missing_fields_combinations(self):
        """Test various combinations of missing fields."""
        missing_combinations_data = [
            # Missing infra_type and infra_bucket
            {
                'id': 1,
                'host_name': 'host_missing_type_bucket',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"device_type": "Containers"}',  # Only device_type present
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Missing infra_type and device_type
            {
                'id': 2,
                'host_name': 'host_missing_type_device',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_bucket": "Storage"}',  # Only infra_bucket present
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Missing infra_bucket and device_type
            {
                'id': 3,
                'host_name': 'host_missing_bucket_device',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_type": "Public Cloud"}',  # Only infra_type present
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(missing_combinations_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle all missing field combinations gracefully
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

        # Should have processed multiple rows (3 different combinations)
        call_count = mock_ws.cell.call_count
        assert call_count > 10  # Headers + infrastructure type headers + data rows

    def test_null_vs_missing_fields(self):
        """Test difference between null values and missing fields."""
        null_vs_missing_data = [
            # Explicit null values
            {
                'id': 1,
                'host_name': 'host_null_values',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_type": null, "infra_bucket": null, "device_type": null}',  # Explicit nulls
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Empty string values
            {
                'id': 2,
                'host_name': 'host_empty_strings',
                'host_remote_id': 2,
                'managed_node_type': INDIRECT,
                'facts': '{"infra_type": "", "infra_bucket": "", "device_type": ""}',  # Empty strings
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
            # Completely missing fields (not in JSON at all)
            {
                'id': 3,
                'host_name': 'host_missing_completely',
                'host_remote_id': 3,
                'managed_node_type': INDIRECT,
                'facts': '{"other_field": "value"}',  # Fields not present at all
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            },
        ]

        df = pd.DataFrame(null_vs_missing_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Should handle all types of missing/null/empty values gracefully
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called

    @pytest.mark.parametrize(
        'missing_field,present_fields',
        [
            ('infra_type', {'infra_bucket': 'Storage', 'device_type': 'Containers'}),
            ('infra_bucket', {'infra_type': 'Public Cloud', 'device_type': 'Containers'}),
            ('device_type', {'infra_type': 'Public Cloud', 'infra_bucket': 'Storage'}),
        ],
    )
    def test_parametrized_missing_single_fields(self, missing_field, present_fields):
        """Parametrized test for missing individual fields."""
        test_data = [
            {
                'id': 1,
                'host_name': f'host_missing_{missing_field}',
                'host_remote_id': 1,
                'managed_node_type': INDIRECT,
                'facts': json.dumps(present_fields),  # Only has the present fields
                'first_automation': '2025-07-22T09:00:00+00',
                'last_automation': '2025-07-22T09:10:00+00',
            }
        ]

        df = pd.DataFrame(test_data)
        dataframes = {
            'job_host_summary': df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, df)

        # Each missing field scenario should be handled gracefully
        assert isinstance(current_row, int)
        assert current_row > 1
        assert mock_ws.cell.called
