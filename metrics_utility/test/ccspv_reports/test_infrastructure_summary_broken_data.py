import json

from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.report.report_ccsp_v2 import ReportCCSPv2
from metrics_utility.metric_utils import INDIRECT


class TestInfrastructureSummaryBrokenData:
    """Test infrastructure summary tab with broken/edge-case data scenarios."""

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

    def create_broken_dataframe(self, scenario):
        """Create dataframes with various broken data scenarios."""
        base_columns = [
            'id',
            'created',
            'modified',
            'host_name',
            'host_remote_id',
            'managed_node_type',
            'facts',
            'first_automation',
            'last_automation',
        ]

        if scenario == 'null_device_type':
            # Test null/empty device type data
            data = [
                {
                    'id': 1,
                    'created': '2025-07-09 18:00:00+00:00',
                    'modified': '2025-07-09 18:00:05+00:00',
                    'host_name': 'test-host-1',
                    'host_remote_id': 101,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps(
                        {
                            'infra_type': 'kubernetes',
                            'infra_bucket': 'container',
                            'device_type': None,  # Null device type
                        }
                    ),
                    'first_automation': '2025-07-09 18:00:00+00:00',
                    'last_automation': '2025-07-09 18:00:05+00:00',
                },
                {
                    'id': 2,
                    'created': '2025-07-09 18:05:00+00:00',
                    'modified': '2025-07-09 18:05:05+00:00',
                    'host_name': 'test-host-2',
                    'host_remote_id': 102,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps(
                        {
                            'infra_type': 'vmware',
                            'infra_bucket': 'virtual',
                            'device_type': '',  # Empty device type
                        }
                    ),
                    'first_automation': '2025-07-09 18:05:00+00:00',
                    'last_automation': '2025-07-09 18:05:05+00:00',
                },
            ]

        elif scenario == 'malformed_facts':
            # Test malformed JSON and corrupted data
            data = [
                {
                    'id': 1,
                    'created': '2025-07-09 18:00:00+00:00',
                    'modified': '2025-07-09 18:00:05+00:00',
                    'host_name': 'malformed-host-1',
                    'host_remote_id': 201,
                    'managed_node_type': INDIRECT,
                    'facts': '{"infra_type": "kubernetes", "incomplete": }',  # Malformed JSON
                    'first_automation': '2025-07-09 18:00:00+00:00',
                    'last_automation': '2025-07-09 18:00:05+00:00',
                },
                {
                    'id': 2,
                    'created': '2025-07-09 18:05:00+00:00',
                    'modified': '2025-07-09 18:05:05+00:00',
                    'host_name': 'corrupted-host-2',
                    'host_remote_id': 202,
                    'managed_node_type': INDIRECT,
                    'facts': 'not_json_at_all',  # Not JSON
                    'first_automation': '2025-07-09 18:05:00+00:00',
                    'last_automation': '2025-07-09 18:05:05+00:00',
                },
            ]

        elif scenario == 'timezone_edge_cases':
            # Test timezone edge cases
            data = [
                {
                    'id': 1,
                    'created': '2025-07-09 23:59:59+00:00',  # End of day UTC
                    'modified': '2025-07-10 00:00:01+00:00',  # Next day UTC
                    'host_name': 'timezone-host-1',
                    'host_remote_id': 301,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps({'infra_type': 'aws', 'infra_bucket': 'cloud', 'device_type': 'ec2'}),
                    'first_automation': '2025-07-09 23:59:59+00:00',
                    'last_automation': '2025-07-10 00:00:01+00:00',
                },
                {
                    'id': 2,
                    'created': '2025-07-09 18:00:00-05:00',  # Different timezone
                    'modified': '2025-07-09 18:05:00-05:00',
                    'host_name': 'timezone-host-2',
                    'host_remote_id': 302,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps({'infra_type': 'azure', 'infra_bucket': 'cloud', 'device_type': 'vm'}),
                    'first_automation': '2025-07-09 18:00:00-05:00',
                    'last_automation': '2025-07-09 18:05:00-05:00',
                },
            ]

        elif scenario == 'uuid_collisions':
            # Test UUID collision scenarios (same host_name, different remote_id)
            data = [
                {
                    'id': 1,
                    'created': '2025-07-09 18:00:00+00:00',
                    'modified': '2025-07-09 18:00:05+00:00',
                    'host_name': 'duplicate-host',
                    'host_remote_id': 401,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps({'infra_type': 'kubernetes', 'infra_bucket': 'container', 'device_type': 'pod'}),
                    'first_automation': '2025-07-09 18:00:00+00:00',
                    'last_automation': '2025-07-09 18:00:05+00:00',
                },
                {
                    'id': 2,
                    'created': '2025-07-09 18:05:00+00:00',
                    'modified': '2025-07-09 18:05:05+00:00',
                    'host_name': 'duplicate-host',  # Same host name
                    'host_remote_id': 402,  # Different remote ID
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps({'infra_type': 'vmware', 'infra_bucket': 'virtual', 'device_type': 'vm'}),
                    'first_automation': '2025-07-09 18:05:00+00:00',
                    'last_automation': '2025-07-09 18:05:05+00:00',
                },
            ]

        elif scenario == 'device_type_edge_cases':
            # Test device type mapping edge cases
            data = [
                {
                    'id': 1,
                    'created': '2025-07-09 18:00:00+00:00',
                    'modified': '2025-07-09 18:00:05+00:00',
                    'host_name': 'edge-case-host-1',
                    'host_remote_id': 501,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps(
                        {
                            'infra_type': 'unknown_infra',
                            'infra_bucket': ['multiple', 'buckets'],  # List instead of string
                            'device_type': {'nested': 'object'},  # Object instead of string
                        }
                    ),
                    'first_automation': '2025-07-09 18:00:00+00:00',
                    'last_automation': '2025-07-09 18:00:05+00:00',
                },
                {
                    'id': 2,
                    'created': '2025-07-09 18:05:00+00:00',
                    'modified': '2025-07-09 18:05:05+00:00',
                    'host_name': 'edge-case-host-2',
                    'host_remote_id': 502,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps(
                        {
                            'infra_type': ['set_type'],  # List instead of set to avoid JSON serialization issues
                            'infra_bucket': None,
                            'device_type': 'very_long_device_type_name_that_exceeds_normal_limits_and_could_cause_formatting_issues',
                        }
                    ),
                    'first_automation': '2025-07-09 18:05:00+00:00',
                    'last_automation': '2025-07-09 18:05:05+00:00',
                },
            ]

        elif scenario == 'empty_dataframe':
            # Test completely empty dataframe
            data = []

        elif scenario == 'same_infra_deduplication':
            # Test deduplication within same infrastructure type
            data = [
                {
                    'id': 1,
                    'created': '2025-07-09 18:00:00+00:00',
                    'modified': '2025-07-09 18:00:05+00:00',
                    'host_name': 'duplicate-same-infra',
                    'host_remote_id': 601,
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps({'infra_type': 'kubernetes', 'infra_bucket': 'container', 'device_type': 'pod'}),
                    'first_automation': '2025-07-09 18:00:00+00:00',
                    'last_automation': '2025-07-09 18:00:05+00:00',
                },
                {
                    'id': 2,
                    'created': '2025-07-09 18:05:00+00:00',
                    'modified': '2025-07-09 18:05:05+00:00',
                    'host_name': 'duplicate-same-infra',  # Same host name
                    'host_remote_id': 602,  # Different remote ID
                    'managed_node_type': INDIRECT,
                    'facts': json.dumps({'infra_type': 'kubernetes', 'infra_bucket': 'container', 'device_type': 'pod'}),  # Same infra type
                    'first_automation': '2025-07-09 18:05:00+00:00',
                    'last_automation': '2025-07-09 18:05:05+00:00',
                },
            ]

        else:
            raise ValueError(f'Unknown scenario: {scenario}')

        return pd.DataFrame(data, columns=base_columns)

    def test_null_empty_device_type_data(self):
        """Test handling of null/empty device type data."""
        broken_df = self.create_broken_dataframe('null_device_type')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)

        # Mock worksheet for testing
        mock_ws = self.create_mock_worksheet()

        # Test that the method handles null/empty device types gracefully
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        # Verify that cells were written (indicating the method didn't crash)
        assert mock_ws.cell.called

        # Verify headers are present
        expected_headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']
        for i, header in enumerate(expected_headers, 1):
            assert self.get_cell_value(1, i) == header

        # Verify that null/empty device types are handled as 'Unknown'
        unknown_row = self.find_data_row_with_device_type('Unknown')
        assert unknown_row is not None, 'Unknown device type should be found in the table for null/empty values'

        # Since we have 2 hosts with null/empty device types, they should be counted
        # (Host 1: kubernetes+container+null, Host 2: vmware+virtual+empty)
        # These should result in 2 separate entries since they have different infra types

        # Find all rows with 'Unknown' device type
        unknown_rows = []
        for (row, col), cell_mock in self.cell_writes.items():
            if col == 3 and hasattr(cell_mock, 'value') and cell_mock.value == 'Unknown':
                row_data = self.find_data_row_with_device_type('Unknown')
                if row_data and row_data['row'] == row:  # Ensure we get the right row
                    row_data_full = {
                        'row': row,
                        'device_type': 'Unknown',
                        'unique_nodes': self.get_cell_value(row, 4),
                        'total_nodes': self.get_cell_value(row, 5),
                        'infra_bucket': self.get_cell_value(row, 2),
                    }
                    unknown_rows.append(row_data_full)

        # We should have entries for both kubernetes/container/Unknown and vmware/virtual/Unknown
        assert len(unknown_rows) >= 1, f'Should have at least 1 Unknown device type entry, got {len(unknown_rows)}'

        # Verify that each Unknown entry has reasonable counts
        for unknown_row_data in unknown_rows:
            unique_nodes = unknown_row_data['unique_nodes']
            total_nodes = unknown_row_data['total_nodes']
            assert unique_nodes >= 1, f'Unknown entries should have at least 1 unique node, got {unique_nodes}'
            assert total_nodes >= 1, f'Unknown entries should have at least 1 total node, got {total_nodes}'

    def test_malformed_corrupted_input_data(self):
        """Test malformed or corrupted input data."""
        broken_df = self.create_broken_dataframe('malformed_facts')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)

        mock_ws = self.create_mock_worksheet()

        # Test that malformed JSON is handled gracefully
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        # Should not crash on malformed JSON
        assert mock_ws.cell.called

    def test_timezone_edge_cases_timestamps(self):
        """Test timezone edge cases for first/last seen timestamps."""
        broken_df = self.create_broken_dataframe('timezone_edge_cases')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)

        mock_ws = self.create_mock_worksheet()

        # Test timezone handling
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        assert mock_ws.cell.called

    def test_uuid_collision_scenarios(self):
        """Test UUID collision scenarios."""
        broken_df = self.create_broken_dataframe('uuid_collisions')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)

        mock_ws = self.create_mock_worksheet()

        # Test handling of duplicate host names with different remote IDs
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        assert mock_ws.cell.called

        # Verify headers are written correctly
        expected_headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']
        for i, header in enumerate(expected_headers, 1):
            assert self.get_cell_value(1, i) == header

        # Verify specific device types and their counts in the table structure
        pod_row = self.find_data_row_with_device_type('pod')
        vm_row = self.find_data_row_with_device_type('vm')

        assert pod_row is not None, 'pod device type should be found in the table'
        assert vm_row is not None, 'vm device type should be found in the table'

        # Verify the pod row has correct bucket and counts
        assert pod_row['infra_bucket'] == 'container', f'pod should be in container bucket, got {pod_row["infra_bucket"]}'
        assert pod_row['unique_nodes'] == 1, f'pod should have 1 unique node, got {pod_row["unique_nodes"]}'
        assert pod_row['total_nodes'] == 1, f'pod should have 1 total node, got {pod_row["total_nodes"]}'

        # Verify the vm row has correct bucket and counts
        assert vm_row['infra_bucket'] == 'virtual', f'vm should be in virtual bucket, got {vm_row["infra_bucket"]}'
        assert vm_row['unique_nodes'] == 1, f'vm should have 1 unique node, got {vm_row["unique_nodes"]}'
        assert vm_row['total_nodes'] == 1, f'vm should have 1 total node, got {vm_row["total_nodes"]}'

        # Key business logic test: Same hostname with DIFFERENT infra types
        # should appear in BOTH categories with 1 unique node each
        # This tests that hosts are properly categorized by infrastructure type

    def test_same_infrastructure_deduplication(self):
        """Test deduplication within the same infrastructure category."""
        broken_df = self.create_broken_dataframe('same_infra_deduplication')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        assert mock_ws.cell.called

        # Verify headers
        expected_headers = ['Infrastructure', 'Device Category', 'Device Type', 'Unique Nodes', 'Total Nodes']
        for i, header in enumerate(expected_headers, 1):
            assert self.get_cell_value(1, i) == header

        # Find the pod row (should be only one since both hosts have same infra type)
        pod_row = self.find_data_row_with_device_type('pod')
        assert pod_row is not None, 'pod device type should be found in the table'

        # Key business logic test: Same hostname with SAME infra type
        # should be deduplicated - 1 unique node, 2 total nodes
        assert pod_row['infra_bucket'] == 'container', f'pod should be in container bucket, got {pod_row["infra_bucket"]}'
        assert pod_row['unique_nodes'] == 1, f'pod should have 1 unique node (deduplicated), got {pod_row["unique_nodes"]}'
        assert pod_row['total_nodes'] == 2, f'pod should have 2 total nodes (both entries counted), got {pod_row["total_nodes"]}'

    def test_device_type_mapping_edge_cases(self):
        """Test device type mapping edge cases."""
        broken_df = self.create_broken_dataframe('device_type_edge_cases')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)

        mock_ws = self.create_mock_worksheet()

        # Test handling of non-string device types and unusual data structures
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        assert mock_ws.cell.called

    def test_empty_dataframe_handling(self):
        """Test handling of completely empty dataframe."""
        broken_df = self.create_broken_dataframe('empty_dataframe')
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)

        mock_ws = self.create_mock_worksheet()

        # Test empty dataframe handling
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        # Should show "No indirect nodes found" message
        assert mock_ws.cell.called

        # Verify the specific message is written to cell (1,1)
        assert self.get_cell_value(1, 1) == 'No indirect nodes found'

        # Should return row 2 to indicate next available row
        assert current_row == 2

    def test_error_handling_comprehensive(self):
        """Test comprehensive error handling across all scenarios."""
        scenarios = ['null_device_type', 'malformed_facts', 'timezone_edge_cases', 'uuid_collisions', 'device_type_edge_cases']

        for scenario in scenarios:
            # This should NOT raise an exception - if it does, the error handling is insufficient
            broken_df = self.create_broken_dataframe(scenario)
            dataframes = {
                'job_host_summary': broken_df,
                'main_jobevent': pd.DataFrame(),
                'main_host': pd.DataFrame(),
                'data_collection_status': pd.DataFrame(),
            }

            report = ReportCCSPv2(dataframes, self.extra_params)
            mock_ws = self.create_mock_worksheet()

            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            # If we reach here without exception, the error handling is working
            assert current_row is not None
            assert isinstance(current_row, int)

    def test_user_friendly_error_messages(self):
        """Validate error messages are user-friendly."""
        # Test with empty data - should show user-friendly message
        empty_df = self.create_broken_dataframe('empty_dataframe')
        dataframes = {
            'job_host_summary': empty_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, empty_df)

        # Verify user-friendly message is set using our tracking system
        assert self.get_cell_value(1, 1) == 'No indirect nodes found'
        assert current_row == 2  # Should advance to next row

    @pytest.mark.parametrize('scenario', ['null_device_type', 'malformed_facts', 'timezone_edge_cases', 'uuid_collisions', 'device_type_edge_cases'])
    def test_all_broken_data_scenarios_parametrized(self, scenario):
        """Parametrized test for all broken data scenarios."""
        broken_df = self.create_broken_dataframe(scenario)
        dataframes = {
            'job_host_summary': broken_df,
            'main_jobevent': pd.DataFrame(),
            'main_host': pd.DataFrame(),
            'data_collection_status': pd.DataFrame(),
        }

        report = ReportCCSPv2(dataframes, self.extra_params)
        mock_ws = self.create_mock_worksheet()

        # All scenarios should complete without raising exceptions
        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
        assert current_row is not None
        assert isinstance(current_row, int)
        assert current_row > 0
