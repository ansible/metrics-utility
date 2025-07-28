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
        mock_ws.cell.return_value = Mock()

        # Mock row_dimensions to support subscripting
        mock_ws.row_dimensions = MagicMock()
        mock_ws.row_dimensions.__getitem__.return_value = Mock()
        mock_ws.row_dimensions.__setitem__.return_value = None

        # Mock column_dimensions
        mock_ws.column_dimensions = MagicMock()
        mock_ws.column_dimensions.__getitem__.return_value = Mock()
        mock_ws.column_dimensions.__setitem__.return_value = None

        return mock_ws

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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            # Verify that cells were written (indicating the method didn't crash)
            assert mock_ws.cell.called
        except Exception as e:
            pytest.fail(f'Method failed with null/empty device types: {str(e)}')

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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            # Should not crash on malformed JSON
            assert mock_ws.cell.called
        except Exception as e:
            pytest.fail(f'Method failed with malformed data: {str(e)}')

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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            assert mock_ws.cell.called
        except Exception as e:
            pytest.fail(f'Method failed with timezone edge cases: {str(e)}')

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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            assert mock_ws.cell.called

            # Verify that both records are processed despite having the same host name
            call_count = mock_ws.cell.call_count
            assert call_count > 0
        except Exception as e:
            pytest.fail(f'Method failed with UUID collisions: {str(e)}')

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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            assert mock_ws.cell.called
        except Exception as e:
            pytest.fail(f'Method failed with device type edge cases: {str(e)}')

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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            # Should show "No indirect nodes found" message
            assert mock_ws.cell.called
            mock_ws.cell.assert_called()
            call_args = mock_ws.cell.call_args_list[0]
            assert call_args[1]['row'] == 1
            assert call_args[1]['column'] == 1
        except Exception as e:
            pytest.fail(f'Method failed with empty dataframe: {str(e)}')

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

            try:
                current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
                # If we reach here without exception, the error handling is working
                assert current_row is not None
                assert isinstance(current_row, int)
            except Exception as e:
                pytest.fail(f'Error handling insufficient for scenario {scenario}: {str(e)}')

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
        mock_cell = Mock()
        mock_ws.cell.return_value = mock_cell

        current_row = report._build_data_section_infrastructure_summary(1, mock_ws, empty_df)

        # Verify user-friendly message is set
        assert mock_cell.value == 'No indirect nodes found'
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
        try:
            current_row = report._build_data_section_infrastructure_summary(1, mock_ws, broken_df)
            assert current_row is not None
            assert isinstance(current_row, int)
            assert current_row > 0
        except Exception as e:
            pytest.fail(f"Scenario '{scenario}' failed: {str(e)}")
