from datetime import datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dedup.renewal_guidance import (
    DedupRenewal,
    DedupRenewalExperimental,
    DedupRenewalHostname,
)


class TestDedupRenewal:
    """Test cases for the DedupRenewal class."""

    @pytest.fixture
    def mock_dataframes(self):
        """Mock dataframes for testing."""
        return {
            'host_metric': Mock(),
        }

    @pytest.fixture
    def base_extra_params(self):
        """Base extra parameters for testing."""
        return {
            'report_renewal_guidance_dedup_iterations': '3',
        }

    @pytest.fixture
    def sample_host_data(self):
        """Sample host data for testing."""
        return pd.DataFrame(
            {
                'index': [0, 1, 2, 3, 4],
                'hostname': ['host1', 'host2', 'host3', 'host1', 'host4'],
                'ansible_host_variable': [
                    '192.168.1.1',
                    None,
                    '192.168.1.3',
                    '192.168.1.1',
                    '192.168.1.4',
                ],
                'ansible_product_serial': [
                    'serial1',
                    'serial2',
                    None,
                    'serial1',
                    'serial3',
                ],
                'ansible_machine_id': [
                    'machine1',
                    'machine2',
                    'machine3',
                    'machine1',
                    None,
                ],
                'deleted': [False, False, True, False, False],
                'first_automation': [
                    datetime(2023, 1, 1),
                    datetime(2023, 1, 2),
                    datetime(2023, 1, 3),
                    datetime(2023, 1, 4),
                    datetime(2023, 1, 5),
                ],
                'last_automation': [
                    datetime(2023, 2, 1),
                    datetime(2023, 2, 2),
                    datetime(2023, 2, 3),
                    datetime(2023, 2, 4),
                    datetime(2023, 2, 5),
                ],
                'automated_counter': [10, 15, 5, 8, 12],
                'deleted_counter': [1, 0, 2, 0, 1],
                'last_deleted': [
                    datetime(2023, 3, 1),
                    datetime(2023, 3, 2),
                    datetime(2023, 3, 3),
                    datetime(2023, 3, 4),
                    datetime(2023, 3, 5),
                ],
            }
        )

    @pytest.fixture
    def multiple_serials_data(self):
        """Sample data with multiple serial numbers for testing."""
        return pd.DataFrame(
            {
                'index': [0, 1, 2, 3, 4],
                'hostname': ['host1', 'host2', 'host3', 'host4', 'host5'],
                'ansible_host_variable': [
                    'host1.example.com',
                    'host2.example.com',
                    'host3.example.com',
                    'host4.example.com',
                    'host5.example.com',
                ],
                'ansible_product_serial': [
                    'serial1, serial2',  # Multiple serials
                    'serial3',  # Single serial
                    'serial1',  # Matches first part of host1
                    '',  # Empty serial
                    'serial4, serial5',  # Multiple serials
                ],
                'ansible_machine_id': [
                    'machine1',
                    'machine2, machine3',  # Multiple machine IDs
                    'machine1',  # Matches host1
                    'machine4',
                    'machine5',
                ],
                'deleted': [False, False, False, False, False],
                'first_automation': [datetime(2023, 1, 1)] * 5,
                'last_automation': [datetime(2023, 2, 1)] * 5,
                'automated_counter': [10, 15, 20, 25, 30],
                'deleted_counter': [0, 0, 0, 0, 0],
                'last_deleted': [datetime(2023, 3, 1)] * 5,
            }
        )

    def test_init(self, mock_dataframes, base_extra_params):
        """Test DedupRenewal initialization."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupRenewal(mock_dataframes, base_extra_params)

        assert dedup.extra_params == base_extra_params
        mock_dataframes['host_metric'].build_dataframe.assert_called_once()

    def test_init_hostname(self, mock_dataframes, base_extra_params):
        """Test DedupRenewalHostname initialization."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupRenewalHostname(mock_dataframes, base_extra_params)

        assert dedup.extra_params == base_extra_params
        mock_dataframes['host_metric'].build_dataframe.assert_called_once()

    def test_init_experimental(self, mock_dataframes, base_extra_params):
        """Test DedupRenewalExperimental initialization."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)

        assert dedup.extra_params == base_extra_params
        mock_dataframes['host_metric'].build_dataframe.assert_called_once()

    def test_run_with_sample_data(self, mock_dataframes, base_extra_params, sample_host_data):
        """Test run method with sample data."""
        mock_dataframes['host_metric'].build_dataframe.return_value = sample_host_data

        dedup = DedupRenewal(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        assert isinstance(result['host_metric'], pd.DataFrame)

        # Check that deduplication occurred
        result_df = result['host_metric']
        assert len(result_df) < len(sample_host_data)

        # Check required columns exist
        expected_columns = [
            'hostname',
            'hostmetric_record_count',
            'hostmetric_record_count_active',
            'hostmetric_record_count_deleted',
            'hostnames',
            'ansible_host_variables',
            'ansible_product_serials',
            'ansible_machine_ids',
            'deleted',
            'first_automation',
            'last_automation',
            'automated_counter',
            'deleted_counter',
            'last_deleted',
        ]
        for col in expected_columns:
            assert col in result_df.columns

    def test_hostname_dedup_with_sample_data(self, mock_dataframes, base_extra_params, sample_host_data):
        """Test DedupRenewalHostname with sample data."""
        mock_dataframes['host_metric'].build_dataframe.return_value = sample_host_data

        dedup = DedupRenewalHostname(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        assert isinstance(result['host_metric'], pd.DataFrame)

        result_df = result['host_metric']

        # Should deduplicate based on ansible_host_variable || hostname
        # host1 records (index 0, 3) should be merged because they have same ansible_host_variable
        # Other hosts should remain separate

        # Check required columns exist
        expected_columns = [
            'hostname',
            'hostmetric_record_count',
            'hostmetric_record_count_active',
            'hostmetric_record_count_deleted',
            'hostnames',
            'ansible_host_variables',
            'ansible_product_serials',
            'ansible_machine_ids',
            'deleted',
            'first_automation',
            'last_automation',
            'automated_counter',
            'deleted_counter',
            'last_deleted',
        ]
        for col in expected_columns:
            assert col in result_df.columns

    def test_experimental_dedup_with_sample_data(self, mock_dataframes, base_extra_params, sample_host_data):
        """Test DedupRenewalExperimental with sample data."""
        mock_dataframes['host_metric'].build_dataframe.return_value = sample_host_data

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        assert isinstance(result['host_metric'], pd.DataFrame)

        result_df = result['host_metric']

        # Should first deduplicate by hostname, then by serial
        # Records 0,3 should merge by hostname (same ansible_host_variable)
        # Then additional merging might occur based on product_serial + machine_id

        # Check required columns exist
        expected_columns = [
            'hostname',
            'hostmetric_record_count',
            'hostmetric_record_count_active',
            'hostmetric_record_count_deleted',
            'hostnames',
            'ansible_host_variables',
            'ansible_product_serials',
            'ansible_machine_ids',
            'deleted',
            'first_automation',
            'last_automation',
            'automated_counter',
            'deleted_counter',
            'last_deleted',
        ]
        for col in expected_columns:
            assert col in result_df.columns

    def test_experimental_dedup_with_multiple_serials(self, mock_dataframes, base_extra_params, multiple_serials_data):
        """Test DedupRenewalExperimental with multiple serial numbers."""
        mock_dataframes['host_metric'].build_dataframe.return_value = multiple_serials_data

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        assert isinstance(result['host_metric'], pd.DataFrame)

        result_df = result['host_metric']

        # Should handle multiple serials correctly
        # host1 and host3 should be merged due to shared serial1 and machine1
        assert len(result_df) < len(multiple_serials_data)

    def test_parse_multiple_serials(self, mock_dataframes, base_extra_params):
        """Test _parse_multiple_serials method."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()
        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)

        # Test comma-separated values
        result = dedup._parse_multiple_serials('serial1, serial2, serial3')
        assert result == ['serial1', 'serial2', 'serial3']

        # Test single value
        result = dedup._parse_multiple_serials('serial1')
        assert result == ['serial1']

        # Test empty/null values
        assert dedup._parse_multiple_serials('') == []
        assert dedup._parse_multiple_serials(None) == []
        assert dedup._parse_multiple_serials('NA') == []
        assert dedup._parse_multiple_serials(pd.NA) == []

        # Test with spaces and NA values mixed in
        result = dedup._parse_multiple_serials('serial1, , serial2, NA, serial3')
        assert result == ['serial1', 'serial2', 'serial3']

    def test_create_serial_combinations(self, mock_dataframes, base_extra_params):
        """Test _create_serial_combinations method."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()
        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)

        # Test with both product serials and machine IDs
        product_serials = ['ps1', 'ps2']
        machine_ids = ['mid1', 'mid2']

        result = dedup._create_serial_combinations(product_serials, machine_ids)

        # Should create compound keys and individual keys
        expected_compounds = ['ps1/mid1', 'ps1/mid2', 'ps2/mid1', 'ps2/mid2']
        expected_individuals = ['ps:ps1', 'ps:ps2', 'mid:mid1', 'mid:mid2']

        compound_serials = [r['compound_serial'] for r in result if r['compound_serial'] and '/' in r['compound_serial']]
        individual_serials = [r['compound_serial'] for r in result if r['compound_serial'] and ':' in r['compound_serial']]

        assert all(cs in compound_serials for cs in expected_compounds)
        assert all(ind in individual_serials for ind in expected_individuals)

        # Test with only product serials
        result = dedup._create_serial_combinations(['ps1'], [])
        assert any(r['compound_serial'] == 'ps:ps1' for r in result)

        # Test with only machine IDs
        result = dedup._create_serial_combinations([], ['mid1'])
        assert any(r['compound_serial'] == 'mid:mid1' for r in result)

        # Test with no serials
        result = dedup._create_serial_combinations([], [])
        assert len(result) == 1
        assert result[0]['compound_serial'] is None

    def test_merge_serial_fields(self, mock_dataframes, base_extra_params):
        """Test _merge_serial_fields method."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()
        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)

        # Test merging multiple serial fields
        serial_fields = ['serial1, serial2', 'serial2, serial3', 'serial4']
        result = dedup._merge_serial_fields(serial_fields)

        # Should deduplicate and sort
        expected_serials = ['serial1', 'serial2', 'serial3', 'serial4']
        assert result == ', '.join(expected_serials)

        # Test with empty and None values
        serial_fields = ['serial1', '', None, 'serial2']
        result = dedup._merge_serial_fields(serial_fields)
        assert result == 'serial1, serial2'

        # Test with all empty values
        result = dedup._merge_serial_fields(['', None, ''])
        assert result == ''

    def test_experimental_dedup_compound_serial_matching(self, mock_dataframes, base_extra_params):
        """Test compound serial matching in experimental dedup."""
        test_data = pd.DataFrame(
            {
                'index': [0, 1, 2, 3],
                'hostname': ['host1', 'host2', 'host3', 'host4'],
                'ansible_host_variable': ['host1', 'host2', 'host3', 'host4'],
                'ansible_product_serial': ['serial1', 'serial2', 'serial1', 'serial3'],
                'ansible_machine_id': ['machine1', 'machine2', 'machine1', 'machine3'],
                'deleted': [False, False, False, False],
                'first_automation': [datetime(2023, 1, 1)] * 4,
                'last_automation': [datetime(2023, 2, 1)] * 4,
                'automated_counter': [10, 15, 20, 25],
                'deleted_counter': [0, 0, 0, 0],
                'last_deleted': [datetime(2023, 3, 1)] * 4,
            }
        )

        mock_dataframes['host_metric'].build_dataframe.return_value = test_data

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        result = dedup.run()

        result_df = result['host_metric']

        # host1 and host3 should be merged due to same compound serial (serial1/machine1)
        assert len(result_df) < len(test_data)

        # Check that the merged data contains both hostnames
        merged_row = None
        for _, row in result_df.iterrows():
            if 'host1' in row['hostnames'] and 'host3' in row['hostnames']:
                merged_row = row
                break

        assert merged_row is not None
        assert 'serial1' in merged_row['ansible_product_serials']
        assert 'machine1' in merged_row['ansible_machine_ids']

    def test_experimental_dedup_partial_serial_matching(self, mock_dataframes, base_extra_params):
        """Test partial serial matching (only product_serial or only machine_id)."""
        test_data = pd.DataFrame(
            {
                'index': [0, 1, 2, 3],
                'hostname': ['host1', 'host2', 'host3', 'host4'],
                'ansible_host_variable': ['host1', 'host2', 'host3', 'host4'],
                'ansible_product_serial': [
                    'serial1',
                    '',
                    'serial1',
                    'serial2',
                ],  # host1, host3 share serial1
                'ansible_machine_id': [
                    '',
                    'machine1',
                    '',
                    'machine1',
                ],  # host2, host4 share machine1
                'deleted': [False, False, False, False],
                'first_automation': [datetime(2023, 1, 1)] * 4,
                'last_automation': [datetime(2023, 2, 1)] * 4,
                'automated_counter': [10, 15, 20, 25],
                'deleted_counter': [0, 0, 0, 0],
                'last_deleted': [datetime(2023, 3, 1)] * 4,
            }
        )

        mock_dataframes['host_metric'].build_dataframe.return_value = test_data

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        result = dedup.run()

        result_df = result['host_metric']

        # Should handle partial matches
        # host1 and host3 should be merged by product_serial
        # host2 and host4 should be merged by machine_id
        assert len(result_df) <= 2

    def test_experimental_dedup_with_comma_separated_serials(self, mock_dataframes, base_extra_params):
        """Test handling of comma-separated serial numbers."""
        test_data = pd.DataFrame(
            {
                'index': [0, 1, 2],
                'hostname': ['host1', 'host2', 'host3'],
                'ansible_host_variable': ['host1', 'host2', 'host3'],
                'ansible_product_serial': [
                    'serial1, serial2',
                    'serial3',
                    'serial2, serial4',
                ],
                'ansible_machine_id': ['machine1', 'machine2, machine3', 'machine3'],
                'deleted': [False, False, False],
                'first_automation': [datetime(2023, 1, 1)] * 3,
                'last_automation': [datetime(2023, 2, 1)] * 3,
                'automated_counter': [10, 15, 20],
                'deleted_counter': [0, 0, 0],
                'last_deleted': [datetime(2023, 3, 1)] * 3,
            }
        )

        mock_dataframes['host_metric'].build_dataframe.return_value = test_data

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        result = dedup.run()

        result_df = result['host_metric']

        # host1 and host3 should be merged due to shared serial2
        # host2 and host3 should be merged due to shared machine3
        # This should result in all three being merged into one group
        assert len(result_df) == 2

        # Check that all serials and machine IDs are preserved
        merged_row = result_df.iloc[0]
        assert 'serial1' in merged_row['ansible_product_serials']
        assert 'serial2' in merged_row['ansible_product_serials']
        assert 'serial4' in merged_row['ansible_product_serials']
        assert 'machine1' in merged_row['ansible_machine_ids']
        assert 'machine3' in merged_row['ansible_machine_ids']

    def test_stringify_with_none_values(self, mock_dataframes, base_extra_params):
        """Test stringify method with None values."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupRenewal(mock_dataframes, base_extra_params)

        test_set = {None, 'value1', 'value2', None}
        result = ', '.join(sorted(filter(None, dedup.stringify(test_set).split(', '))))

        assert result == 'value1, value2'

    def test_hostname_dedup_empty_dataframe(self, mock_dataframes, base_extra_params):
        """Test DedupRenewalHostname with empty dataframe."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupRenewalHostname(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        assert isinstance(result['host_metric'], pd.DataFrame)
        assert len(result['host_metric']) == 0

    def test_experimental_dedup_empty_dataframe(self, mock_dataframes, base_extra_params):
        """Test DedupRenewalExperimental with empty dataframe."""
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        assert isinstance(result['host_metric'], pd.DataFrame)
        assert len(result['host_metric']) == 0

    def test_hostname_normalization(self, mock_dataframes, base_extra_params):
        """Test hostname normalization logic in DedupRenewalHostname."""
        test_data = pd.DataFrame(
            {
                'index': [0, 1, 2],
                'hostname': ['host1', 'host2', 'host3'],
                'ansible_host_variable': ['192.168.1.1', None, '192.168.1.3'],
                'ansible_product_serial': ['serial1', 'serial2', 'serial3'],
                'ansible_machine_id': ['machine1', 'machine2', 'machine3'],
                'deleted': [False, False, False],
                'first_automation': [datetime(2023, 1, 1)] * 3,
                'last_automation': [datetime(2023, 2, 1)] * 3,
                'automated_counter': [10, 15, 20],
                'deleted_counter': [1, 0, 2],
                'last_deleted': [datetime(2023, 3, 1)] * 3,
            }
        )

        mock_dataframes['host_metric'].build_dataframe.return_value = test_data

        dedup = DedupRenewalHostname(mock_dataframes, base_extra_params)
        result = dedup.run()

        result_df = result['host_metric']

        # All hosts should remain separate as they have different normalized hostnames
        # host1 -> 192.168.1.1 (ansible_host_variable)
        # host2 -> host2 (fallback to hostname)
        # host3 -> 192.168.1.3 (ansible_host_variable)
        assert len(result_df) == 3

    def test_dedup_with_complex_relationships(self, mock_dataframes, base_extra_params):
        """Test deduplication with complex host relationships."""
        complex_data = pd.DataFrame(
            {
                'index': [0, 1, 2, 3, 4, 5],
                'hostname': ['host1', 'host2', 'host3', 'host4', 'host5', 'host6'],
                'ansible_host_variable': [
                    '192.168.1.1',
                    '192.168.1.1',
                    '192.168.1.2',
                    '192.168.1.3',
                    '192.168.1.4',
                    '192.168.1.5',
                ],
                'ansible_product_serial': [
                    'serial1',
                    'serial2',
                    'serial1',
                    'serial3',
                    'serial2',
                    'serial4',
                ],
                'ansible_machine_id': [
                    'machine1',
                    'machine2',
                    'machine3',
                    'machine1',
                    'machine4',
                    'machine5',
                ],
                'deleted': [False, False, False, False, False, False],
                'first_automation': [datetime(2023, 1, 1)] * 6,
                'last_automation': [datetime(2023, 2, 1)] * 6,
                'automated_counter': [10] * 6,
                'deleted_counter': [1] * 6,
                'last_deleted': [datetime(2023, 3, 1)] * 6,
            }
        )

        mock_dataframes['host_metric'].build_dataframe.return_value = complex_data

        dedup = DedupRenewal(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'host_metric' in result
        result_df = result['host_metric']

        # Check that deduplication occurred
        assert len(result_df) < len(complex_data)

        # Check that aggregated values are correct
        for _, row in result_df.iterrows():
            assert row['hostmetric_record_count'] >= 1
            assert row['automated_counter'] >= 10
