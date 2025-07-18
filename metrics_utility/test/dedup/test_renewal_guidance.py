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
