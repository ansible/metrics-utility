from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dedup.renewal_guidance import (
    BaseDedupRenewal,
    DedupRenewal,
    DedupRenewalExperimental,
    DedupRenewalHostname,
)
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance


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

    def test_individual_serial_groups_ignore_blank_and_repeated_serials(self, mock_dataframes, base_extra_params):
        mock_dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()
        dedup = DedupRenewalExperimental(mock_dataframes, base_extra_params)
        expanded = pd.DataFrame(
            [
                ('first', 'shared/shared', ['shared', 'shared']),
                ('second', 'mid:shared', ['shared']),
                ('unrelated', 'ps:unique', ['', 'unique']),
                ('also-blank', None, ['']),
                ('no-serials', None, []),
            ],
            columns=['hostname_group', 'compound_serial', 'individual_serials'],
        )

        groups, processed = dedup._create_serial_groups(expanded)

        assert processed == {'first', 'second'}
        assert set(groups) == {'individual:shared'}
        assert groups['individual:shared']['canonical_group'] == 'first'
        assert list(groups['individual:shared']['groups_to_merge']) == ['first', 'second']

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

        test_set = {None, 'value1', 'value2'}
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


########################################################################################
# Differential test oracles: verbatim copies of the pre-optimisation algorithms.
# The optimised implementations must produce byte-for-byte identical results (values,
# ordering and dtypes) while no longer scanning the whole DataFrame per duplicate group.
########################################################################################


class _LegacyDedupRenewal(BaseDedupRenewal):
    """Original O(N^2) DedupRenewal implementation, used as a test oracle only."""

    def run(self):
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        self._cleanup_null_values()
        deduped_list = []
        processed_dupes_index = set()

        for index, row in self.dataframe.iterrows():
            if index in processed_dupes_index:
                continue

            dupes = self.dataframe[self.dataframe['hostname'] == row['hostname']]

            iterations = int(self.extra_params['report_renewal_guidance_dedup_iterations'])
            for _i in range(iterations):
                dupes = self.find_dupes(dupes, 'hostname', dupes['hostname'])
                dupes = self.find_dupes(dupes, 'ansible_host_variable', dupes['ansible_host_variable'])
                dupes = self.find_dupes(dupes, 'ansible_product_serial', dupes['ansible_product_serial'])
                dupes = self.find_dupes(dupes, 'ansible_machine_id', dupes['ansible_machine_id'])

            processed_dupes_index.update(dupes['index'])
            latest_hostname = self._get_latest_hostname(dupes)
            deduped_list.append(self._build_deduped_record(dupes, latest_hostname))

        return {'host_metric': pd.DataFrame(deduped_list)}

    def find_dupes(self, dupes, key, values):
        next_iteration_dupes = self.dataframe[self.dataframe[key].notnull() & self.dataframe[key].isin(values)]
        return pd.concat([dupes, next_iteration_dupes]).drop_duplicates().reset_index(drop=True)


class _LegacyDedupRenewalHostname(BaseDedupRenewal):
    """Original O(N^2) DedupRenewalHostname implementation, used as a test oracle only."""

    def run(self):
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        if 'ansible_host_variable' not in self.dataframe.columns:
            self.dataframe['ansible_host_variable'] = None

        self._cleanup_null_values()
        self.dataframe['normalized_hostname'] = self.dataframe['ansible_host_variable'].fillna(self.dataframe['hostname'])

        deduped_list = []
        processed_dupes_index = set()

        for index, row in self.dataframe.iterrows():
            if index in processed_dupes_index:
                continue

            dupes = self.dataframe[self.dataframe['normalized_hostname'] == row['normalized_hostname']]
            processed_dupes_index.update(dupes['index'])

            latest_hostname = self._get_latest_hostname(dupes)

            dupes_clean = dupes.copy()
            dupes_clean['ansible_product_serial'] = dupes_clean['ansible_product_serial'].replace('NA', None).replace('', None)
            dupes_clean['ansible_machine_id'] = dupes_clean['ansible_machine_id'].replace('NA', None).replace('', None)

            deduped_list.append(self._build_deduped_record(dupes, latest_hostname, dupes_clean))

        return {'host_metric': pd.DataFrame(deduped_list)}


class _LegacyDedupRenewalExperimental(DedupRenewalExperimental):
    """Original O(N^2) serial-dedup helpers, used as a test oracle only.

    Inherits the (unchanged) parsing/combination/merging helpers from the production class
    and re-implements exactly the methods that were rewritten.
    """

    def run(self):
        if self.dataframe is None or self.dataframe.empty:
            return {'host_metric': pd.DataFrame()}

        class MockDataframe:
            def __init__(self, dataframe):
                self.dataframe = dataframe

            def build_dataframe(self):
                return self.dataframe

        hostname_dedup = _LegacyDedupRenewalHostname({'host_metric': MockDataframe(self.dataframe)}, self.extra_params)
        hostname_df = hostname_dedup.run()['host_metric']
        return self._apply_serial_deduplication(hostname_df)

    def _expand_serial_records(self, hostname_df):
        expanded_records = []
        for _, group_row in hostname_df.iterrows():
            hostnames_in_group = [h.strip() for h in group_row['hostnames'].split(',') if h.strip()]
            original_records = self.dataframe[self.dataframe['hostname'].isin(hostnames_in_group)]
            for _, orig_row in original_records.iterrows():
                product_serials = self._parse_multiple_serials(orig_row.get('ansible_product_serial'))
                machine_ids = self._parse_multiple_serials(orig_row.get('ansible_machine_id'))

                for serial_info in self._create_serial_combinations(product_serials, machine_ids):
                    expanded_records.append(
                        {
                            'hostname': orig_row['hostname'],
                            'hostname_group': group_row['hostname'],
                            'product_serial': serial_info['product_serial'],
                            'machine_id': serial_info['machine_id'],
                            'compound_serial': serial_info['compound_serial'],
                            'individual_serials': serial_info['individual_serials'],
                            'original_data': group_row.to_dict(),
                        }
                    )
        if not expanded_records:
            return None
        return pd.DataFrame(expanded_records)

    def _group_by_compound_serial(self, expanded_df, serial_groups, processed_hostname_groups):
        compound_serials = expanded_df['compound_serial'].dropna()
        for compound_serial in compound_serials.unique():
            serial_matches = expanded_df[expanded_df['compound_serial'] == compound_serial]
            hostname_groups_in_serial = serial_matches['hostname_group'].unique()
            if len(hostname_groups_in_serial) > 1:
                serial_groups[compound_serial] = {
                    'canonical_group': hostname_groups_in_serial[0],
                    'groups_to_merge': hostname_groups_in_serial,
                    'serial_type': 'compound',
                }
                processed_hostname_groups.update(hostname_groups_in_serial)

    def _group_by_individual_serial(self, expanded_df, serial_groups, processed_hostname_groups):
        for _, row in expanded_df.iterrows():
            if row['hostname_group'] not in processed_hostname_groups:
                for serial in row['individual_serials']:
                    if serial:
                        serial_matches = expanded_df[expanded_df['individual_serials'].apply(lambda x, s=serial: s in x if x else False)]
                        hostname_groups_in_serial = serial_matches['hostname_group'].unique()
                        if len(hostname_groups_in_serial) > 1:
                            serial_key = f'individual:{serial}'
                            if serial_key not in serial_groups:
                                serial_groups[serial_key] = {
                                    'canonical_group': hostname_groups_in_serial[0],
                                    'groups_to_merge': hostname_groups_in_serial,
                                    'serial_type': 'individual',
                                }
                                processed_hostname_groups.update(hostname_groups_in_serial)

    def _build_final_deduped_list(self, hostname_df, serial_groups, processed_hostname_groups):
        final_deduped_list = []
        canonical_groups = {info['canonical_group'] for info in serial_groups.values()}
        for _, row in hostname_df.iterrows():
            hostname_group = row['hostname']
            if hostname_group in processed_hostname_groups:
                if hostname_group in canonical_groups:
                    for serial_info in serial_groups.values():
                        if hostname_group == serial_info['canonical_group']:
                            final_deduped_list.append(self._merge_hostname_groups(hostname_df, serial_info['groups_to_merge']))
                            break
            else:
                final_deduped_list.append(row.to_dict())
        return final_deduped_list

    def _merge_hostname_groups(self, hostname_df, groups_to_merge):
        groups_data = hostname_df[hostname_df['hostname'].isin(groups_to_merge)]
        if groups_data.empty:
            return {}

        latest_group = groups_data.sort_values(by=['last_automation'], ascending=[False]).iloc[0]

        return {
            'hostname': latest_group['hostname'],
            'hostmetric_record_count': (groups_data['hostmetric_record_count'].sum()),
            'hostmetric_record_count_active': (groups_data['hostmetric_record_count_active'].sum()),
            'hostmetric_record_count_deleted': (groups_data['hostmetric_record_count_deleted'].sum()),
            'hostnames': ', '.join([h for group in groups_data['hostnames'] for h in group.split(', ') if h]),
            'ansible_host_variables': ', '.join([h for group in groups_data['ansible_host_variables'] for h in group.split(', ') if h]),
            'ansible_product_serials': self._merge_serial_fields([group['ansible_product_serials'] for _, group in groups_data.iterrows()]),
            'ansible_machine_ids': self._merge_serial_fields([group['ansible_machine_ids'] for _, group in groups_data.iterrows()]),
            'deleted': groups_data['deleted'].min(),
            'first_automation': groups_data['first_automation'].min(),
            'last_automation': groups_data['last_automation'].max(),
            'automated_counter': groups_data['automated_counter'].sum(),
            'deleted_counter': groups_data['deleted_counter'].sum(),
            'last_deleted': groups_data['last_deleted'].max(),
        }


def make_host_metric_frame(rows):
    """Build a DataFrame shaped like ``DBDataframeHostMetric.build_dataframe()``.

    Each row is ``(hostname, ansible_host_variable, ansible_product_serial,
    ansible_machine_id, last_automation_day_offset, deleted)``.
    """
    base = datetime(2023, 1, 1)
    records = []
    for offset, (hostname, host_variable, product_serial, machine_id, last_day, deleted) in enumerate(rows):
        records.append(
            {
                'hostname': hostname,
                'ansible_host_variable': host_variable,
                'ansible_product_serial': product_serial,
                'ansible_machine_id': machine_id,
                'deleted': deleted,
                'first_automation': base,
                'last_automation': base + timedelta(days=last_day),
                'automated_counter': 10 + offset,
                'deleted_counter': 1 if deleted else 0,
                'last_deleted': base + timedelta(days=100 + offset),
            }
        )
    frame = pd.DataFrame(records)
    frame.insert(0, 'index', range(len(frame)))
    return frame


DEDUP_ITERATIONS = [0, 1, 2, 3, 5]

DEDUP_DATASETS = {
    # serial hops to machine id, ahv hops to the next host: needs 2 iterations to close.
    'multi_hop_chain': [
        ('host-a', None, 'serial-1', None, 1, False),
        ('host-b', None, 'serial-1', 'machine-1', 2, False),
        ('host-c', '10.0.0.1', None, 'machine-1', 3, False),
        ('host-d', '10.0.0.1', None, None, 4, False),
    ],
    'duplicate_hostnames': [
        ('host-a', '10.0.0.1', 'serial-1', 'machine-1', 1, False),
        ('host-a', '10.0.0.2', 'serial-2', 'machine-2', 2, True),
        ('host-b', '10.0.0.1', 'serial-2', 'machine-2', 3, False),
    ],
    'null_and_na_fields': [
        ('host-a', None, None, None, 1, False),
        ('host-b', '', 'NA', '', 2, False),
        ('host-c', np.nan, 'NA', 'machine-1', 3, True),
        ('host-d', None, '', 'machine-1', 4, False),
    ],
    'multiple_serials': [
        ('host-a', '10.0.0.1', 'serial-1, serial-2', 'machine-1', 1, False),
        ('host-b', '10.0.0.2', 'serial-2', 'machine-2, machine-3', 2, False),
        ('host-c', '10.0.0.3', 'serial-3', 'machine-3', 3, False),
        ('host-d', '10.0.0.4', '', '', 4, True),
    ],
    'deleted_and_active': [
        ('host-a', '10.0.0.1', 'serial-1', 'machine-1', 5, True),
        ('host-b', '10.0.0.1', 'serial-1', 'machine-1', 9, False),
        ('host-c', '10.0.0.2', 'serial-2', 'machine-2', 1, False),
    ],
    # Many distinct hostnames reporting one shared serial: a single large group that must
    # keep first-seen (ascending) order.
    'shared_serial_many_hosts': [
        (f'host-{offset:02d}', None, 'shared-serial', f'machine-{offset:02d}', offset, offset % 5 == 0) for offset in range(30)
    ],
}


def run_dedup(dedup_class, frame, iterations):
    """Run a deduplicator over a fresh copy of ``frame`` and return the output DataFrame."""
    dataframes = {'host_metric': Mock()}
    dataframes['host_metric'].build_dataframe.return_value = frame.copy()
    extra_params = {'report_renewal_guidance_dedup_iterations': str(iterations)}
    return dedup_class(dataframes, extra_params).run()['host_metric']


DEDUP_CLASS_PAIRS = [
    (DedupRenewal, _LegacyDedupRenewal),
    (DedupRenewalHostname, _LegacyDedupRenewalHostname),
    (DedupRenewalExperimental, _LegacyDedupRenewalExperimental),
]


@pytest.mark.parametrize('dedup_class, legacy_class', DEDUP_CLASS_PAIRS, ids=['renewal', 'hostname', 'experimental'])
@pytest.mark.parametrize('dataset_name', sorted(DEDUP_DATASETS))
@pytest.mark.parametrize('iterations', DEDUP_ITERATIONS)
def test_matches_legacy_algorithm(dedup_class, legacy_class, dataset_name, iterations):
    """Every dataset/iteration combination must be identical to the original implementation."""
    frame = make_host_metric_frame(DEDUP_DATASETS[dataset_name])

    expected = run_dedup(legacy_class, frame, iterations)
    actual = run_dedup(dedup_class, frame, iterations)

    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('dedup_class, legacy_class', DEDUP_CLASS_PAIRS, ids=['renewal', 'hostname', 'experimental'])
def test_empty_dataframe_matches_legacy(dedup_class, legacy_class):
    """An empty frame returns an empty frame, exactly like before."""
    frame = pd.DataFrame()

    expected = run_dedup(legacy_class, frame, 3)
    actual = run_dedup(dedup_class, frame, 3)

    assert actual.empty
    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('dedup_class, legacy_class', DEDUP_CLASS_PAIRS, ids=['renewal', 'hostname', 'experimental'])
def test_index_column_semantics_match_legacy(dedup_class, legacy_class):
    """Row skipping still keys off the DataFrame index (checked) and the 'index' column (recorded)."""
    frame = make_host_metric_frame(DEDUP_DATASETS['duplicate_hostnames'])
    frame.index = [10, 11, 12]  # labels intentionally no longer match the 'index' column

    expected = run_dedup(legacy_class, frame, 3)
    actual = run_dedup(dedup_class, frame, 3)

    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('iterations', [0, 1, 3])
@pytest.mark.parametrize('dedup_class, legacy_class', DEDUP_CLASS_PAIRS, ids=['renewal', 'hostname', 'experimental'])
def test_duplicate_index_values_match_legacy(dedup_class, legacy_class, iterations):
    """Repeated 'index' column values make exact duplicate rows possible: they are still collapsed."""
    frame = make_host_metric_frame(
        [
            ('host-a', None, 'serial-1', 'machine-1', 1, False),
            ('host-a', None, 'serial-1', 'machine-1', 1, False),
            ('host-b', '10.0.0.1', 'serial-1', 'machine-1', 2, False),
        ]
    )
    frame.loc[1] = frame.loc[0]
    frame['index'] = [0, 0, 1]

    expected = run_dedup(legacy_class, frame, iterations)
    actual = run_dedup(dedup_class, frame, iterations)

    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('iterations', [1, 3])
def test_duplicate_index_values_collapse_identical_rows(iterations):
    """`drop_duplicates()` parity: a genuinely duplicated row is still counted once."""
    frame = make_host_metric_frame(
        [
            ('host-a', None, 'serial-1', 'machine-1', 1, False),
            ('host-a', None, 'serial-1', 'machine-1', 1, False),
        ]
    )
    frame.loc[1] = frame.loc[0]

    result = run_dedup(DedupRenewal, frame, iterations)

    # The repeated 'index' value makes the second label re-emit the same group (legacy quirk),
    # but the duplicated row itself is still collapsed: counters are 10, not 20.
    assert result['hostmetric_record_count'].tolist() == [1, 1]
    assert result['automated_counter'].tolist() == [10, 10]


def test_random_frames_match_legacy():
    """Fuzz the parity contract over randomly generated frames and iteration bounds."""
    rng = np.random.default_rng(20240607)
    hostnames = ['host-a', 'host-b', 'host-c', 'host-d']
    variables = [None, '', np.nan, '10.0.0.1', '10.0.0.2']
    serials = [None, '', 'NA', np.nan, 'serial-1', 'serial-2', 'serial-1, serial-2']
    machine_ids = [None, '', 'NA', np.nan, 'machine-1', 'machine-2']

    for _ in range(15):
        rows = []
        for _row in range(int(rng.integers(1, 9))):
            rows.append(
                (
                    str(rng.choice(hostnames)),
                    rng.choice(variables),
                    rng.choice(serials),
                    rng.choice(machine_ids),
                    int(rng.integers(0, 10)),
                    bool(rng.integers(0, 2)),
                )
            )
        frame = make_host_metric_frame(rows)

        for iterations in (1, 2, 3):
            for dedup_class, legacy_class in DEDUP_CLASS_PAIRS:
                expected = run_dedup(legacy_class, frame, iterations)
                actual = run_dedup(dedup_class, frame, iterations)
                pd.testing.assert_frame_equal(actual, expected)


def test_bounded_iterations_are_preserved():
    """iterations=1 must stop one hop earlier than iterations=2, as in the original."""
    frame = make_host_metric_frame(DEDUP_DATASETS['multi_hop_chain'])

    one_iteration = run_dedup(DedupRenewal, frame, 1)
    two_iterations = run_dedup(DedupRenewal, frame, 2)

    pd.testing.assert_frame_equal(one_iteration, run_dedup(_LegacyDedupRenewal, frame, 1))
    pd.testing.assert_frame_equal(two_iterations, run_dedup(_LegacyDedupRenewal, frame, 2))

    assert len(one_iteration) == 2  # host-a/host-b/host-c merged, then host-b/host-c/host-d
    assert len(two_iterations) == 1  # a second pass joins everything through the ahv hop
    assert sorted(len(hostnames.split(', ')) for hostnames in one_iteration['hostnames']) == [3, 3]
    assert set(two_iterations['hostnames'].iloc[0].split(', ')) == {'host-a', 'host-b', 'host-c', 'host-d'}


def test_null_normalized_hostname_behaves_like_legacy():
    """A null normalized hostname matched nothing before, so it still fails on an empty group."""
    frame = make_host_metric_frame(
        [
            ('host-a', None, 'serial-1', 'machine-1', 1, False),
            (None, None, 'serial-2', 'machine-2', 2, False),
        ]
    )

    with pytest.raises(IndexError):
        run_dedup(_LegacyDedupRenewalHostname, frame, 3)
    with pytest.raises(IndexError):
        run_dedup(DedupRenewalHostname, frame, 3)


@pytest.mark.parametrize('dedup_class', [DedupRenewal, DedupRenewalHostname, DedupRenewalExperimental])
def test_na_and_empty_serials_are_cleaned_once(dedup_class):
    """'NA'/'' serials are dropped without the redundant per-group re-cleanup."""
    frame = make_host_metric_frame(
        [
            ('host-a', '10.0.0.1', 'NA', 'NA', 1, False),
            ('host-a', '10.0.0.1', '', '', 2, False),
        ]
    )

    result = run_dedup(dedup_class, frame, 3)

    assert len(result) == 1
    assert result.iloc[0]['ansible_product_serials'] == ''
    assert result.iloc[0]['ansible_machine_ids'] == ''


class TestReportDaysAutomated:
    """`days_automated` must keep the exact values and dtype of the removed elementwise max(0, x)."""

    @staticmethod
    def _build_frame(offsets):
        base = datetime(2025, 1, 1)
        first = [base] * len(offsets)
        last = [base + timedelta(days=offset) if offset is not None else pd.NaT for offset in offsets]
        return pd.DataFrame(
            {
                'index': range(len(offsets)),
                'hostname': [f'host-{position}' for position in range(len(offsets))],
                'first_automation': first,
                'last_automation': last,
                'deleted': [False] * len(offsets),
            }
        )

    @staticmethod
    def _build_report(frame):
        extra_params = {
            'ephemeral_days': None,
            'report_period': '2025-01-01,2025-02-01',
            'since_date': '2025-01-01',
            'until_date': '2025-02-01',
            'optional_sheets': [],
        }
        patch_target = 'metrics_utility.automation_controller_billing.report.report_renewal_guidance.Workbook'
        with patch(patch_target):
            ReportRenewalGuidance(dataframes={'host_metric': frame}, extra_params=extra_params).build_definition()
        return frame

    @pytest.mark.parametrize(
        'offsets',
        [
            (-3, 3, 0, None),  # negative, positive, zero and NaT-derived NaN
            (-5, -1, None, None),  # only non-positive values (legacy inferred int64)
            (0, 0, 0, 0),
            (1, 2, 3, None),
        ],
    )
    def test_matches_elementwise_max(self, offsets):
        frame = self._build_frame(offsets)
        expected = (frame['last_automation'] - frame['first_automation']).dt.days.apply(lambda x: max(0, x))
        expected.name = 'days_automated'

        result = self._build_report(frame)

        pd.testing.assert_series_equal(result['days_automated'], expected)

    def test_values_and_dtype(self):
        """Explicit expectations: negative/NaN become 0, the column stays float64."""
        frame = self._build_frame((-3, 3, 0, None))

        result = self._build_report(frame)

        assert result['days_automated'].tolist() == [0.0, 3.0, 0.0, 0.0]
        assert result['days_automated'].dtype == np.dtype('float64')

    def test_all_non_positive_keeps_legacy_int64(self):
        """`max(0, nan)`/`max(0, -n)` returned integers, so pandas inferred int64."""
        frame = self._build_frame((-5, -1, None))

        result = self._build_report(frame)

        assert result['days_automated'].tolist() == [0, 0, 0]
        assert result['days_automated'].dtype == np.dtype('int64')


def test_experimental_group_by_individual_serial_and_direct_merge():
    """Cover individual-serial fallback grouping (including empty serials) and direct merge."""
    base_df = pd.DataFrame(
        [
            {
                'index': 0,
                'hostname': 'host-a',
                'ansible_host_variable': None,
                'ansible_product_serial': 'SHARED-ID',
                'ansible_machine_id': None,
                'deleted': False,
                'first_automation': pd.Timestamp('2026-01-01'),
                'last_automation': pd.Timestamp('2026-01-10'),
                'automated_counter': 2,
                'deleted_counter': 0,
                'last_deleted': pd.NaT,
            },
            {
                'index': 1,
                'hostname': 'host-b',
                'ansible_host_variable': None,
                'ansible_product_serial': None,
                'ansible_machine_id': 'SHARED-ID',
                'deleted': False,
                'first_automation': pd.Timestamp('2026-01-02'),
                'last_automation': pd.Timestamp('2026-01-12'),
                'automated_counter': 3,
                'deleted_counter': 0,
                'last_deleted': pd.NaT,
            },
        ]
    )
    host_metric_engine = Mock()
    host_metric_engine.build_dataframe.side_effect = lambda: base_df.copy()
    dedup = DedupRenewalExperimental({'host_metric': host_metric_engine}, {'report_renewal_guidance_dedup_iterations': '3'})

    # End-to-end: product_serial on host-a matches machine_id on host-b via individual serial fallback.
    result = dedup.run()['host_metric']
    assert len(result) == 1
    assert result.iloc[0]['hostname'] == 'host-b'
    assert result.iloc[0]['automated_counter'] == 5

    # Direct helper check: empty serial entries are ignored and repeated serial keys are deduplicated.
    expanded_df = pd.DataFrame(
        [
            {'hostname_group': 'host-a', 'individual_serials': ['', 'S1']},
            {'hostname_group': 'host-b', 'individual_serials': ['S1', '']},
            {'hostname_group': 'host-c', 'individual_serials': ['S1']},
        ]
    )
    serial_groups = {}
    processed = set()
    dedup._group_by_individual_serial(expanded_df, serial_groups, processed)
    assert serial_groups == {
        'individual:S1': {
            'canonical_group': 'host-a',
            'groups_to_merge': ['host-a', 'host-b', 'host-c'],
            'serial_type': 'individual',
        }
    }
    assert processed == {'host-a', 'host-b', 'host-c'}

    # Direct _merge_hostname_groups() call with default hostname_positions=None.
    hostname_df = DedupRenewalHostname({'host_metric': host_metric_engine}, {'report_renewal_guidance_dedup_iterations': '3'}).run()['host_metric']
    merged = dedup._merge_hostname_groups(hostname_df, ['host-a', 'host-b'])
    assert merged['hostname'] == 'host-b'
    assert merged['automated_counter'] == 5


########################################################################################
# Scaling regressions: one identifier shared by many rows/groups must not go quadratic.
# No timing assertions here (they are flaky); these tests assert the optimised paths still
# match the legacy oracles on the shapes that used to blow up.
########################################################################################

SHARED_SERIAL_ROW_COUNT = 3000
SHARED_SERIAL_HOSTNAME_GROUP_COUNT = 8000


def make_shared_serial_frame(row_count=SHARED_SERIAL_ROW_COUNT):
    """Many distinct hostnames, every one reporting the same product serial."""
    return make_host_metric_frame(
        [(f'host-{offset:05d}', None, 'shared-product-serial', f'machine-{offset:05d}', offset % 90, offset % 7 == 0) for offset in range(row_count)]
    )


@pytest.mark.parametrize('iterations', [1, 3])
def test_renewal_shared_serial_matches_legacy(iterations):
    """One serial shared by thousands of rows must still match the legacy oracle.

    Before the fix, every position in the group re-expanded the same serial bucket once per
    row and per iteration (O(N^2)); now each value is expanded at most once per group.
    """
    frame = make_shared_serial_frame()

    expected = run_dedup(_LegacyDedupRenewal, frame, iterations)
    actual = run_dedup(DedupRenewal, frame, iterations)

    pd.testing.assert_frame_equal(actual, expected)
    assert len(actual) == 1
    assert actual.iloc[0]['hostmetric_record_count'] == SHARED_SERIAL_ROW_COUNT
    assert len(actual.iloc[0]['hostnames'].split(', ')) == SHARED_SERIAL_ROW_COUNT


def _normalize_serial_groups(serial_groups):
    """Make legacy (numpy array) and optimised (list) groups_to_merge comparable."""
    return {
        serial_key: {
            'canonical_group': info['canonical_group'],
            'groups_to_merge': list(info['groups_to_merge']),
            'serial_type': info['serial_type'],
        }
        for serial_key, info in serial_groups.items()
    }


@pytest.mark.parametrize(
    'compound_serial',
    ['shared-serial/machine-id', None],
    ids=['compound_serial', 'individual_serial'],
)
def test_experimental_serial_grouping_shared_serial_matches_legacy(compound_serial):
    """Thousands of hostname groups sharing one serial must match the legacy grouping.

    The first pass used `if hostname_group not in hostname_groups` (a list scan per row), so a
    single shared serial made it O(N^2). Insertion-ordered deduplication keeps the same
    first-seen order, canonical group and processed-group set.
    """
    hostname_groups = [f'host-{offset:05d}' for offset in range(SHARED_SERIAL_HOSTNAME_GROUP_COUNT)]
    expanded_df = pd.DataFrame(
        {
            'hostname': hostname_groups,
            'hostname_group': hostname_groups,
            'compound_serial': [compound_serial] * SHARED_SERIAL_HOSTNAME_GROUP_COUNT,
            'individual_serials': [['shared-serial'] for _ in hostname_groups],
        }
    )

    dataframes = {'host_metric': Mock()}
    dataframes['host_metric'].build_dataframe.return_value = pd.DataFrame()
    extra_params = {'report_renewal_guidance_dedup_iterations': '3'}
    dedup = DedupRenewalExperimental(dataframes, extra_params)
    legacy = _LegacyDedupRenewalExperimental(dataframes, extra_params)

    serial_groups, processed_hostname_groups = dedup._create_serial_groups(expanded_df)
    legacy_serial_groups, legacy_processed_hostname_groups = legacy._create_serial_groups(expanded_df)

    assert _normalize_serial_groups(serial_groups) == _normalize_serial_groups(legacy_serial_groups)
    assert processed_hostname_groups == legacy_processed_hostname_groups == set(hostname_groups)

    # Exactly one serial group, spanning every hostname group in first-seen order.
    assert len(serial_groups) == 1
    (serial_info,) = serial_groups.values()
    assert serial_info['groups_to_merge'] == hostname_groups
    assert serial_info['canonical_group'] == hostname_groups[0]
