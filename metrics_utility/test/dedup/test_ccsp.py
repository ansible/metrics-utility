from unittest.mock import Mock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dedup.ccsp import DedupCCSP


class TestDedupCCSP:
    """Test cases for the DedupCCSP class."""

    @pytest.fixture
    def mock_dataframes(self):
        """Mock dataframes for testing."""
        return {
            'main_host': Mock(),
            'job_host_summary': Mock(),
            'main_jobevent': Mock(),
            'data_collection_status': Mock(),
        }

    @pytest.fixture
    def base_extra_params(self):
        """Base extra parameters for testing."""
        return {
            'report_type': 'CCSP',
            'deduplicator': None,
        }

    def test_init_non_experimental(self, mock_dataframes, base_extra_params):
        """Test initialization with non-experimental mode."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)
        assert dedup.dataframes == mock_dataframes
        assert dedup.extra_params == base_extra_params
        assert dedup.experimental is False

    def test_init_experimental(self, mock_dataframes, base_extra_params):
        """Test initialization with experimental mode."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params, experimental=True)
        assert dedup.dataframes == mock_dataframes
        assert dedup.extra_params == base_extra_params
        assert dedup.experimental is True

    def test_run_non_experimental(self, mock_dataframes, base_extra_params):
        """Test run method in non-experimental mode."""
        # Mock the build_dataframe method
        for df in mock_dataframes.values():
            df.build_dataframe.return_value = pd.DataFrame()

        mock_dataframes['main_host'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupCCSP(mock_dataframes, base_extra_params)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'main_host' in result
        assert 'job_host_summary' in result
        assert 'main_jobevent' in result
        assert 'data_collection_status' in result

    def test_run_experimental_empty_host_data(self, mock_dataframes, base_extra_params):
        """Test run method in experimental mode with empty host data."""
        # Mock the build_dataframe method
        for df in mock_dataframes.values():
            df.build_dataframe.return_value = pd.DataFrame()

        mock_dataframes['main_host'].build_dataframe.return_value = pd.DataFrame()

        dedup = DedupCCSP(mock_dataframes, base_extra_params, experimental=True)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'main_host' in result
        assert 'job_host_summary' in result
        assert 'main_jobevent' in result
        assert 'data_collection_status' in result

    def test_run_experimental_with_host_data(self, mock_dataframes, base_extra_params):
        """Test run method in experimental mode with host data."""
        # Create sample host data
        host_data = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3'],
                'serials': [['serial1', 'serial2'], ['serial3'], ['serial1']],
            }
        )

        # Mock the build_dataframe method
        for df in mock_dataframes.values():
            df.build_dataframe.return_value = pd.DataFrame({'dummy': [1]})  # Non-empty dataframe

        mock_dataframes['main_host'].build_dataframe.return_value = host_data
        # Ensure job_host_summary and main_jobevent return non-empty dataframes
        mock_dataframes['job_host_summary'].build_dataframe.return_value = pd.DataFrame({'host_name': ['host1', 'host2']})
        mock_dataframes['main_jobevent'].build_dataframe.return_value = pd.DataFrame({'host_name': ['host1', 'host3']})

        # Mock the dedup method for dataframes that support it
        mock_dataframes['job_host_summary'].dedup = Mock(return_value=pd.DataFrame())
        mock_dataframes['main_jobevent'].dedup = Mock(return_value=pd.DataFrame())

        dedup = DedupCCSP(mock_dataframes, base_extra_params, experimental=True)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'main_host' in result
        assert 'job_host_summary' in result
        assert 'main_jobevent' in result
        assert 'data_collection_status' in result

        # Check that dedup was called on relevant dataframes
        mock_dataframes['job_host_summary'].dedup.assert_called_once()
        mock_dataframes['main_jobevent'].dedup.assert_called_once()
        mock_dataframes['main_host'].dedup.assert_called_once()

    def test_df_to_mapping_simple(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with simple data."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create test dataframe
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3'],
                'serials': [['serial1'], ['serial2'], ['serial1']],
            }
        )

        result = dedup.df_to_mapping(df)

        expected = {
            'host1': 'host1',
            'host3': 'host1',  # Both host1 and host3 have serial1
            'host2': 'host2',
        }

        assert result == expected

    def test_df_to_mapping_empty_serials(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with empty serials."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create test dataframe
        df = pd.DataFrame({'host_name': ['host1', 'host2', 'host3'], 'serials': [[], [], []]})

        result = dedup.df_to_mapping(df)

        assert result == {}

    def test_df_to_mapping_mixed_serials(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with mixed serial data."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create test dataframe
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3', 'host4'],
                'serials': [['serial1'], [], ['serial1', 'serial2'], ['serial2']],
            }
        )

        result = dedup.df_to_mapping(df)

        expected = {
            'host1': 'host1',
            'host3': 'host3',
            'host4': 'host3',  # host4 has serial2, which is also in host3
        }

        assert result == expected

    def test_df_to_mapping_none_serials(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with None values in serials."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create test dataframe
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3'],
                'serials': [['serial1', None], [None], ['serial1']],
            }
        )

        result = dedup.df_to_mapping(df)

        expected = {
            'host1': 'host1',
            'host3': 'host1',  # Both have serial1
        }

        assert result == expected

    def test_df_to_mapping_duplicate_serials(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with duplicate serials in same host."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create test dataframe
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2'],
                'serials': [['serial1', 'serial1'], ['serial2', 'serial2']],
            }
        )

        result = dedup.df_to_mapping(df)

        expected = {
            'host1': 'host1',
            'host2': 'host2',
        }

        assert result == expected

    def test_df_to_mapping_overlapping_serials(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with overlapping serial groups."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create test dataframe
        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3', 'host4'],
                'serials': [
                    ['serial1'],
                    ['serial1', 'serial2'],
                    ['serial2', 'serial3'],
                    ['serial3'],
                ],
            }
        )

        result = dedup.df_to_mapping(df)

        expected = {
            'host1': 'host1',
            'host2': 'host2',
            'host3': 'host3',
            'host4': 'host3',
        }

        assert result == expected

    def test_df_to_mapping_empty_dataframe(self, mock_dataframes, base_extra_params):
        """Test df_to_mapping with empty dataframe."""
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        # Create empty dataframe
        df = pd.DataFrame({'host_name': [], 'serials': []})

        result = dedup.df_to_mapping(df)

        assert result == {}

    def test_run_with_null_host_data(self, mock_dataframes, base_extra_params):
        """Test run method with None host data."""
        # Mock the build_dataframe method
        for df in mock_dataframes.values():
            df.build_dataframe.return_value = pd.DataFrame()

        mock_dataframes['main_host'].build_dataframe.return_value = None

        dedup = DedupCCSP(mock_dataframes, base_extra_params, experimental=True)
        result = dedup.run()

        assert isinstance(result, dict)
        assert 'main_host' in result
        assert result['main_host'] is None

    def test_df_to_mapping_nan_float_serials(self, mock_dataframes, base_extra_params):
        """Regression: pandas 3.0 stores missing serials as float('nan'), not None.

        Before the fix, bool(float('nan')) is True so NaN passed the old `if serial:` guard,
        causing all no-serial hosts to be bucketed together under serial_to_hosts[nan] and
        mapped to one canonical hostname.  After the fix, NaN serials must be ignored.
        """
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3'],
                'serials': [[float('nan')], [float('nan')], ['real-serial']],
            }
        )

        result = dedup.df_to_mapping(df)

        # host1 and host2 have no real serial — they must NOT be merged together
        assert 'host1' not in result or result.get('host1') == 'host1'
        assert 'host2' not in result or result.get('host2') == 'host2'
        # host3 has a real serial and maps to itself
        assert result.get('host3') == 'host3'
        # The NaN sentinel must never appear as a key in the mapping
        for key in result:
            assert isinstance(key, str), f'Expected string serial key, got {type(key)}: {key!r}'

    def test_df_to_mapping_empty_string_serials(self, mock_dataframes, base_extra_params):
        """Empty-string serials must not be used as canonical keys.

        compute_serial() returns None for missing data, but defensively ensure that
        an empty string — which would be falsy and semantically meaningless — is also
        filtered out rather than grouping unrelated hosts.
        """
        dedup = DedupCCSP(mock_dataframes, base_extra_params)

        df = pd.DataFrame(
            {
                'host_name': ['host1', 'host2', 'host3'],
                'serials': [[''], [''], ['valid-serial']],
            }
        )

        result = dedup.df_to_mapping(df)

        # host1 and host2 share only an empty serial — must NOT be merged
        assert 'host1' not in result or result.get('host1') == 'host1'
        assert 'host2' not in result or result.get('host2') == 'host2'
        assert result.get('host3') == 'host3'

    def test_run_experimental_preserves_data_collection_status(self, mock_dataframes, base_extra_params):
        """Test that experimental mode preserves data_collection_status without dedup."""
        # Create sample host data
        host_data = pd.DataFrame({'host_name': ['host1', 'host2'], 'serials': [['serial1'], ['serial2']]})

        # Mock the build_dataframe method
        for df in mock_dataframes.values():
            df.build_dataframe.return_value = pd.DataFrame()

        mock_dataframes['main_host'].build_dataframe.return_value = host_data

        # Mock the dedup method for dataframes that support it
        mock_dataframes['job_host_summary'].dedup = Mock(return_value=pd.DataFrame())
        mock_dataframes['main_jobevent'].dedup = Mock(return_value=pd.DataFrame())

        dedup = DedupCCSP(mock_dataframes, base_extra_params, experimental=True)
        dedup.run()

        # data_collection_status should not have dedup method called
        assert not hasattr(mock_dataframes['data_collection_status'], 'dedup') or not mock_dataframes['data_collection_status'].dedup.called
