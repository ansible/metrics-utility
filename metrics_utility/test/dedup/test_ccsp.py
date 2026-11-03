from unittest.mock import Mock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dedup.ccsp import (
    DedupCCSP,
    _LazyDataFrameDict,
    _optimize_dtypes,
)


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


# ---------------------------------------------------------------------------
# Fake engine that tracks whether build_dataframe() was called
# ---------------------------------------------------------------------------


class FakeEngine:
    """Minimal stand-in for a dataframe engine."""

    def __init__(self, df):
        self._df = df
        self.build_count = 0

    def build_dataframe(self):
        self.build_count += 1
        return self._df

    def dedup(self, df, mapping, **kwargs):
        return df


# ---------------------------------------------------------------------------
# _LazyDataFrameDict
# ---------------------------------------------------------------------------


class TestLazyDataFrameDict:
    def test_builds_on_first_access(self):
        engine = FakeEngine(pd.DataFrame({'a': [1]}))
        d = _LazyDataFrameDict({'x': engine})
        assert engine.build_count == 0
        result = d['x']
        assert engine.build_count == 1
        assert list(result['a']) == [1]

    def test_second_access_does_not_rebuild(self):
        engine = FakeEngine(pd.DataFrame({'a': [1]}))
        d = _LazyDataFrameDict({'x': engine})
        d['x']
        d['x']
        assert engine.build_count == 1

    def test_eager_set_bypasses_engine(self):
        engine = FakeEngine(pd.DataFrame({'a': [1]}))
        d = _LazyDataFrameDict({'x': engine})
        d['x'] = pd.DataFrame({'b': [2]})
        result = d['x']
        assert engine.build_count == 0
        assert list(result.columns) == ['b']

    def test_contains_checks_engines(self):
        engine = FakeEngine(pd.DataFrame())
        d = _LazyDataFrameDict({'x': engine})
        assert 'x' in d
        assert 'y' not in d

    def test_del_removes_built_and_engine(self):
        engine = FakeEngine(pd.DataFrame({'a': [1]}))
        d = _LazyDataFrameDict({'x': engine})
        d['x']  # build it
        del d['x']
        assert 'x' not in d

    def test_del_unbuilt_key_removes_engine(self):
        engine = FakeEngine(pd.DataFrame({'a': [1]}))
        d = _LazyDataFrameDict({'x': engine})
        del d['x']
        assert 'x' not in d
        assert engine.build_count == 0

    def test_del_missing_key_does_not_raise(self):
        d = _LazyDataFrameDict({})
        del d['nope']  # should not raise

    def test_items_yields_all_entries(self):
        eng_a = FakeEngine(pd.DataFrame({'v': [1]}))
        eng_b = FakeEngine(pd.DataFrame({'v': [2]}))
        d = _LazyDataFrameDict({'a': eng_a, 'b': eng_b})

        # set only 'a' eagerly
        d['a'] = pd.DataFrame({'v': [10]})

        items = dict(d.items())
        assert set(items.keys()) == {'a', 'b'}
        # 'a' was set eagerly, so no build; 'b' was built lazily via items()
        assert eng_a.build_count == 0
        assert eng_b.build_count == 1

    def test_getitem_on_missing_key_raises(self):
        d = _LazyDataFrameDict({'x': FakeEngine(pd.DataFrame())})
        with pytest.raises(KeyError):
            d['no_such_key']

    def test_lazy_build_applies_categorical_dtypes(self):
        engine = FakeEngine(
            pd.DataFrame(
                {
                    'install_uuid': ['uuid1'] * 100,
                    'organization_name': ['org1', 'org2'] * 50,
                    'other_col': range(100),
                }
            )
        )
        d = _LazyDataFrameDict({'x': engine})
        df = d['x']
        assert df['install_uuid'].dtype.name == 'category'
        assert df['organization_name'].dtype.name == 'category'
        assert df['other_col'].dtype != 'category'


# ---------------------------------------------------------------------------
# _optimize_dtypes
# ---------------------------------------------------------------------------


class TestOptimizeDtypes:
    def test_converts_known_columns(self):
        df = pd.DataFrame(
            {
                'install_uuid': ['u1'] * 10,
                'organization_name': ['o1', 'o2'] * 5,
                'job_template_name': ['t1'] * 10,
                'module_name': ['m1', 'm2'] * 5,
                'collection_name': ['c1'] * 10,
                'role_name': ['r1'] * 10,
            }
        )
        _optimize_dtypes(df)
        for col in df.columns:
            assert df[col].dtype.name == 'category'

    def test_leaves_non_string_columns_alone(self):
        df = pd.DataFrame({'install_uuid': [1, 2, 3]})
        _optimize_dtypes(df)
        assert df['install_uuid'].dtype != 'category'

    def test_leaves_unknown_columns_alone(self):
        df = pd.DataFrame({'host_name': ['h1', 'h2', 'h3']})
        _optimize_dtypes(df)
        assert df['host_name'].dtype.name != 'category'

    def test_handles_none_and_empty(self):
        _optimize_dtypes(None)
        _optimize_dtypes(pd.DataFrame())


# ---------------------------------------------------------------------------
# DedupCCSP standard mode (lazy loading)
# ---------------------------------------------------------------------------


class TestDedupCCSPStandardLazy:
    def _engines(self, main_host_df=None, jhs_df=None, events_df=None, status_df=None):
        return {
            'main_host': FakeEngine(main_host_df),
            'job_host_summary': FakeEngine(jhs_df),
            'main_jobevent': FakeEngine(events_df),
            'data_collection_status': FakeEngine(status_df),
        }

    def test_only_jhs_built_eagerly(self):
        engines = self._engines(
            main_host_df=pd.DataFrame({'host_name': ['h1'], 'serials': [['s1']]}),
            jhs_df=pd.DataFrame({'x': [1]}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        dedup.run()

        assert engines['job_host_summary'].build_count == 1
        assert engines['main_host'].build_count == 0
        assert engines['main_jobevent'].build_count == 0
        assert engines['data_collection_status'].build_count == 0

    def test_lazy_access_triggers_build(self):
        engines = self._engines(
            main_host_df=pd.DataFrame({'host_name': ['h1']}),
            jhs_df=pd.DataFrame({'x': [1]}),
            events_df=pd.DataFrame({'y': [2]}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()

        assert engines['main_jobevent'].build_count == 0
        _ = result['main_jobevent']
        assert engines['main_jobevent'].build_count == 1

    def test_returns_lazy_dict(self):
        engines = self._engines(jhs_df=pd.DataFrame({'x': [1]}))
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()
        assert isinstance(result, _LazyDataFrameDict)

    def test_items_yields_all_keys(self):
        engines = self._engines(
            main_host_df=pd.DataFrame({'host_name': ['h1']}),
            jhs_df=pd.DataFrame({'x': [1]}),
            events_df=pd.DataFrame({'y': [2]}),
            status_df=pd.DataFrame({'status': ['ok']}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()

        built_keys = [name for name, _ in result.items()]
        assert set(built_keys) == {
            'job_host_summary',
            'main_host',
            'main_jobevent',
            'data_collection_status',
        }

    def test_all_empty_check_works_with_nonempty_jhs(self):
        engines = self._engines(
            main_host_df=None,
            jhs_df=pd.DataFrame({'x': [1]}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()

        all_empty = all(df is None or df.empty for _, df in result.items())
        assert all_empty is False

    def test_all_empty_check_with_all_empty(self):
        engines = self._engines(
            main_host_df=pd.DataFrame(),
            jhs_df=pd.DataFrame(),
            events_df=pd.DataFrame(),
            status_df=pd.DataFrame(),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()

        all_empty = all(df is None or df.empty for _, df in result.items())
        assert all_empty is True

    def test_empty_jhs_nonempty_status_not_skipped(self):
        """When JHS is empty but data_collection_status has data, the
        all_empty check must return False so the report is still generated."""
        engines = self._engines(
            main_host_df=pd.DataFrame(),
            jhs_df=pd.DataFrame(),
            events_df=pd.DataFrame(),
            status_df=pd.DataFrame({'status': ['ok']}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()

        all_empty = all(df is None or df.empty for _, df in result.items())
        assert all_empty is False

    def test_drops_host_names_before_dedup(self):
        engines = self._engines(
            jhs_df=pd.DataFrame(
                {
                    'x': [1],
                    'host_names_before_dedup': [None],
                }
            ),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()
        assert 'host_names_before_dedup' not in result['job_host_summary'].columns

    def test_applies_categorical_dtypes_to_jhs(self):
        engines = self._engines(
            jhs_df=pd.DataFrame(
                {
                    'install_uuid': ['uuid1'] * 10,
                    'organization_name': ['org1', 'org2'] * 5,
                    'host_name': [f'h{i}' for i in range(10)],
                }
            ),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=False)
        result = dedup.run()
        jhs = result['job_host_summary']
        assert jhs['install_uuid'].dtype.name == 'category'
        assert jhs['organization_name'].dtype.name == 'category'
        assert jhs['host_name'].dtype.name != 'category'


# ---------------------------------------------------------------------------
# DedupCCSP experimental mode (lazy loading)
# ---------------------------------------------------------------------------


class TestDedupCCSPExperimentalLazy:
    def _engines(self, main_host_df=None, jhs_df=None, events_df=None, status_df=None):
        return {
            'main_host': FakeEngine(main_host_df),
            'job_host_summary': FakeEngine(jhs_df),
            'main_jobevent': FakeEngine(events_df),
            'data_collection_status': FakeEngine(status_df),
        }

    def test_builds_dedup_participants_eagerly(self):
        engines = self._engines(
            main_host_df=pd.DataFrame(
                {
                    'host_name': ['h1', 'h2'],
                    'serials': [['serial1'], ['serial1']],
                }
            ),
            jhs_df=pd.DataFrame({'host_name': ['h1'], 'x': [1]}),
            events_df=pd.DataFrame({'host_name': ['h1'], 'y': [2]}),
            status_df=pd.DataFrame({'status': ['ok']}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=True)
        dedup.run()

        assert engines['main_host'].build_count == 1
        assert engines['job_host_summary'].build_count == 1
        assert engines['main_jobevent'].build_count == 1
        assert engines['data_collection_status'].build_count == 0

    def test_data_collection_status_built_lazily(self):
        engines = self._engines(
            main_host_df=pd.DataFrame({'host_name': ['h1'], 'serials': [['s1']]}),
            jhs_df=pd.DataFrame({'host_name': ['h1'], 'x': [1]}),
            events_df=pd.DataFrame({'host_name': ['h1'], 'y': [2]}),
            status_df=pd.DataFrame({'status': ['ok']}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=True)
        result = dedup.run()

        assert engines['data_collection_status'].build_count == 0
        _ = result['data_collection_status']
        assert engines['data_collection_status'].build_count == 1

    def test_empty_main_host_skips_dedup(self):
        engines = self._engines(
            main_host_df=pd.DataFrame(),
            jhs_df=pd.DataFrame({'x': [1]}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=True)
        result = dedup.run()
        assert isinstance(result, _LazyDataFrameDict)

    def test_preserves_host_names_before_dedup(self):
        engines = self._engines(
            main_host_df=pd.DataFrame({'host_name': ['h1'], 'serials': [['s1']]}),
            jhs_df=pd.DataFrame(
                {
                    'host_name': ['h1'],
                    'host_names_before_dedup': [{'h1'}],
                }
            ),
            events_df=pd.DataFrame({'host_name': ['h1']}),
        )
        dedup = DedupCCSP(engines, extra_params={}, experimental=True)
        result = dedup.run()
        assert 'host_names_before_dedup' in result['job_host_summary'].columns
