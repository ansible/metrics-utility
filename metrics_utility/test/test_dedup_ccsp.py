"""Unit tests for the CCSP dedup module, including _LazyDataFrameDict."""

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dedup.ccsp import (
    DedupCCSP,
    _LazyDataFrameDict,
)


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


# ---------------------------------------------------------------------------
# DedupCCSP standard (non-experimental)
# ---------------------------------------------------------------------------


class TestDedupCCSPStandard:
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


# ---------------------------------------------------------------------------
# DedupCCSP experimental
# ---------------------------------------------------------------------------


class TestDedupCCSPExperimental:
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
