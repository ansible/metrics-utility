import pandas as pd

from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup


# ---------------------------------------------------------------------------
# prepare()
# ---------------------------------------------------------------------------


def test_prepare_returns_flag_names():
    rollup = FeatureFlagsAnonymizedRollup()
    df = pd.DataFrame([{'name': 'flag_a'}, {'name': 'flag_b'}])
    result = rollup.prepare(df)
    assert result == ['flag_a', 'flag_b']


def test_prepare_none_returns_empty_list():
    rollup = FeatureFlagsAnonymizedRollup()
    result = rollup.prepare(None)
    assert result == []


def test_prepare_empty_dataframe_returns_empty_list():
    rollup = FeatureFlagsAnonymizedRollup()
    result = rollup.prepare(pd.DataFrame())
    assert result == []


def test_prepare_missing_name_column_returns_empty_list():
    rollup = FeatureFlagsAnonymizedRollup()
    df = pd.DataFrame([{'other_column': 'value'}])
    result = rollup.prepare(df)
    assert result == []


# ---------------------------------------------------------------------------
# merge()
# ---------------------------------------------------------------------------


def test_merge_returns_new_data():
    rollup = FeatureFlagsAnonymizedRollup()
    assert rollup.merge(['old_flag'], ['new_flag']) == ['new_flag']


def test_merge_with_none_old_returns_new():
    rollup = FeatureFlagsAnonymizedRollup()
    assert rollup.merge(None, ['flag_x']) == ['flag_x']


# ---------------------------------------------------------------------------
# base()
# ---------------------------------------------------------------------------


def test_base_none_returns_empty_json():
    rollup = FeatureFlagsAnonymizedRollup()
    result = rollup.base(None)
    assert result == {'json': []}


def test_base_with_list():
    rollup = FeatureFlagsAnonymizedRollup()
    data = ['flag_a', 'flag_b']
    result = rollup.base(data)
    assert result == {'json': ['flag_a', 'flag_b']}


def test_base_with_dataframe_calls_prepare():
    rollup = FeatureFlagsAnonymizedRollup()
    df = pd.DataFrame([{'name': 'flag_x'}])
    result = rollup.base(df)
    assert result == {'json': ['flag_x']}
