import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.task_executions_anonymized_rollup import TaskExecutionsAnonymizedRollup


def make_df(rows):
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# prepare()
# ---------------------------------------------------------------------------


def test_prepare_none_returns_empty_list():
    rollup = TaskExecutionsAnonymizedRollup()
    assert rollup.prepare(None) == []


def test_prepare_empty_dataframe_returns_empty_list():
    rollup = TaskExecutionsAnonymizedRollup()
    assert rollup.prepare(pd.DataFrame()) == []


def test_prepare_hourly_collector_counts_and_durations():
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {
            'collector_type': 'unified_jobs',
            'started_at': '2025-06-13T00:00:00Z',
            'completed_at': '2025-06-13T00:01:00Z',
        },
        {
            'collector_type': 'unified_jobs',
            'started_at': '2025-06-13T01:00:00Z',
            'completed_at': '2025-06-13T01:02:00Z',
        },
    ]
    result = rollup.prepare(make_df(rows))

    assert isinstance(result, list)
    assert len(result) == 1
    item = result[0]

    assert item['collector_type'] == 'unified_jobs'
    assert item['executions_total'] == 2
    # 24 expected for hourly – 2 actual = 22 missing
    assert item['executions_missing_total'] == 22
    assert item['execution_duration_total_seconds'] == pytest.approx(60.0 + 120.0)
    assert item['execution_duration_minimum_seconds'] == pytest.approx(60.0)
    assert item['execution_duration_maximum_seconds'] == pytest.approx(120.0)


def test_prepare_snapshot_collector_expected_1():
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {
            'collector_type': 'feature_flags_service',
            'started_at': '2025-06-13T10:00:00Z',
            'completed_at': '2025-06-13T10:00:05Z',
        },
    ]
    result = rollup.prepare(make_df(rows))

    assert len(result) == 1
    item = result[0]
    assert item['collector_type'] == 'feature_flags_service'
    assert item['executions_total'] == 1
    # 1 expected – 1 actual = 0 missing
    assert item['executions_missing_total'] == 0
    assert item['execution_duration_total_seconds'] == pytest.approx(5.0)
    assert item['execution_duration_minimum_seconds'] == pytest.approx(5.0)
    assert item['execution_duration_maximum_seconds'] == pytest.approx(5.0)


def test_prepare_executions_exceeding_expected_give_zero_missing():
    rollup = TaskExecutionsAnonymizedRollup()
    # 25 executions for an hourly collector (expected 24)
    rows = [
        {
            'collector_type': 'unified_jobs',
            'started_at': '2025-06-13T00:00:00Z',
            'completed_at': '2025-06-13T00:01:00Z',
        }
    ] * 25
    result = rollup.prepare(make_df(rows))
    assert result[0]['executions_missing_total'] == 0  # max(0, 24 – 25)


def test_prepare_unknown_collector_defaults_to_24_expected():
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {
            'collector_type': 'some_new_collector',
            'started_at': '2025-06-13T00:00:00Z',
            'completed_at': '2025-06-13T00:01:00Z',
        },
    ]
    result = rollup.prepare(make_df(rows))
    # default expected = 24, actual = 1 → missing = 23
    assert result[0]['executions_missing_total'] == 23


def test_prepare_missing_timestamps_gives_none_durations():
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {'collector_type': 'unified_jobs', 'started_at': None, 'completed_at': None},
    ]
    result = rollup.prepare(make_df(rows))
    item = result[0]
    assert item['execution_duration_total_seconds'] is None
    assert item['execution_duration_minimum_seconds'] is None
    assert item['execution_duration_maximum_seconds'] is None


def test_prepare_strips_surrounding_quotes_from_collector_type():
    """Collector types may arrive with surrounding quotes – they must be stripped."""
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {
            'collector_type': '"unified_jobs"',
            'started_at': '2025-06-13T00:00:00Z',
            'completed_at': '2025-06-13T00:01:00Z',
        },
    ]
    result = rollup.prepare(make_df(rows))
    assert result[0]['collector_type'] == 'unified_jobs'


def test_prepare_multiple_collector_types_produces_one_row_each():
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {
            'collector_type': 'unified_jobs',
            'started_at': '2025-06-13T00:00:00Z',
            'completed_at': '2025-06-13T00:01:00Z',
        },
        {
            'collector_type': 'feature_flags_service',
            'started_at': '2025-06-13T10:00:00Z',
            'completed_at': '2025-06-13T10:00:10Z',
        },
    ]
    result = rollup.prepare(make_df(rows))
    assert len(result) == 2
    types = {r['collector_type'] for r in result}
    assert types == {'unified_jobs', 'feature_flags_service'}


def test_prepare_all_snapshot_collectors_expect_1():
    rollup = TaskExecutionsAnonymizedRollup()
    for collector_type in ('execution_environments', 'table_metadata', 'controller_version_service', 'feature_flags_service'):
        rows = [
            {
                'collector_type': collector_type,
                'started_at': '2025-06-13T10:00:00Z',
                'completed_at': '2025-06-13T10:00:01Z',
            }
        ]
        result = rollup.prepare(make_df(rows))
        assert result[0]['executions_missing_total'] == 0, f'{collector_type} should have 0 missing'


def test_prepare_all_hourly_collectors_expect_24():
    rollup = TaskExecutionsAnonymizedRollup()
    for collector_type in ('unified_jobs', 'job_host_summary_service', 'credentials_service', 'main_jobevent_service'):
        rows = [
            {
                'collector_type': collector_type,
                'started_at': '2025-06-13T00:00:00Z',
                'completed_at': '2025-06-13T00:01:00Z',
            }
        ]
        result = rollup.prepare(make_df(rows))
        assert result[0]['executions_missing_total'] == 23, f'{collector_type} should have 23 missing'


# ---------------------------------------------------------------------------
# merge()
# ---------------------------------------------------------------------------


def test_merge_always_returns_new_data():
    rollup = TaskExecutionsAnonymizedRollup()
    old = [{'collector_type': 'unified_jobs', 'executions_total': 5}]
    new = [{'collector_type': 'unified_jobs', 'executions_total': 10}]
    assert rollup.merge(old, new) is new


def test_merge_with_none_old_returns_new():
    rollup = TaskExecutionsAnonymizedRollup()
    new = [{'collector_type': 'unified_jobs', 'executions_total': 3}]
    assert rollup.merge(None, new) is new


# ---------------------------------------------------------------------------
# base()
# ---------------------------------------------------------------------------


def test_base_none_returns_empty_json():
    rollup = TaskExecutionsAnonymizedRollup()
    result = rollup.base(None)
    assert result == {'json': []}


def test_base_with_prepared_list():
    rollup = TaskExecutionsAnonymizedRollup()
    data = [{'collector_type': 'unified_jobs', 'executions_total': 5}]
    result = rollup.base(data)
    assert result == {'json': data}


def test_base_with_dataframe_calls_prepare():
    rollup = TaskExecutionsAnonymizedRollup()
    rows = [
        {
            'collector_type': 'unified_jobs',
            'started_at': '2025-06-13T00:00:00Z',
            'completed_at': '2025-06-13T00:01:00Z',
        },
    ]
    df = pd.DataFrame(rows)
    result = rollup.base(df)
    assert isinstance(result, dict)
    assert 'json' in result
    assert isinstance(result['json'], list)
    assert result['json'][0]['collector_type'] == 'unified_jobs'
