from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

import metrics_utility.automation_controller_billing.helpers as helpers_module

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import (
    DataframeInventoryScope,
    compute_serial,
    serial_from_facts,
)
from metrics_utility.automation_controller_billing.helpers import merge_json_sets


@pytest.mark.parametrize(
    ('facts', 'expected'),
    [
        ('{"ansible_product_serial":"serial","ansible_machine_id":"machine"}', 'serial/machine'),
        ({'ansible_product_serial': 'serial', 'ansible_machine_id': 'machine'}, 'serial/machine'),
        ('{"ansible_product_serial":"serial","ansible_machine_id":""}', 'serial/'),
        ('{"ansible_product_serial":null,"ansible_machine_id":"machine"}', None),
        ('{}', None),
        ('not json', None),
        (None, None),
        (pd.NA, None),
    ],
)
def test_serial_from_facts_handles_facts(facts, expected):
    assert serial_from_facts(facts) == expected
    dataframe = pd.DataFrame({'canonical_facts': [facts]})
    assert dataframe['canonical_facts'].map(serial_from_facts).iloc[0] == expected


def test_serial_from_facts_preserves_duplicate_index_and_dtype():
    dataframe = pd.DataFrame(
        {
            'canonical_facts': [
                '{"ansible_product_serial":"first","ansible_machine_id":"one"}',
                None,
                '{"ansible_product_serial":"second","ansible_machine_id":"two"}',
            ],
            'other': [1, 2, 3],
        },
        index=[4, 2, 4],
    )

    result = dataframe['canonical_facts'].map(serial_from_facts)

    expected = pd.Series(['first/one', None, 'second/two'], index=[4, 2, 4], dtype=result.dtype)
    pd.testing.assert_series_equal(result, expected, check_names=False)
    pd.testing.assert_series_equal(result, dataframe.apply(compute_serial, axis=1), check_names=False)


@pytest.mark.parametrize('facts', ['null', '[]', '123'])
def test_serial_from_facts_non_object_json_currently_raises(facts):
    """Documents current behavior, not a contract: the parse_json gap is out of scope."""
    with pytest.raises(AttributeError):
        serial_from_facts(facts)


def test_build_dataframe_combines_serials_across_batches():
    def host(facts):
        return pd.DataFrame(
            {
                'host_name': ['host'],
                'organization_name': ['org'],
                'inventory_name': ['inventory'],
                'canonical_facts': [facts],
                'facts': ['{}'],
                'last_automation': ['2026-01-01T12:00:00+00:00'],
            }
        )

    batches = [
        {
            'main_host': host('{"ansible_product_serial":"serial-a","ansible_machine_id":"machine-a"}'),
            'main_host_daily': pd.DataFrame(),
            'config': {'install_uuid': 'install'},
        },
        {
            'main_host': host('{"ansible_product_serial":"serial-b","ansible_machine_id":"machine-b"}'),
            'main_host_daily': pd.DataFrame(),
            'config': {'install_uuid': 'install'},
        },
    ]
    extractor = MagicMock()
    extractor.iter_batches.return_value = iter(batches)
    engine = DataframeInventoryScope(
        extractor=extractor,
        month=date(2026, 1, 1),
        extra_params={'since_date': date(2026, 1, 1), 'until_date': date(2026, 1, 1)},
    )

    result = engine.build_dataframe()

    assert len(result) == 1
    assert result.iloc[0]['host_name'] == 'host'
    assert result.iloc[0]['serials'] == {'serial-a/machine-a', 'serial-b/machine-b'}


def _engine_for_batches(batches):
    extractor = MagicMock()
    extractor.iter_batches.return_value = iter(batches)
    return DataframeInventoryScope(
        extractor=extractor,
        month=date(2026, 1, 1),
        extra_params={'since_date': date(2026, 1, 1), 'until_date': date(2026, 1, 1)},
    )


def _single_batch(facts_values, facts='{}'):
    return [
        {
            'main_host': pd.DataFrame(
                {
                    'host_name': ['host'] * len(facts_values),
                    'organization_name': ['org'] * len(facts_values),
                    'inventory_name': ['inventory'] * len(facts_values),
                    'canonical_facts': facts_values,
                    'facts': [facts] * len(facts_values),
                    'last_automation': ['2026-01-01T12:00:00+00:00'] * len(facts_values),
                }
            ),
            'main_host_daily': pd.DataFrame(),
            'config': {'install_uuid': 'install'},
        }
    ]


def test_build_dataframe_parses_canonical_facts_once_per_row(monkeypatch):
    """json.loads() must run once per canonical_facts row, not twice."""
    real_loads = helpers_module.json.loads
    decoded = []

    def counting_loads(value, *args, **kwargs):
        if isinstance(value, str) and 'ansible_product_serial' in value:
            decoded.append(value)
        return real_loads(value, *args, **kwargs)

    monkeypatch.setattr(helpers_module.json, 'loads', counting_loads)

    facts_values = [
        '{"ansible_product_serial":"s1","ansible_machine_id":"m1"}',
        '{"ansible_product_serial":"s2","ansible_machine_id":"m2"}',
        '{"ansible_product_serial":"s3","ansible_machine_id":"m3"}',
    ]
    result = _engine_for_batches(_single_batch(facts_values)).build_dataframe()

    assert result.iloc[0]['serials'] == {'s1/m1', 's2/m2', 's3/m3'}
    # Three rows, three decodes: the previous implementation decoded each row a
    # second time inside merge_json_sets().
    assert len(decoded) == len(facts_values)


@pytest.mark.parametrize(
    'raw',
    [
        '{"ansible_product_serial":"s1","ansible_machine_id":"m1"}',
        {'ansible_product_serial': 's2', 'ansible_machine_id': 'm2'},
        '{"ansible_product_serial":"s3","ansible_machine_id":"m3","nested":{"k":[1,2]}}',
        '{}',
        'not json',
        None,
        float('nan'),
    ],
    ids=['json-string', 'dict', 'nested-json', 'empty-json', 'malformed', 'none', 'nan'],
)
def test_build_dataframe_matches_direct_helper_results(raw):
    """Pre-parsing must not change serial/facts results vs. the per-row helpers."""
    expected_serial = serial_from_facts(raw)
    expected_facts = merge_json_sets([raw])

    result = _engine_for_batches(_single_batch([raw])).build_dataframe()

    assert result.iloc[0]['serials'] == {expected_serial}
    assert result.iloc[0]['canonical_facts'] == expected_facts


@pytest.mark.parametrize('raw', ['[]', '123'])
def test_build_dataframe_non_object_json_yields_no_serial(raw):
    """Single-parse makes non-dict JSON degrade gracefully instead of raising."""
    result = _engine_for_batches(_single_batch([raw])).build_dataframe()

    assert result.iloc[0]['serials'] == {None}
    assert result.iloc[0]['canonical_facts'] == {}


def test_build_dataframe_nested_facts_are_merged_across_batches():
    batch1 = _single_batch(['{"tags":{"a":[1,2]},"ansible_product_serial":"s","ansible_machine_id":"m"}'])
    batch2 = _single_batch(['{"tags":{"a":[1,2]},"ansible_product_serial":"s","ansible_machine_id":"m"}'])

    result = _engine_for_batches(batch1 + batch2).build_dataframe()

    assert result.iloc[0]['serials'] == {'s/m'}
    canonical_facts = result.iloc[0]['canonical_facts']
    assert set(canonical_facts) == {'ansible_product_serial', 'ansible_machine_id', 'tags'}
    assert canonical_facts['tags'] == set(merge_json_sets([{'t': {'a': [1, 2]}}])['t'])
