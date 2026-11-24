from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import (
    DataframeInventoryScope,
    _compute_serials,
    compute_serial,
)


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
def test_compute_serials_handles_facts(facts, expected):
    dataframe = pd.DataFrame({'canonical_facts': [facts], 'other': [1]})

    assert _compute_serials(dataframe).iloc[0] == expected


def test_compute_serials_preserves_duplicate_index_and_dtype():
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

    result = _compute_serials(dataframe)

    expected = pd.Series(['first/one', None, 'second/two'], index=[4, 2, 4], dtype=result.dtype)
    pd.testing.assert_series_equal(result, expected, check_names=False)
    pd.testing.assert_series_equal(result, dataframe.apply(compute_serial, axis=1), check_names=False)


@pytest.mark.parametrize('facts', ['null', '[]', '123'])
def test_compute_serials_preserves_invalid_json_value_errors(facts):
    dataframe = pd.DataFrame({'canonical_facts': [facts], 'other': [1]})

    with pytest.raises(AttributeError):
        _compute_serials(dataframe)


def test_compute_serials_preserves_missing_column_error():
    with pytest.raises(KeyError, match='canonical_facts'):
        _compute_serials(pd.DataFrame({'other': [1]}))


@pytest.mark.parametrize(
    'dataframe',
    [
        pd.DataFrame({'canonical_facts': pd.Series(dtype=object)}),
        pd.DataFrame([['{}', '{}']], columns=['canonical_facts', 'canonical_facts']),
        pd.DataFrame([['{}']], columns=pd.MultiIndex.from_tuples([('canonical_facts', 'value')])),
    ],
    ids=['empty', 'duplicate-columns', 'multi-level-columns'],
)
def test_compute_serials_preserves_unusual_dataframe_behavior(dataframe):
    expected = dataframe.apply(compute_serial, axis=1)

    pd.testing.assert_series_equal(_compute_serials(dataframe), expected)


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
