from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_inventory_scope import (
    DataframeInventoryScope,
    compute_serial,
    serial_from_facts,
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
