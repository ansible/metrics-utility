from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import DBDataframeHostMetric


def test_build_dataframe_filters_deleted_only_host_after_raw_collection():
    since = datetime(2025, 6, 1, 12, tzinfo=UTC)
    extractor = MagicMock()
    extractor.extra_params = {'opt_since': since}
    extractor.iter_batches.return_value = iter(
        [
            {
                'host_metric': pd.DataFrame(
                    [
                        {
                            'hostname': 'automated-in-period',
                            'first_automation': '2025-05-01T00:00:00+00:00',
                            'last_automation': '2025-06-01T12:00:00+00:00',
                            'last_deleted': None,
                        },
                        {
                            'hostname': 'deleted-only',
                            'first_automation': '2025-05-01T00:00:00+00:00',
                            'last_automation': '2025-05-01T00:00:00+00:00',
                            'last_deleted': '2025-06-01T12:00:00+00:00',
                        },
                    ]
                )
            }
        ]
    )

    result = DBDataframeHostMetric(extractor=extractor, month='2025-06', extra_params={}).build_dataframe()

    assert list(result['hostname']) == ['automated-in-period']
    assert result['last_automation'].iloc[0] == datetime(2025, 6, 1, 12)


def test_build_dataframe_skips_empty_batch():
    extractor = MagicMock()
    extractor.extra_params = {'opt_since': datetime(2025, 6, 1, tzinfo=UTC)}
    extractor.iter_batches.return_value = iter([{'host_metric': pd.DataFrame()}])

    result = DBDataframeHostMetric(extractor=extractor, month='2025-06', extra_params={}).build_dataframe()

    assert result is None


def test_build_dataframe_skips_batch_with_no_automation_since_naive_since_is_utc():
    extractor = MagicMock()
    extractor.extra_params = {'opt_since': datetime(2025, 6, 1, 12)}
    extractor.iter_batches.return_value = iter(
        [
            {
                'host_metric': pd.DataFrame(
                    [
                        {
                            'hostname': 'outside-period',
                            'last_automation': '2025-06-01T11:59:59+00:00',
                        }
                    ]
                )
            }
        ]
    )

    result = DBDataframeHostMetric(extractor=extractor, month='2025-06', extra_params={}).build_dataframe()

    assert result is None


def test_build_dataframe_concatenates_multiple_qualifying_batches():
    extractor = MagicMock()
    extractor.extra_params = {'opt_since': datetime(2025, 6, 1, tzinfo=UTC)}

    def batch(hostname, timestamp):
        return {
            'host_metric': pd.DataFrame(
                [
                    {
                        'hostname': hostname,
                        'first_automation': '2025-05-01T00:00:00+00:00',
                        'last_automation': timestamp,
                        'last_deleted': None,
                    }
                ]
            )
        }

    extractor.iter_batches.return_value = iter([batch('first-host', '2025-06-01T00:00:00+00:00'), batch('second-host', '2025-06-02T00:00:00+00:00')])

    result = DBDataframeHostMetric(extractor=extractor, month='2025-06', extra_params={}).build_dataframe()

    assert list(result['hostname']) == ['first-host', 'second-host']
