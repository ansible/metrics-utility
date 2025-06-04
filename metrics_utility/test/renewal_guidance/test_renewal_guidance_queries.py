import datetime as dt_actual

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import DBDataframeHostMetric
from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance
from metrics_utility.test.util import generate_renewal_guidance_dataframe


# 1. Define a fixed datetime for testing
@pytest.fixture
def fixed_now():
    return dt_actual.datetime(2025, 6, 3, 10, 0, 0, tzinfo=dt_actual.timezone.utc)


# 2. Fixture to set up the processed DataFrame from DBDataframeHostMetric
@pytest.fixture
def setup_processed_dataframe(fixed_now):
    """
    Sets up the processed DataFrame by mocking DBDataframeHostMetric's extractor.
    """
    mock_full_dataframe_raw = generate_renewal_guidance_dataframe(is_empty=False, current_datetime=fixed_now)

    mock_df_for_batch_processed = mock_full_dataframe_raw.copy()
    for col in ['first_automation', 'last_automation']:
        mock_df_for_batch_processed[col] = pd.to_datetime(mock_df_for_batch_processed[col], utc=True).dt.tz_localize(None)

    mock_df_for_batch_processed['last_deleted'] = mock_df_for_batch_processed['last_deleted'].apply(
        lambda x: (x.isoformat(timespec='seconds') if pd.notna(x) and isinstance(x, dt_actual.datetime) else None)
    )

    mock_batches = [{'host_metric': mock_df_for_batch_processed}]

    mock_extractor = MagicMock()
    mock_extractor.iter_batches.return_value = (batch for batch in mock_batches)

    db_host_metric_instance = DBDataframeHostMetric(
        extractor=mock_extractor,
        month=fixed_now.strftime('%Y-%m'),
        extra_params={},
    )

    processed_df = db_host_metric_instance.build_dataframe()

    assert processed_df is not None, 'Fixture: build_dataframe should return a DataFrame'
    assert not processed_df.empty, 'Fixture: DataFrame should not be empty'
    assert len(processed_df) == len(mock_full_dataframe_raw), 'Fixture: DataFrame should contain all mock rows'
    assert pd.api.types.is_datetime64_any_dtype(processed_df['first_automation'])
    assert pd.api.types.is_datetime64_any_dtype(processed_df['last_automation'])
    assert pd.api.types.is_datetime64_any_dtype(processed_df['last_deleted'])

    yield processed_df


# 3. Fixture for patching datetime and providing extra_params for ReportRenewalGuidance
@pytest.fixture
def setup_report_renewal_guidance_instance(fixed_now):
    """
    Patches datetime.datetime.now() in report_renewal_guidance.py and
    provides parameters for ReportRenewalGuidance instance.
    """
    patch_target = 'metrics_utility.automation_controller_billing.report.report_renewal_guidance.datetime'

    with patch(patch_target) as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_now
        mock_datetime_module.datetime.utcnow.return_value = fixed_now.replace(tzinfo=None)
        mock_datetime_module.timedelta = dt_actual.timedelta
        mock_datetime_module.timezone = MagicMock(spec=dt_actual.timezone)
        mock_datetime_module.timezone.utc = dt_actual.timezone.utc

        test_ephemeral_days = 30
        report_period_range = '2025-01-01,2025-06-03'
        test_extra_params = {
            'opt_ephemeral': f'{test_ephemeral_days} days',
            'price_per_node': 0.1,
            'report_period_range': report_period_range,
            'since_date': '2025-01-01',
            'until_date': '2025-06-03',
        }

        yield {
            'report_period': report_period_range,
            'extra_params': test_extra_params,
        }


def test_renewal_guidance_queries_with_mocked_data(setup_report_renewal_guidance_instance, setup_processed_dataframe):
    """
    Tests the df_managed_nodes_query and df_deleted_managed_nodes_query methods
    of ReportRenewalGuidance using the mocked processed DataFrame and patched datetime.
    """
    processed_df = setup_processed_dataframe

    report_instance_params = setup_report_renewal_guidance_instance

    report_instance = ReportRenewalGuidance(
        dataframe=[processed_df],
        report_period=report_instance_params['report_period'],
        extra_params=report_instance_params['extra_params'],
    )

    # --- Test df_managed_nodes_query ---
    non_deleted_df = report_instance.df_managed_nodes_query(processed_df, ephemeral=None)
    assert 'deleted' in non_deleted_df.columns
    assert (~non_deleted_df['deleted']).all(), 'Non-deleted query should only return non-deleted hosts'
    assert len(non_deleted_df) == 7, 'Expected 7 non-deleted hosts'
    assert 'localhost' in non_deleted_df['hostname'].values
    assert 'localhost-duplicate' not in non_deleted_df['hostname'].values

    ephemeral_df = report_instance.df_managed_nodes_query(processed_df, ephemeral=True)
    assert len(ephemeral_df) == 3, f'Expected 3 ephemeral hosts, got {len(ephemeral_df)}'
    assert 'ephemeral-dev-short-life' in ephemeral_df['hostname'].values
    assert 'boundary-days-ephemeral' in ephemeral_df['hostname'].values
    assert 'boundary-date-ephemeral' in ephemeral_df['hostname'].values
    assert 'stable-prod-long-life' not in ephemeral_df['hostname'].values
    assert (~ephemeral_df['deleted']).all(), 'Ephemeral query should only return non-deleted hosts'

    non_ephemeral_df = report_instance.df_managed_nodes_query(processed_df, ephemeral=False)
    assert len(non_ephemeral_df) == 4, f'Expected 4 non-ephemeral hosts, got {len(non_ephemeral_df)}'
    assert 'localhost' in non_ephemeral_df['hostname'].values
    assert 'stable-prod-long-life' in non_ephemeral_df['hostname'].values
    assert 'ephemeral-dev-short-life' not in non_ephemeral_df['hostname'].values
    assert (~non_ephemeral_df['deleted']).all(), 'Non-ephemeral query should only return non-deleted hosts'

    # --- Test df_deleted_managed_nodes_query ---
    deleted_df = report_instance.df_deleted_managed_nodes_query(processed_df)
    assert (deleted_df['deleted']).all(), 'Deleted query should only return deleted hosts'
    assert len(deleted_df) == 3, f'Expected 3 deleted hosts, got {len(deleted_df)}'
    assert 'localhost-duplicate' in deleted_df['hostname'].values
    assert 'long-lived-deleted' in deleted_df['hostname'].values
    assert 'short-lived-deleted' in deleted_df['hostname'].values
    assert 'localhost' not in deleted_df['hostname'].values


def test_renewal_guidance_queries_with_empty_data(fixed_now):
    """
    Tests df_managed_nodes_query and df_deleted_managed_nodes_query functions when
    the underlying data is empty.
    """
    patch_target = 'metrics_utility.automation_controller_billing.report.report_renewal_guidance.datetime'

    with patch(patch_target) as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_now
        mock_datetime_module.datetime.utcnow.return_value = fixed_now.replace(tzinfo=None)
        mock_datetime_module.timedelta = dt_actual.timedelta
        mock_datetime_module.timezone = MagicMock(spec=dt_actual.timezone)
        mock_datetime_module.timezone.utc = dt_actual.timezone.utc

        test_ephemeral_days = 30
        report_period_range = '2025-01-01,2025-06-03'
        test_extra_params = {
            'opt_ephemeral': f'{test_ephemeral_days} days',
            'price_per_node': 0.1,
            'report_period_range': report_period_range,
            'since_date': '2025-01-01',
            'until_date': '2025-06-03',
        }

        empty_df_with_cols = generate_renewal_guidance_dataframe(is_empty=True)
        # Explicitly cast relevant columns to dtypes, to ensure Pandas retains schema
        empty_df_with_cols = empty_df_with_cols.astype(
            {
                'deleted': bool,
                'days_automated': 'int64',
                'first_automation': 'datetime64[ns]',
                'last_automation': 'datetime64[ns]',
                'last_deleted': 'datetime64[ns]',
            }
        )

        report_renewal_guidance_instance_empty = ReportRenewalGuidance(
            dataframe=[empty_df_with_cols],
            report_period=report_period_range,
            extra_params=test_extra_params,
        )

        # Test df_managed_nodes_query with empty input
        result_non_deleted_empty = report_renewal_guidance_instance_empty.df_managed_nodes_query(
            empty_df_with_cols,
            ephemeral=None,
        )
        assert result_non_deleted_empty.empty
        assert list(result_non_deleted_empty.columns) == list(empty_df_with_cols.columns)

        result_ephemeral_empty = report_renewal_guidance_instance_empty.df_managed_nodes_query(
            empty_df_with_cols,
            ephemeral=True,
        )
        assert result_ephemeral_empty.empty
        assert list(result_ephemeral_empty.columns) == list(empty_df_with_cols.columns)

        # Test df_deleted_managed_nodes_query with empty input
        result_deleted_empty = report_renewal_guidance_instance_empty.df_deleted_managed_nodes_query(
            empty_df_with_cols,
        )
        assert result_deleted_empty.empty
        assert list(result_deleted_empty.columns) == list(empty_df_with_cols.columns)
