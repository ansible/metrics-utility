import datetime as dt_actual

from unittest.mock import MagicMock

import pandas as pd
import pytest

from metrics_utility import prepare
from metrics_utility.automation_controller_billing.dataframe_engine.db_dataframe_host_metric import DBDataframeHostMetric
from metrics_utility.test.util import generate_renewal_guidance_dataframe


# this updates python paths to include mock_awx/ (and not fail to find awx imports)
# has to happen before any tests that import code that imports awx are imported
prepare()


@pytest.fixture
def fixed_now():
    """
    Provides a fixed, timezone-aware datetime for deterministic tests.
    """
    return dt_actual.datetime(2025, 6, 3, 10, 0, 0, tzinfo=dt_actual.timezone.utc)


@pytest.fixture
def setup_processed_dataframe(fixed_now):
    """
    Sets up a processed Pandas DataFrame ready for use by report methods.
    It mocks the DBDataframeHostMetric's data extraction to provide consistent data.
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

    # Assertions to confirm the generated DataFrame is as expected
    assert processed_df is not None, 'Fixture: build_dataframe should return a DataFrame'
    assert not processed_df.empty, 'Fixture: DataFrame should not be empty'
    assert len(processed_df) == len(mock_full_dataframe_raw), 'Fixture: DataFrame should contain all mock rows'
    assert pd.api.types.is_datetime64_any_dtype(processed_df['first_automation'])
    assert pd.api.types.is_datetime64_any_dtype(processed_df['last_automation'])
    assert pd.api.types.is_datetime64_any_dtype(processed_df['last_deleted'])

    yield processed_df
