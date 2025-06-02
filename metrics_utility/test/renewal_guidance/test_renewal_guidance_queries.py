import random
import uuid

from datetime import datetime, timedelta, timezone

import pandas as pd

from metrics_utility.automation_controller_billing.helpers import parse_number_of_days


def generate_renewal_guidance_dataframe(is_empty=False):
    """
    Generates a pandas DataFrame with specific, hardcoded mock renewal guidance
    report data for predefined test scenarios, or an empty DataFrame with defined columns.

    Args:
        is_empty (bool): If True, returns an empty DataFrame with correct columns.
                         Otherwise, returns the predefined rows.
    """
    column_names = [
        'host_id',
        'hostname',
        'first_automation',
        'last_automation',
        'automated_counter',
        'deleted_counter',
        'last_deleted',
        'deleted',
        'ansible_product_serial',
        'ansible_machine_id',
        'ansible_host_variable',
        'ansible_connection_variable',
        'days_automated',
    ]

    if is_empty:
        return pd.DataFrame(columns=column_names)

    # --- Helper to generate common datetime objects for consistency ---
    now_utc = datetime.now(timezone.utc).replace(microsecond=0)

    deleted_date_example_dt = now_utc - timedelta(days=5)

    rows = []

    # Row 1: localhost (Non-deleted, Non-ephemeral)
    first_auto_dt_1 = datetime(2025, 5, 7, 14, 45, 32, tzinfo=timezone.utc)
    last_auto_dt_1 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_1 = 'localhost'
    rows.append(
        {
            'host_id': 1001,
            'hostname': hostname_1,
            'first_automation': first_auto_dt_1.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_1.isoformat(timespec='seconds'),
            'automated_counter': 731,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_1 - first_auto_dt_1).days,
        }
    )

    # Row 2: localhost2 (Non-deleted, Non-ephemeral)
    first_auto_dt_2 = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt_2 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_2 = 'localhost2'
    rows.append(
        {
            'host_id': 1002,
            'hostname': hostname_2,
            'first_automation': first_auto_dt_2.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_2.isoformat(timespec='seconds'),
            'automated_counter': 730,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost2',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_2 - first_auto_dt_2).days,
        }
    )

    # Row 3: localhost3 (Non-deleted, Non-ephemeral)
    first_auto_dt_3 = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt_3 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_3 = 'localhost3'
    rows.append(
        {
            'host_id': 1003,
            'hostname': hostname_3,
            'first_automation': first_auto_dt_3.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_3.isoformat(timespec='seconds'),
            'automated_counter': 730,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost3',
            'ansible_connection_variable': 'winrm',
            'days_automated': (last_auto_dt_3 - first_auto_dt_3).days,
        }
    )

    # Row 4: localhost-duplicate (Deleted, Non-ephemeral)
    first_auto_dt_4 = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt_4 = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    hostname_4 = 'localhost-duplicate'
    rows.append(
        {
            'host_id': 1004,
            'hostname': hostname_4,
            'first_automation': first_auto_dt_4.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_4.isoformat(timespec='seconds'),
            'automated_counter': 730,
            'deleted_counter': 1,
            'last_deleted': deleted_date_example_dt,
            'deleted': True,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'localhost-duplicate',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_4 - first_auto_dt_4).days,
        }
    )

    # --- New Rows for Specific Test Scenarios ---

    # Row 5: Clearly Ephemeral (Short-lived)
    hostname_5 = 'ephemeral-dev-short-life'
    first_auto_dt_5 = datetime(2025, 4, 20, 10, 0, 0, tzinfo=timezone.utc)  # 40 days ago
    last_auto_dt_5 = first_auto_dt_5 + timedelta(days=5)  # 5 days automated
    rows.append(
        {
            'host_id': 1005,
            'hostname': hostname_5,
            'first_automation': first_auto_dt_5.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_5.isoformat(timespec='seconds'),
            'automated_counter': 10,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'ephemeral-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_5 - first_auto_dt_5).days,
        }
    )

    # Row 6: Clearly NON-Ephemeral (Long-lived)
    hostname_6 = 'stable-prod-long-life'
    first_auto_dt_6 = datetime(2025, 5, 20, 9, 0, 0, tzinfo=timezone.utc)  # 10 days ago
    last_auto_dt_6 = first_auto_dt_6 + timedelta(days=100)  # 100 days automated
    rows.append(
        {
            'host_id': 1006,
            'hostname': hostname_6,
            'first_automation': first_auto_dt_6.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_6.isoformat(timespec='seconds'),
            'automated_counter': 500,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'stable-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_6 - first_auto_dt_6).days,
        }
    )

    # Row 7: Boundary Ephemeral (days_automated exactly at threshold)
    hostname_7 = 'boundary-days-ephemeral'
    first_auto_dt_7 = datetime(2025, 4, 20, 10, 0, 0, tzinfo=timezone.utc)  # 40 days ago
    last_auto_dt_7 = first_auto_dt_7 + timedelta(days=30)  # 30 days automated
    rows.append(
        {
            'host_id': 1007,
            'hostname': hostname_7,
            'first_automation': first_auto_dt_7.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_7.isoformat(timespec='seconds'),
            'automated_counter': 50,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'ephemeral-boundary-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_7 - first_auto_dt_7).days,
        }
    )

    # Row 8: Boundary Ephemeral (first_automation exactly at threshold date)
    hostname_8 = 'boundary-date-ephemeral'
    first_auto_dt_8 = now_utc - timedelta(days=30)  # Exactly 30 days ago
    last_auto_dt_8 = first_auto_dt_8 + timedelta(days=5)  # 5 days automated
    rows.append(
        {
            'host_id': 1008,
            'hostname': hostname_8,
            'first_automation': first_auto_dt_8.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_8.isoformat(timespec='seconds'),
            'automated_counter': 10,
            'deleted_counter': 0,
            'last_deleted': None,
            'deleted': False,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'ephemeral-boundary-2',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_8 - first_auto_dt_8).days,
        }
    )

    # Row 9: Deleted host, would be non-ephemeral if not deleted
    hostname_9 = 'long-lived-deleted'
    first_auto_dt_9 = now_utc - timedelta(days=100)  # 100 days ago
    last_auto_dt_9 = first_auto_dt_9 + timedelta(days=90)  # 90 days automated
    rows.append(
        {
            'host_id': 1009,
            'hostname': hostname_9,
            'first_automation': first_auto_dt_9.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_9.isoformat(timespec='seconds'),
            'automated_counter': 200,
            'deleted_counter': 1,
            'last_deleted': deleted_date_example_dt,
            'deleted': True,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'deleted-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_9 - first_auto_dt_9).days,
        }
    )

    # Row 10: Deleted host, would be ephemeral if not deleted
    hostname_10 = 'short-lived-deleted'
    first_auto_dt_10 = now_utc - timedelta(days=40)  # 40 days ago
    last_auto_dt_10 = first_auto_dt_10 + timedelta(days=10)  # 10 days automated
    rows.append(
        {
            'host_id': 1010,
            'hostname': hostname_10,
            'first_automation': first_auto_dt_10.isoformat(timespec='seconds'),
            'last_automation': last_auto_dt_10.isoformat(timespec='seconds'),
            'automated_counter': 5,
            'deleted_counter': 1,
            'last_deleted': deleted_date_example_dt,
            'deleted': True,
            'ansible_product_serial': 'SN' + str(random.randint(10**9, 10**10 - 1)),
            'ansible_machine_id': str(uuid.uuid4()),
            'ansible_host_variable': 'deleted-ephemeral-host-1',
            'ansible_connection_variable': 'ssh',
            'days_automated': (last_auto_dt_10 - first_auto_dt_10).days,
        }
    )

    return pd.DataFrame(rows)


# --- Mock class for DBDataframeHostMetric to provide extra_params ---


class MockDBDataframeHostMetricForTest:
    def __init__(self, extra_params=None):
        self.extra_params = extra_params if extra_params is not None else {}

    def df_managed_nodes_query(self, dataframe, ephemeral=None, with_deleted=False):
        if dataframe.empty:
            return pd.DataFrame(columns=dataframe.columns)

        if ephemeral is None:
            return dataframe[~dataframe['deleted']].copy()
        else:
            # Take only non deleted
            if not with_deleted:
                dataframe = dataframe[~dataframe['deleted']].copy()

            ephemeral_days = parse_number_of_days(self.extra_params.get('opt_ephemeral'))

            ephemeral_threshold = pd.to_datetime(
                datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=ephemeral_days - 1), utc=True
            ).replace(second=59, microsecond=999999)

            if ephemeral is True:
                return dataframe[(dataframe['days_automated'] <= ephemeral_days) & (dataframe['first_automation'] <= ephemeral_threshold)].copy()
            if ephemeral is False:
                return dataframe[(dataframe['days_automated'] > ephemeral_days) | (dataframe['first_automation'] > ephemeral_threshold)].copy()

        return dataframe.copy()

    def df_deleted_managed_nodes_query(self, dataframe):
        if dataframe.empty:
            return pd.DataFrame(columns=dataframe.columns)

        return dataframe[dataframe['deleted']].copy()


# Test Scenario 1: df_managed_nodes_query - Default (ephemeral=None, no deleted)
def test_managed_nodes_query_default_filter():
    mock_dataframe_full = generate_renewal_guidance_dataframe(is_empty=False)

    mock_dataframe_full['first_automation'] = pd.to_datetime(mock_dataframe_full['first_automation'], format='ISO8601', utc=True)
    mock_dataframe_full['last_automation'] = pd.to_datetime(mock_dataframe_full['last_automation'], format='ISO8601', utc=True)
    mock_dataframe_full['last_deleted'] = pd.to_datetime(mock_dataframe_full['last_deleted'], format='ISO8601', utc=True, errors='coerce')

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={})

    result_df = mock_instance.df_managed_nodes_query(mock_dataframe_full, ephemeral=None, with_deleted=False)

    expected_host_ids = [1001, 1002, 1003, 1005, 1006, 1007, 1008]
    assert sorted(result_df['host_id'].tolist()) == sorted(expected_host_ids)
    assert not result_df['deleted'].any()  # Ensure no 'deleted' hosts are present


# Test Scenario 2: df_deleted_managed_nodes_query
def test_deleted_managed_nodes_query():
    mock_dataframe_full = generate_renewal_guidance_dataframe(is_empty=False)
    mock_dataframe_full['first_automation'] = pd.to_datetime(mock_dataframe_full['first_automation'], format='ISO8601', utc=True)
    mock_dataframe_full['last_automation'] = pd.to_datetime(mock_dataframe_full['last_automation'], format='ISO8601', utc=True)
    mock_dataframe_full['last_deleted'] = pd.to_datetime(mock_dataframe_full['last_deleted'], format='ISO8601', utc=True, errors='coerce')

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={})

    result_df = mock_instance.df_deleted_managed_nodes_query(mock_dataframe_full)

    # Expected: Only deleted hosts (host_id 1004, 1009, 1010)
    expected_host_ids = [1004, 1009, 1010]
    assert sorted(result_df['host_id'].tolist()) == sorted(expected_host_ids)
    assert result_df['deleted'].all()  # Ensure all hosts in result are 'deleted'


# Test Scenario 3: df_managed_nodes_query - Empty DataFrame input
def test_managed_nodes_query_empty_dataframe():
    empty_df = generate_renewal_guidance_dataframe(is_empty=True)

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={})

    result_df = mock_instance.df_managed_nodes_query(empty_df, ephemeral=None, with_deleted=False)

    # Expected: An empty DataFrame with the same columns as the input.
    pd.testing.assert_frame_equal(result_df, empty_df)
    assert result_df.empty
    assert list(result_df.columns) == list(empty_df.columns)  # Ensure columns are preserved


# Test Scenario 4: df_deleted_managed_nodes_query - Empty DataFrame input
def test_deleted_managed_nodes_query_empty_dataframe():
    empty_df = generate_renewal_guidance_dataframe(is_empty=True)
    mock_instance = MockDBDataframeHostMetricForTest(extra_params={})

    result_df = mock_instance.df_deleted_managed_nodes_query(empty_df)

    # Expected: An empty DataFrame with the same columns as the input.
    pd.testing.assert_frame_equal(result_df, empty_df)
    assert result_df.empty
    assert list(result_df.columns) == list(empty_df.columns)


# --- Test Functions for Ephemeral Logic ---


# Helper to prepare the DataFrame with correct dtypes for testing
def _prepare_dataframe_for_query(df):
    df['first_automation'] = pd.to_datetime(df['first_automation'], format='ISO8601', utc=True)
    df['last_automation'] = pd.to_datetime(df['last_automation'], format='ISO8601', utc=True)
    df['last_deleted'] = pd.to_datetime(df['last_deleted'], format='ISO8601', utc=True, errors='coerce')
    return df


# Test Scenario 5: df_managed_nodes_query - ephemeral=True, with_deleted=False (Default behavior)
def test_managed_nodes_query_ephemeral_true_no_deleted():
    mock_dataframe_full = generate_renewal_guidance_dataframe(is_empty=False)
    mock_dataframe_full = _prepare_dataframe_for_query(mock_dataframe_full)

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={'opt_ephemeral': '30d'})

    result_df = mock_instance.df_managed_nodes_query(mock_dataframe_full, ephemeral=True, with_deleted=False)
    # Expected: Ephemeral hosts, excluding deleted ones.
    # (days_automated <= 30 AND first_automation <= now-30d):
    # 1005 (ephemeral-dev-short-life)
    # 1007 (boundary-days-ephemeral)
    # 1008 (boundary-date-ephemeral)
    # 1010 (short-lived-deleted - but this one IS deleted, so it should be excluded by with_deleted=False)
    expected_host_ids = [1005, 1007, 1008]
    assert sorted(result_df['host_id'].tolist()) == sorted(expected_host_ids)
    assert not result_df['deleted'].any()  # Ensure no 'deleted' hosts are present


# Test Scenario 6: df_managed_nodes_query - ephemeral=True, with_deleted=True
def test_managed_nodes_query_ephemeral_true_with_deleted():
    mock_dataframe_full = generate_renewal_guidance_dataframe(is_empty=False)
    mock_dataframe_full = _prepare_dataframe_for_query(mock_dataframe_full)

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={'opt_ephemeral': '30d'})

    result_df = mock_instance.df_managed_nodes_query(mock_dataframe_full, ephemeral=True, with_deleted=True)

    # Expected: Ephemeral hosts, INCLUDING deleted ones.
    # 1005, 1007, 1008 are ephemeral and not deleted.
    # 1010 is ephemeral AND deleted.
    expected_host_ids = [1005, 1007, 1008, 1010]
    assert sorted(result_df['host_id'].tolist()) == sorted(expected_host_ids)
    # Note: 'deleted' column might still contain True for host 1010


# Test Scenario 7: df_managed_nodes_query - ephemeral=False, with_deleted=False (Default behavior)
def test_managed_nodes_query_ephemeral_false_no_deleted():
    mock_dataframe_full = generate_renewal_guidance_dataframe(is_empty=False)
    mock_dataframe_full = _prepare_dataframe_for_query(mock_dataframe_full)

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={'opt_ephemeral': '30d'})

    result_df = mock_instance.df_managed_nodes_query(mock_dataframe_full, ephemeral=False, with_deleted=False)

    # Expected: NON-ephemeral hosts, excluding deleted ones.
    # Non-ephemeral hosts (from mock data comments that are NOT deleted):
    # 1001, 1002, 1003, 1006
    expected_host_ids = [1001, 1002, 1003, 1006]
    assert sorted(result_df['host_id'].tolist()) == sorted(expected_host_ids)
    assert not result_df['deleted'].any()  # Ensure no 'deleted' hosts are present


# Test Scenario 8: df_managed_nodes_query - ephemeral=False, with_deleted=True
def test_managed_nodes_query_ephemeral_false_with_deleted():
    mock_dataframe_full = generate_renewal_guidance_dataframe(is_empty=False)
    mock_dataframe_full = _prepare_dataframe_for_query(mock_dataframe_full)

    mock_instance = MockDBDataframeHostMetricForTest(extra_params={'opt_ephemeral': '30d'})

    result_df = mock_instance.df_managed_nodes_query(mock_dataframe_full, ephemeral=False, with_deleted=True)

    # Expected: NON-ephemeral hosts, INCLUDING deleted ones.
    # 1001, 1002, 1003, 1006, 1004 (deleted, non-ephemeral), 1009 (deleted, non-ephemeral)
    expected_host_ids = [1001, 1002, 1003, 1004, 1006, 1009]
    assert sorted(result_df['host_id'].tolist()) == sorted(expected_host_ids)
    # Note: 'deleted' column might still contain True for hosts 1004, 1009
