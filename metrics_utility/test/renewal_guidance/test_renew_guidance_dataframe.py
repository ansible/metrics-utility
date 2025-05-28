import random
import uuid

from datetime import datetime, timezone

import pandas as pd


def generate_renewal_guidance_dataframe(is_empty=False):
    """
    Generates a pandas DataFrame with specific, hardcoded mock renewal guidance report data for predefined test scenarios,
    or an empty DataFrame with defined columns.

        Args:
            is_empty (bool): If True, returns an empty DataFrame with correct columns.
                             Otherwise, returns the predefined 4 rows.
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
    ]

    if is_empty:
        return pd.DataFrame(columns=column_names)

    # Helper to generate common datetime objects
    first_auto_dt = datetime(2025, 5, 7, 15, 40, 6, tzinfo=timezone.utc)
    last_auto_dt = datetime(2025, 5, 22, 19, 45, 11, tzinfo=timezone.utc)
    # last_deleted_string: This is what should be fed into the DataFrame for 'last_deleted'
    last_deleted_string_example = (datetime(2025, 5, 20, 10, 0, 0, tzinfo=timezone.utc)).isoformat(timespec='seconds')

    # --- Row 1 Data ---
    row1 = {
        'hostname': 'localhost',
        'host_id': 1001,
        'first_automation': first_auto_dt.isoformat(timespec='seconds'),  # Keep as ISO8601 string
        'last_automation': last_auto_dt.isoformat(timespec='seconds'),  # Keep as ISO8601 string
        'automated_counter': 731,
        'deleted_counter': 0,
        'last_deleted': None,  # This is correct for None
        'deleted': False,
        'ansible_product_serial': 'SN' + str(random.randint(1000000000, 9999999999)),
        'ansible_machine_id': str(uuid.uuid4()),
        'ansible_host_variable': 'localhost',
        'ansible_connection_variable': 'ssh',
    }

    # --- Row 2 Data ---
    row2 = {
        'hostname': 'localhost2',
        'host_id': 1002,
        'first_automation': first_auto_dt.isoformat(timespec='seconds'),
        'last_automation': last_auto_dt.isoformat(timespec='seconds'),
        'automated_counter': 730,
        'deleted_counter': 0,
        'last_deleted': None,
        'deleted': False,
        'ansible_product_serial': 'SN' + str(random.randint(1000000000, 9999999999)),
        'ansible_machine_id': str(uuid.uuid4()),
        'ansible_host_variable': 'localhost2',
        'ansible_connection_variable': 'ssh',
    }

    # --- Row 3 Data ---
    row3 = {
        'hostname': 'localhost3',
        'host_id': 1003,
        'first_automation': first_auto_dt.isoformat(timespec='seconds'),
        'last_automation': last_auto_dt.isoformat(timespec='seconds'),
        'automated_counter': 730,
        'deleted_counter': 0,
        'last_deleted': None,
        'deleted': False,
        'ansible_product_serial': 'SN' + str(random.randint(1000000000, 9999999999)),
        'ansible_machine_id': str(uuid.uuid4()),
        'ansible_host_variable': 'localhost3',
        'ansible_connection_variable': 'winrm',
    }

    # --- Row 4 Data (with potential for deletion) ---
    row4 = {
        'hostname': 'localhost-duplicate',
        'host_id': 1004,
        'first_automation': first_auto_dt.isoformat(timespec='seconds'),
        'last_automation': last_auto_dt.isoformat(timespec='seconds'),
        'automated_counter': 730,
        'deleted_counter': 1,
        'last_deleted': last_deleted_string_example,  # Now an ISO8601 string
        'deleted': True,
        'ansible_product_serial': 'SN' + str(random.randint(1000000000, 9999999999)),
        'ansible_machine_id': str(uuid.uuid4()),
        'ansible_host_variable': 'localhost-duplicate',
        'ansible_connection_variable': 'ssh',
    }

    return pd.DataFrame([row1, row2, row3, row4])


# Example usage:
# WITH DATA
# df_with_data = generate_renewal_guidance_dataframe(is_empty=False)
# print(df_with_data)
#
# EMPTY
# empty_df = generate_renewal_guidance_dataframe(is_empty=True)
# print("\nEmpty DataFrame (is_empty=True):\n", empty_df)

#
# You can check if it's empty and see its columns:
# print("\nIs empty_df empty?", empty_df.empty)
# print("Columns of empty_df:", empty_df.columns.tolist())
