import json

from datetime import datetime

import pandas as pd
import pytest

from django.db import connection

from metrics_utility.anonymized_rollups import (
    ControllerVersionAnonymizedRollup,
    CredentialsAnonymizedRollup,
    EventModulesAnonymizedRollup,
    ExecutionEnvironmentsAnonymizedRollup,
    JobHostSummaryAnonymizedRollup,
    JobsAnonymizedRollup,
    TableMetadataAnonymizedRollup,
)
from metrics_utility.anonymized_rollups.helpers import sanitize_json
from metrics_utility.library.collectors.controller import (
    controller_version_service,
    credentials_service,
    execution_environments,
    job_host_summary_service,
    main_jobevent_service,
    table_metadata,
    unified_jobs,
)


def _deep_compare(obj1, obj2, path=""):
    """
    Deep comparison helper that handles lists, dicts, and primitive types.
    Returns tuple (are_equal, error_message).
    """
    # Handle None cases
    if obj1 is None and obj2 is None:
        return True, None
    if obj1 is None or obj2 is None:
        return False, f"Mismatch at {path}: one is None, other is not"

    # Handle type mismatches
    if type(obj1) != type(obj2):
        return False, f"Type mismatch at {path}: {type(obj1).__name__} vs {type(obj2).__name__}"

    # Handle dicts
    if isinstance(obj1, dict):
        keys1 = set(obj1.keys())
        keys2 = set(obj2.keys())
        if keys1 != keys2:
            missing = keys1 - keys2
            extra = keys2 - keys1
            return False, f"Key mismatch at {path}: missing keys {missing}, extra keys {extra}"

        for key in keys1:
            are_equal, error = _deep_compare(obj1[key], obj2[key], f"{path}.{key}" if path else key)
            if not are_equal:
                return False, error
        return True, None

    # Handle lists
    if isinstance(obj1, list):
        if len(obj1) != len(obj2):
            return False, f"List length mismatch at {path}: {len(obj1)} vs {len(obj2)}"

        # For lists of dicts, we need to compare more carefully
        # Sort by a key if possible, or compare element by element
        for i, (item1, item2) in enumerate(zip(obj1, obj2)):
            are_equal, error = _deep_compare(item1, item2, f"{path}[{i}]")
            if not are_equal:
                return False, error
        return True, None

    # Handle primitive types (int, float, str, bool)
    if obj1 != obj2:
        # Special handling for float NaN and infinity
        if isinstance(obj1, float) and isinstance(obj2, float):
            if pd.isna(obj1) and pd.isna(obj2):
                return True, None
            if (pd.isinf(obj1) and pd.isinf(obj2)) and (obj1 > 0) == (obj2 > 0):
                return True, None

        return False, f"Value mismatch at {path}: {obj1} vs {obj2}"

    return True, None


def test_json_serialization_roundtrip(cleanup_glob):
    """
    Test that calling all collectors, then calling rollup prepare,
    serializing to JSON and parsing back results in matching data.

    Similar to test_from_gather_to_json but focuses on JSON serialization
    roundtrip for each collector's prepare output.
    """
    # Time range for data collection
    since = datetime(2025, 6, 13, 0, 0, 0)
    until = datetime(2025, 6, 14, 0, 0, 0)

    db = connection

    # Map collectors to their rollup classes
    collector_rollup_map = [
        ('execution_environments', execution_environments, ExecutionEnvironmentsAnonymizedRollup, {}),
        ('unified_jobs', unified_jobs, JobsAnonymizedRollup, {'since': since, 'until': until}),
        ('job_host_summary_service', job_host_summary_service, JobHostSummaryAnonymizedRollup, {'since': since, 'until': until}),
        ('main_jobevent_service', main_jobevent_service, EventModulesAnonymizedRollup, {'since': since, 'until': until}),
        ('credentials_service', credentials_service, CredentialsAnonymizedRollup, {'since': since, 'until': until}),
        ('table_metadata', table_metadata, TableMetadataAnonymizedRollup, {}),
        ('controller_version_service', controller_version_service, ControllerVersionAnonymizedRollup, {}),
    ]

    for collector_name, collector_func, rollup_class, collector_kwargs in collector_rollup_map:
        print(f'\n{"=" * 70}')
        print(f'Testing: {collector_name}')
        print(f'{"=" * 70}')

        try:
            # Call collector to get data
            collector = collector_func(db=db, **collector_kwargs)
            collected_data = collector.gather()

            # Handle empty data
            if collected_data is None or (isinstance(collected_data, (list, pd.DataFrame)) and len(collected_data) == 0):
                print(f'  No data collected for {collector_name}, skipping...')
                continue

            # Convert list of dataframes to single dataframe if needed
            if isinstance(collected_data, list):
                if len(collected_data) == 0:
                    print(f'  Empty list for {collector_name}, skipping...')
                    continue
                # If list contains dataframes, concatenate them
                if isinstance(collected_data[0], pd.DataFrame):
                    collected_data = pd.concat(collected_data, ignore_index=True)
                else:
                    # If list contains other types, use first element
                    collected_data = collected_data[0]

            # Create rollup instance and call prepare
            rollup = rollup_class()
            prepared_data = rollup.prepare(collected_data)

            # pretty print prepared_data
            print(json.dumps(prepared_data, indent=2))

            # Verify prepare returned a dict or list
            assert isinstance(prepared_data, (dict, list)), (
                f'{collector_name}: prepare() should return dict or list, got {type(prepared_data).__name__}'
            )

            # prepared_data is already sanitized by prepare() method
            # Serialize to JSON
            json_str = json.dumps(prepared_data, default=str, indent=2)

            # Parse JSON back
            parsed_data = json.loads(json_str)

            # Deep compare original and parsed data
            are_equal, error_msg = _deep_compare(prepared_data, parsed_data)
            assert are_equal, (
                f'{collector_name}: JSON roundtrip failed - {error_msg}\n'
                f'Original type: {type(prepared_data).__name__}\n'
                f'Parsed type: {type(parsed_data).__name__}'
            )

            print(f'  ✅ {collector_name}: JSON roundtrip successful')

        except Exception as e:
            print(f'  ❌ {collector_name}: Error during test - {e}')
            raise


@pytest.fixture
def cleanup_glob():
    """Fixture placeholder for consistency with other tests."""
    yield
