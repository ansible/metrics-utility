import json
import os
import shutil

import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.anonymized_rollups.task_anonymized_rollups import task_anonymized_rollups


# where to find the tar.gz (match jobhostsummary test layout)


@pytest.fixture
def cleanup_glob():
    out_dir = './out'

    # --- Cleanup before test ---
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    yield  # Run your test

    # --- Cleanup after test ---
    # if os.path.exists(out_dir):
    #    shutil.rmtree(out_dir)


def test_empty_data(cleanup_glob):
    compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13)


def test_from_gather_to_json(cleanup_glob):
    # run gather
    json_data = task_anonymized_rollups('salt', 2025, 6, 13, './out')

    print(json_data)

    # save as json inside rollups/2025/06/13/anonymized.json
    with open(f'./out/rollups/{2025}/06/13/anonymized.json', 'w') as f:
        json.dump(json_data, f, indent=4)
