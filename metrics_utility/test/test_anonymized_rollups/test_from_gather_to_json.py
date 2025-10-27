import glob
import os

import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.anonymized_rollups.task_anonymized_rollups import task_anonymized_rollups
import json

# where to find the tar.gz (match jobhostsummary test layout)
uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./out/**/{uuid}-*.tar.gz'
file_paths = f'./out/data/2025/06/13/{uuid}-*.tar.gz'

@pytest.fixture(autouse=True)
def cleanup_glob():
    for file in glob.glob(file_glob, recursive=True):
        os.remove(file)
    yield
    for file in glob.glob(file_glob, recursive=True):
        os.remove(file)





def test_empty_data(cleanup_glob):
    return
    compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13, True)


def test_from_gather_to_json(cleanup_glob):
    return
    # run gather
    json_data = task_anonymized_rollups('salt', 2025, 6, 13, './out')

    print(json_data)

    # save as json inside rollups/2025/06/13/anonymized.json
    with open(f'./out/rollups/{2025}/06/13/anonymized.json', 'w') as f:
        json.dump(json_data, f, indent=4)