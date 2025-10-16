import glob
import json
import os

import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.util import run_gather_ext


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './out',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_jobevent_service,execution_environments,unified_jobs,job_host_summary_service',
    'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true',
}


@pytest.fixture
def cleanup_glob():
    for file in glob.glob(file_glob):
        os.remove(file)
    yield
    # for file in glob.glob(file_glob):
    #    os.remove(file)


# where to find the tar.gz (match jobhostsummary test layout)
uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./out/*/{uuid}-*.tar.gz'
file_paths = f'./out/data/2025/06/13/{uuid}-*.tar.gz'


def test_empty_data(cleanup_glob):
    compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13, True)


def test_from_gather_to_json(cleanup_glob):
    # run gather
    run_gather_ext(env_vars, ['--ship', '--force', '--since=2025-06-13', '--until=2025-06-14'])

    # load data for each collector
    rollup = compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13, True)

    # save as json inside rollups/2025/06/13/anonymized.json
    with open('./out/rollups/2025/06/13/anonymized.json', 'w') as f:
        json.dump(rollup, f, indent=4)
