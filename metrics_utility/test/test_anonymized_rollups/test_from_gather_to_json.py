import csv
import glob
import os

from datetime import datetime

import pytest


from metrics_utility.test.util import run_gather_ext

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data

env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './out',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_OPTIONAL_COLLECTORS' : 'main_jobevent_service,execution_environments,unified_jobs,job_host_summary_service',
    'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR' : 'true',
}

@pytest.fixture
def cleanup_glob():
    for file in glob.glob(file_glob):
        os.remove(file)
    yield
    #for file in glob.glob(file_glob):
    #    os.remove(file)

# where to find the tar.gz (match jobhostsummary test layout)
uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./out/*/{uuid}-*.tar.gz'
file_paths = f'./out/data/2025/06/13/{uuid}-*.tar.gz'



def test_from_gather_to_json(cleanup_glob):
    # run gather
    run_gather_ext(env_vars, ['--ship', '--force', '--since=2025-06-13', '--until=2025-06-14'])

    # load data for each collector
    compute_anonymized_rollup_from_raw_data('salt', 2025, 6, 13)
    

  
  
