# import all the data definitions from other test files
# from events_modules, execution_environments, jobhostsummary and jobs

# import events

# make sure we are cleaning any file in the out directory

import json

import pandas as pd
import pytest

# import base from anonymized rollups
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollups
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollups
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollups

# import save_rollup
from metrics_utility.anonymized_rollups.save_rollup import save_rollup
from metrics_utility.test.test_anonymized_rollups.test_events_modules_anonymized_rollups import events
from metrics_utility.test.test_anonymized_rollups.test_execution_environments_anonymized_rollups import execution_environments
from metrics_utility.test.test_anonymized_rollups.test_jobhostsummary_anonymized_rollups import jobhostsummary
from metrics_utility.test.test_anonymized_rollups.test_jobs_anonymized_rollups import jobs


file_glob = './out/rollups/*/*/*/*/*'


@pytest.fixture
def cleanup_glob():
    yield
    # for file in glob.glob(file_glob):
    #    os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_save_rollup(cleanup_glob):
    # construct dataframes
    events_df = pd.DataFrame(events)
    execution_environments_df = pd.DataFrame(execution_environments)
    jobhostsummary_df = pd.DataFrame(jobhostsummary)
    jobs_df = pd.DataFrame(jobs)

    # TODO - Saving EE does not work yet

    # call the anonymized rollups
    events_modules_result = EventModulesAnonymizedRollups.base(events_df)
    execution_environments_result = ExecutionEnvironmentsAnonymizedRollups.base(execution_environments_df)
    jobhostsummary_result = JobHostSummaryAnonymizedRollup.base(jobhostsummary_df)
    jobs_result = JobsAnonymizedRollups.base(jobs_df)

    # read json
    events_modules_json = events_modules_result['json']
    execution_environments_json = execution_environments_result['json']
    jobhostsummary_json = jobhostsummary_result['json']
    jobs_json = jobs_result['json']

    # read the 'rollup' field from the anonymized rollups
    events_modules_rollup = events_modules_result['rollup']
    execution_environments_rollup = execution_environments_result['rollup']
    jobhostsummary_rollup = jobhostsummary_result['rollup']
    jobs_rollup = jobs_result['rollup']

    # save the rollups
    save_rollup(events_modules_rollup, 'events_modules', './out', '2024', 1, 1)
    save_rollup(execution_environments_rollup, 'execution_environments', './out', '2024', 1, 1)
    save_rollup(jobhostsummary_rollup, 'jobhostsummary', './out', '2024', 1, 1)
    save_rollup(jobs_rollup, 'jobs', './out', '2024', 1, 1)

    # create unified json from the partial json results
    unified_json = {
        'events_modules': events_modules_json,
        'execution_environments': execution_environments_json,
        'jobhostsummary': jobhostsummary_json,
        'jobs': jobs_json,
    }

    # path is year/month/day/unified.json
    with open('./out/rollups/2024/1/1/unified.json', 'w') as f:
        # pretty json
        json.dump(unified_json, f, indent=4)
