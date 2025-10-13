# import all the data definitions from other test files
# from events_modules, execution_environments, jobhostsummary and jobs

# import events

# make sure we are cleaning any file in the out directory

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
    events_modules = EventModulesAnonymizedRollups.base(events_df)
    execution_environments = ExecutionEnvironmentsAnonymizedRollups.base(execution_environments_df)
    jobhostsummary = JobHostSummaryAnonymizedRollup.base(jobhostsummary_df)
    jobs = JobsAnonymizedRollups.base(jobs_df)

    # read json
    events_modules_json = events_modules['json']
    execution_environments_json = execution_environments['json']
    jobhostsummary_json = jobhostsummary['json']
    jobs_json = jobs['json']

    # read the 'rollup' field from the anonymized rollups
    events_modules_rollup = events_modules['rollup']
    execution_environments_rollup = execution_environments['rollup']
    jobhostsummary_rollup = jobhostsummary['rollup']
    jobs_rollup = jobs['rollup']

    # save the rollups
    save_rollup(events_modules_rollup, 'events_modules', './out', '2024', 1, 1)
    save_rollup(execution_environments_rollup, 'execution_environments', './out', '2024', 1, 1)
    save_rollup(jobhostsummary_rollup, 'jobhostsummary', './out', '2024', 1, 1)
    save_rollup(jobs_rollup, 'jobs', './out', '2024', 1, 1)

    # create unified json from the partial json results
    unified_json = {
        'events_modules': events_modules,
        'execution_environments': execution_environments_rollup,
        'jobhostsummary': jobhostsummary_rollup,
        'jobs': jobs_rollup,
    }
    with open('./out/rollups/unified.json', 'w') as f:

    # assert the files are created

    # events_modules
    """
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/list_of_modules_used_to_automate.json')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/modules_used_to_automate_total.json')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/avg_number_of_modules_used_in_a_playbooks.json')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/modules_used_per_playbook_total.json')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/module_stats.csv')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/collection_stats.csv')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/job_time_stats.csv')
    assert os.path.exists('./out/rollups/events_modules/2024/1/1/total_hosts_automated.json')

    # execution_environments
    assert os.path.exists('./out/rollups/execution_environments/2024/1/1/execution_environments.csv')
    assert os.path.exists('./out/rollups/execution_environments/2024/1/1/execution_environments_usage.csv')

    # jobhostsummary
    assert os.path.exists('./out/rollups/jobhostsummary/2024/1/1/jobhostsummary.csv')
    assert os.path.exists('./out/rollups/jobhostsummary/2024/1/1/jobhostsummary_usage.csv')

    # jobs
    assert os.path.exists('./out/rollups/jobs/2024/1/1/jobs.csv')
    assert os.path.exists('./out/rollups/jobs/2024/1/1/jobs_usage.csv')
    """
