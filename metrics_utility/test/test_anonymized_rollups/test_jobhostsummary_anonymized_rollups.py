import pandas as pd

from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup


jobhostsummary = [
    # job_template T1, job_id 1001, 3 tasks per job, 5 hosts
    # number of tasks = 3
    # total tasks = 3 * 5 = 15
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
    },
    {
        'dark': 0,
        'failures': 1,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h2',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
    },  # 1 failure
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h3',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
    },
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 1,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h4',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
    },  # 1 skipped
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h5',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
    },
    # job_template T1, job_id 1002, one host skips a task, another fails
    # number of tasks = 3
    # total tasks = 3 * 5 = 15
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'job_remote_id': 1002,
        'job_template_name': 'T1',
    },
    {
        'dark': 0,
        'failures': 1,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h2',
        'job_remote_id': 1002,
        'job_template_name': 'T1',
    },  # 1 failure
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 1,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h3',
        'job_remote_id': 1002,
        'job_template_name': 'T1',
    },  # 1 skipped
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h4',
        'job_remote_id': 1002,
        'job_template_name': 'T1',
    },
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h5',
        'job_remote_id': 1002,
        'job_template_name': 'T1',
    },
    # job_template T2, job_id 2001, 5 tasks per job, 3 hosts
    # number of tasks = 5
    # total tasks = 5 * 3 = 15
    {
        'dark': 0,
        'failures': 0,
        'ok': 5,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'job_remote_id': 2001,
        'job_template_name': 'T2',
    },
    {
        'dark': 0,
        'failures': 1,
        'ok': 4,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h2',
        'job_remote_id': 2001,
        'job_template_name': 'T2',
    },  # 1 failure
    {
        'dark': 0,
        'failures': 0,
        'ok': 5,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h3',
        'job_remote_id': 2001,
        'job_template_name': 'T2',
    },
    # job_template T2, job_id 2002, one host executes only 4 tasks, another fails
    # number of tasks = 5
    # total tasks = 5 * 3 = 15
    {
        'dark': 0,
        'failures': 0,
        'ok': 5,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'job_remote_id': 2002,
        'job_template_name': 'T2',
    },
    {
        'dark': 0,
        'failures': 2,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h2',
        'job_remote_id': 2002,
        'job_template_name': 'T2',
    },  # 2 failures
    {
        'dark': 0,
        'failures': 1,
        'ok': 4,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h3',
        'job_remote_id': 2002,
        'job_template_name': 'T2',
    },
]


def test_jobhostsummary_anonymized():
    df = pd.DataFrame(jobhostsummary)

    jobhostsummary_anonymized_rollup = JobHostSummaryAnonymizedRollup()
    df = jobhostsummary_anonymized_rollup.prepare(df)
    result = jobhostsummary_anonymized_rollup.base(df)
    result = result['json']

    print(result)

    # result should be a dict with 'aggregated' (dict) and 'unique_hosts_total' (int)
    assert 'aggregated' in result, 'result should have aggregated key'
    assert 'unique_hosts_total' in result, 'result should have unique_hosts_total key'
    assert result['unique_hosts_total'] == 5, 'Should have 5 unique hosts (h1, h2, h3, h4, h5)'

    # aggregated should be a single dict (not a list)
    assert isinstance(result['aggregated'], dict), 'aggregated should be a dict, not a list'
    
    totals = result['aggregated']
    
    # Should not have job_template_name field
    assert 'job_template_name' not in totals, 'Should not have job_template_name field'
    
    # Verify totals across all templates
    assert totals['dark_total'] == 0
    assert totals['failures_total'] == 6  # T1: 2 failures, T2: 4 failures
    assert totals['ok_total'] == 52  # T1: 26 ok, T2: 26 ok
    assert totals['skipped_total'] == 2  # T1: 2 skipped, T2: 0 skipped
    assert totals['ignored_total'] == 0
    assert totals['rescued_total'] == 0
