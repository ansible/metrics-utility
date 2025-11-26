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

    # result should be a dict with 'aggregated' (list) and 'unique_hosts_total' (int)
    assert 'aggregated' in result, 'result should have aggregated key'
    assert 'unique_hosts_total' in result, 'result should have unique_hosts_total key'
    assert result['unique_hosts_total'] == 5, 'Should have 5 unique hosts (h1, h2, h3, h4, h5)'

    # convert to mapping for easy assertions
    by_template = {item['job_template_name']: item for item in result['aggregated']}

    assert set(by_template.keys()) == {'T1', 'T2'}

    assert by_template['T1']['dark_total'] == 0
    assert by_template['T2']['dark_total'] == 0

    assert by_template['T1']['failures_total'] == 2
    assert by_template['T2']['failures_total'] == 4

    assert by_template['T1']['ok_total'] == 26
    assert by_template['T2']['ok_total'] == 26

    assert by_template['T1']['skipped_total'] == 2
    assert by_template['T2']['skipped_total'] == 0

    assert by_template['T1']['ignored_total'] == 0
    assert by_template['T2']['ignored_total'] == 0

    assert by_template['T1']['rescued_total'] == 0
    assert by_template['T2']['rescued_total'] == 0
