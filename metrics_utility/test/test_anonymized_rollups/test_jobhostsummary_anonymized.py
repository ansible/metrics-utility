import pandas as pd

from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummary_Anonymized_Rollup


data = [
    # job_template T1, job_id 1001, 3 tasks per job, 5 hosts
    # number of tasks = 3
    # total tasks = 3 * 5 = 15
    [0, 0, 3, 0, 0, 0, 'h1', 1001, 'T1'],
    [0, 1, 2, 0, 0, 0, 'h2', 1001, 'T1'],  # 1 failure
    [0, 0, 3, 0, 0, 0, 'h3', 1001, 'T1'],
    [0, 0, 2, 1, 0, 0, 'h4', 1001, 'T1'],  # 1 skipped
    [0, 0, 3, 0, 0, 0, 'h5', 1001, 'T1'],
    # job_template T1, job_id 1002, one host skips a task, another fails
    # number of tasks = 3
    # total tasks = 3 * 5 = 15
    [0, 0, 3, 0, 0, 0, 'h1', 1002, 'T1'],
    [0, 1, 2, 0, 0, 0, 'h2', 1002, 'T1'],  # 1 failure
    [0, 0, 2, 1, 0, 0, 'h3', 1002, 'T1'],  # 1 skipped
    [0, 0, 3, 0, 0, 0, 'h4', 1002, 'T1'],
    [0, 0, 3, 0, 0, 0, 'h5', 1002, 'T1'],
    # job_template T2, job_id 2001, 5 tasks per job, 3 hosts
    # number of tasks = 5
    # total tasks = 5 * 3 = 15
    [0, 0, 5, 0, 0, 0, 'h1', 2001, 'T2'],
    [0, 1, 4, 0, 0, 0, 'h2', 2001, 'T2'],  # 1 failure
    [0, 0, 5, 0, 0, 0, 'h3', 2001, 'T2'],
    # job_template T2, job_id 2002, one host executes only 4 tasks, another fails
    # number of tasks = 5
    # total tasks = 5 * 3 = 15
    [0, 0, 5, 0, 0, 0, 'h1', 2002, 'T2'],
    [0, 2, 3, 0, 0, 0, 'h2', 2002, 'T2'],  # 2 failures
    [0, 1, 4, 0, 0, 0, 'h3', 2002, 'T2'],
]


def test_jobhostsummary_anonymized():
    df = pd.DataFrame(data, columns=['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued', 'host_name', 'job_id', 'job_template_name'])

    result = JobHostSummary_Anonymized_Rollup.base(df)

    print(result)

    # result should be a list of dicts, one per template
    # convert to mapping for easy assertions
    by_template = {item['job_template_name']: item for item in result}

    assert set(by_template.keys()) == {'T1', 'T2'}

    assert by_template['T1']['total_jobs'] == 2
    assert by_template['T2']['total_jobs'] == 2

    assert by_template['T1']['total_dark'] == 0
    assert by_template['T2']['total_dark'] == 0

    assert by_template['T1']['total_failures'] == 2
    assert by_template['T2']['total_failures'] == 4

    assert by_template['T1']['total_ok'] == 26
    assert by_template['T2']['total_ok'] == 26

    assert by_template['T1']['total_skipped'] == 2
    assert by_template['T2']['total_skipped'] == 0

    assert by_template['T1']['total_ignored'] == 0
    assert by_template['T2']['total_ignored'] == 0

    assert by_template['T1']['total_rescued'] == 0
    assert by_template['T2']['total_rescued'] == 0

    assert by_template['T1']['average_tasks_executed'] == 3.0
    assert by_template['T2']['average_tasks_executed'] == 5.0
