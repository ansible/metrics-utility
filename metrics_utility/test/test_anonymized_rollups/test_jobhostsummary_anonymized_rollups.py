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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'manual',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'manual',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'manual',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'manual',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'manual',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'scheduled',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'scheduled',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'scheduled',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'scheduled',
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
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'scheduled',
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
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
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
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
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
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
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
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
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
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
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
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
    },
]


def test_jobhostsummary_anonymized():
    df = pd.DataFrame(jobhostsummary)

    jobhostsummary_anonymized_rollup = JobHostSummaryAnonymizedRollup()
    df = jobhostsummary_anonymized_rollup.prepare(df)
    result = jobhostsummary_anonymized_rollup.base(df)
    result = result['json']

    print(result)

    # result should be a dict with 'by_job_type', 'by_launch_type', 'by_controller_version' (lists) and 'job_host_pairs_total'
    assert 'by_job_type' in result, 'result should have by_job_type key'
    assert isinstance(result['by_job_type'], list), 'by_job_type should be a list'
    assert 'by_launch_type' in result, 'result should have by_launch_type key'
    assert isinstance(result['by_launch_type'], list), 'by_launch_type should be a list'
    assert 'by_controller_version' in result, 'result should have by_controller_version key'
    assert isinstance(result['by_controller_version'], list), 'by_controller_version should be a list'
    assert 'job_host_pairs_total' in result, 'result should have job_host_pairs_total key'
    assert isinstance(result['job_host_pairs_total'], int), 'job_host_pairs_total should be an integer'

    # Verify job_host_pairs_total count: 16 rows total (5+5 for job type, 3+3 for workflowjob type)
    assert result['job_host_pairs_total'] == 16, f'Should have 16 total job host summary records, got {result["job_host_pairs_total"]}'

    # Should have 2 job_type groups: 'job' and 'workflowjob'
    assert len(result['by_job_type']) == 2, 'Should have 2 job_type groups'

    # Find the 'job' type group
    job_type_data = next((j for j in result['by_job_type'] if j['job_type'] == 'job'), None)
    assert job_type_data is not None, 'Should have job_type job'
    assert job_type_data['unique_hosts_total'] == 5, 'Should have 5 unique hosts (h1, h2, h3, h4, h5)'

    # Verify totals for 'job' type (T1: 10 hosts × 3 tasks = 30 tasks, but we have 26 ok + 2 failures + 2 skipped = 30)
    assert job_type_data['dark_total'] == 0
    assert job_type_data['failures_total'] == 2  # T1: 2 failures
    assert job_type_data['ok_total'] == 26  # T1: 26 ok
    assert job_type_data['skipped_total'] == 2  # T1: 2 skipped
    assert job_type_data['ignored_total'] == 0
    assert job_type_data['rescued_total'] == 0

    # Find the 'workflowjob' type group
    workflowjob_type_data = next((j for j in result['by_job_type'] if j['job_type'] == 'workflowjob'), None)
    assert workflowjob_type_data is not None, 'Should have job_type workflowjob'
    assert workflowjob_type_data['unique_hosts_total'] == 3, 'Should have 3 unique hosts (h1, h2, h3)'

    # Verify totals for 'workflowjob' type (T2: 6 hosts × 5 tasks = 30 tasks, but we have 26 ok + 4 failures = 30)
    assert workflowjob_type_data['dark_total'] == 0
    assert workflowjob_type_data['failures_total'] == 4  # T2: 4 failures
    assert workflowjob_type_data['ok_total'] == 26  # T2: 26 ok
    assert workflowjob_type_data['skipped_total'] == 0  # T2: 0 skipped
    assert workflowjob_type_data['ignored_total'] == 0
    assert workflowjob_type_data['rescued_total'] == 0

    # Verify by_launch_type groupings
    # Should have 3 launch_type groups: 'manual', 'scheduled', 'workflow'
    assert len(result['by_launch_type']) == 3, 'Should have 3 launch_type groups'

    # Find the 'manual' launch_type group
    manual_launch_type_data = next((j for j in result['by_launch_type'] if j['launch_type'] == 'manual'), None)
    assert manual_launch_type_data is not None, 'Should have launch_type manual'
    assert manual_launch_type_data['unique_hosts_total'] == 5, 'Should have 5 unique hosts for manual'
    assert manual_launch_type_data['job_type_total'] == 1, 'Should have 1 job type (job) for manual'

    # Find the 'scheduled' launch_type group
    scheduled_launch_type_data = next((j for j in result['by_launch_type'] if j['launch_type'] == 'scheduled'), None)
    assert scheduled_launch_type_data is not None, 'Should have launch_type scheduled'
    assert scheduled_launch_type_data['unique_hosts_total'] == 5, 'Should have 5 unique hosts for scheduled'
    assert scheduled_launch_type_data['job_type_total'] == 1, 'Should have 1 job type (job) for scheduled'

    # Find the 'workflow' launch_type group
    workflow_launch_type_data = next((j for j in result['by_launch_type'] if j['launch_type'] == 'workflow'), None)
    assert workflow_launch_type_data is not None, 'Should have launch_type workflow'
    assert workflow_launch_type_data['unique_hosts_total'] == 3, 'Should have 3 unique hosts for workflow'
    assert workflow_launch_type_data['job_type_total'] == 1, 'Should have 1 job type (workflowjob) for workflow'

    # Verify by_controller_version groupings
    # Should have 2 controller_version groups: '2.9.10' and '2.10.0'
    assert len(result['by_controller_version']) == 2, 'Should have 2 controller_version groups'

    # Find the '2.9.10' controller_version group
    version_2910_data = next((j for j in result['by_controller_version'] if j['controller_version'] == '2.9.10'), None)
    assert version_2910_data is not None, 'Should have controller_version 2.9.10'
    assert version_2910_data['unique_hosts_total'] == 5, 'Should have 5 unique hosts for 2.9.10'
    assert version_2910_data['job_type_total'] == 1, 'Should have 1 job type (job) for 2.9.10'
    assert version_2910_data['launch_type_total'] == 2, 'Should have 2 launch types (manual, scheduled) for 2.9.10'

    # Find the '2.10.0' controller_version group
    version_2100_data = next((j for j in result['by_controller_version'] if j['controller_version'] == '2.10.0'), None)
    assert version_2100_data is not None, 'Should have controller_version 2.10.0'
    assert version_2100_data['unique_hosts_total'] == 3, 'Should have 3 unique hosts for 2.10.0'
    assert version_2100_data['job_type_total'] == 1, 'Should have 1 job type (workflowjob) for 2.10.0'
    assert version_2100_data['launch_type_total'] == 1, 'Should have 1 launch type (workflow) for 2.10.0'
