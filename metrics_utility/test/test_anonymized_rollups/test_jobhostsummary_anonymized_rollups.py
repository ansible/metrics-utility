import numpy as np
import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup


jobhostsummary = [
    # job_template T1, job_id 1001, 3 tasks per job, 5 hosts
    # number of tasks = 3
    # total tasks = 3 * 5 = 15
    # Host IDs: h1=1, h2=2, h3=3, h4=4, h5=5
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'host_remote_id': 1,
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
        'host_remote_id': 2,
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
        'host_remote_id': 3,
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
        'host_remote_id': 4,
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
        'host_remote_id': 5,
        'job_remote_id': 1001,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'manual',
    },
    # job_template T1, job_id 1002, one host skips a task, another fails
    # number of tasks = 3
    # total tasks = 3 * 5 = 15
    # Same hosts as job 1001 (h1=1, h2=2, h3=3, h4=4, h5=5)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'host_remote_id': 1,
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
        'host_remote_id': 2,
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
        'host_remote_id': 3,
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
        'host_remote_id': 4,
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
        'host_remote_id': 5,
        'job_remote_id': 1002,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.9.10',
        'launch_type': 'scheduled',
    },
    # job_template T2, job_id 2001, 5 tasks per job, 3 hosts
    # number of tasks = 5
    # total tasks = 5 * 3 = 15
    # Host IDs: h1=1, h2=2, h3=3 (same hosts as before, but different job type)
    {
        'dark': 0,
        'failures': 0,
        'ok': 5,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'host_remote_id': 1,
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
        'host_remote_id': 2,
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
        'host_remote_id': 3,
        'job_remote_id': 2001,
        'job_template_name': 'T2',
        'model': 'workflowjob',
        'ansible_version': '2.10.0',
        'launch_type': 'workflow',
    },
    # job_template T2, job_id 2002, one host executes only 4 tasks, another fails
    # number of tasks = 5
    # total tasks = 5 * 3 = 15
    # Same hosts as job 2001 (h1=1, h2=2, h3=3)
    {
        'dark': 0,
        'failures': 0,
        'ok': 5,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h1',
        'host_remote_id': 1,
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
        'host_remote_id': 2,
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
        'host_remote_id': 3,
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

    # result should be a dict with 'by_job_type', 'by_launch_type', 'by_ansible_version' (lists) and 'job_host_pairs_total'
    assert 'by_job_type' in result, 'result should have by_job_type key'
    assert isinstance(result['by_job_type'], list), 'by_job_type should be a list'
    assert 'by_launch_type' in result, 'result should have by_launch_type key'
    assert isinstance(result['by_launch_type'], list), 'by_launch_type should be a list'
    assert 'by_ansible_version' in result, 'result should have by_ansible_version key'
    assert isinstance(result['by_ansible_version'], list), 'by_ansible_version should be a list'
    assert 'job_host_pairs_total' in result, 'result should have job_host_pairs_total key'
    assert isinstance(result['job_host_pairs_total'], int), 'job_host_pairs_total should be an integer'

    # Verify job_host_pairs_total count: 16 rows total (5+5 for job type, 3+3 for workflowjob type)
    assert result['job_host_pairs_total'] == 16, f'Should have 16 total job host summary records, got {result["job_host_pairs_total"]}'

    # Should have 2 job_type groups: 'job' and 'workflowjob'
    assert len(result['by_job_type']) == 2, 'Should have 2 job_type groups'

    # Find the 'job' type group
    job_type_data = next((j for j in result['by_job_type'] if j['job_type'] == 'job'), None)
    assert job_type_data is not None, 'Should have job_type job'
    # Note: unique_hosts_total is only at top level, not in groupings

    # Verify totals for 'job' type (T1: 10 hosts × 3 tasks = 30 tasks, but we have 26 ok + 2 failures + 2 skipped = 30)
    assert job_type_data['unreachable_total'] == 0
    assert job_type_data['failed_total'] == 2  # T1: 2 failures
    assert job_type_data['ok_total'] == 26  # T1: 26 ok
    assert job_type_data['skipped_total'] == 2  # T1: 2 skipped
    assert job_type_data['ignored_total'] == 0
    assert job_type_data['rescued_total'] == 0

    # Find the 'workflowjob' type group
    workflowjob_type_data = next((j for j in result['by_job_type'] if j['job_type'] == 'workflowjob'), None)
    assert workflowjob_type_data is not None, 'Should have job_type workflowjob'
    # Note: unique_hosts_total is only at top level, not in groupings

    # Verify totals for 'workflowjob' type (T2: 6 hosts × 5 tasks = 30 tasks, but we have 26 ok + 4 failures = 30)
    assert workflowjob_type_data['unreachable_total'] == 0
    assert workflowjob_type_data['failed_total'] == 4  # T2: 4 failures
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

    # Find the 'scheduled' launch_type group
    scheduled_launch_type_data = next((j for j in result['by_launch_type'] if j['launch_type'] == 'scheduled'), None)
    assert scheduled_launch_type_data is not None, 'Should have launch_type scheduled'

    # Find the 'workflow' launch_type group
    workflow_launch_type_data = next((j for j in result['by_launch_type'] if j['launch_type'] == 'workflow'), None)
    assert workflow_launch_type_data is not None, 'Should have launch_type workflow'

    # Verify by_ansible_version groupings
    # Should have 2 ansible_version groups: '2.9.10' and '2.10.0'
    assert len(result['by_ansible_version']) == 2, 'Should have 2 ansible_version groups'

    # Find the '2.9.10' ansible_version group
    version_2910_data = next((j for j in result['by_ansible_version'] if j['ansible_version'] == '2.9.10'), None)
    assert version_2910_data is not None, 'Should have ansible_version 2.9.10'
    assert version_2910_data['launch_type_total'] == 2, 'Should have 2 launch types (manual, scheduled) for 2.9.10'

    # Find the '2.10.0' ansible_version group
    version_2100_data = next((j for j in result['by_ansible_version'] if j['ansible_version'] == '2.10.0'), None)
    assert version_2100_data is not None, 'Should have ansible_version 2.10.0'
    assert version_2100_data['launch_type_total'] == 1, 'Should have 1 launch type (workflow) for 2.10.0'


# ---------------------------------------------------------------------------
# Host outcome counting (vectorized int8 flags + groupby('sum'))
# ---------------------------------------------------------------------------


def _outcome_rows(outcomes, job_remote_id=1001):
    """Build job_host_summary rows from outcome names ('successful'/'failed'/'unreachable')."""
    rows = []
    for index, outcome in enumerate(outcomes, start=1):
        rows.append(
            {
                'dark': 1 if outcome == 'unreachable' else 0,
                'failures': 1 if outcome == 'failed' else 0,
                'ok': 1,
                'skipped': 0,
                'ignored': 0,
                'rescued': 0,
                'host_name': f'h{index}',
                'host_remote_id': index,
                'job_remote_id': job_remote_id,
                'job_template_name': 'T1',
                'model': 'job',
                'ansible_version': '2.9.10',
                'launch_type': 'manual',
            }
        )
    return rows


@pytest.mark.parametrize(
    ('outcomes', 'expected'),
    [
        (['successful', 'successful', 'successful'], (3, 0, 0)),
        (['failed', 'failed', 'failed'], (0, 3, 0)),
        (['unreachable', 'unreachable', 'unreachable'], (0, 0, 3)),
        (['successful', 'failed', 'unreachable', 'successful'], (2, 1, 1)),
    ],
    ids=['successful', 'failed', 'unreachable', 'mixed'],
)
def test_host_outcome_counts_by_job_type(outcomes, expected):
    rollup = JobHostSummaryAnonymizedRollup()

    result = rollup.prepare(pd.DataFrame(_outcome_rows(outcomes)))

    assert result['job_host_pairs_total'] == len(outcomes)
    assert len(result['by_job_type']) == 1
    group = result['by_job_type'][0]
    assert group['successful_hosts_total'] == expected[0]
    assert group['failed_hosts_total'] == expected[1]
    assert group['unreachable_hosts_total'] == expected[2]


def test_host_outcome_counts_by_job_remote_id():
    """Two jobs, one all-successful and one with a failure and an unreachable host."""
    rollup = JobHostSummaryAnonymizedRollup()
    rows = _outcome_rows(['successful', 'successful'], job_remote_id=1001) + _outcome_rows(['failed', 'unreachable'], job_remote_id=1002)

    result = rollup.prepare(pd.DataFrame(rows))

    assert result['by_job_type'] == [
        {
            'job_type': 'job',
            'unreachable_total': 1,
            'failed_total': 1,
            'ok_total': 4,
            'skipped_total': 0,
            'ignored_total': 0,
            'rescued_total': 0,
            'successful_hosts_total': 2,
            'failed_hosts_total': 1,
            'unreachable_hosts_total': 1,
        }
    ]


def test_host_outcome_dark_wins_over_failures():
    """A host with both dark>0 and failures>0 is counted as unreachable (legacy precedence)."""
    rollup = JobHostSummaryAnonymizedRollup()
    rows = _outcome_rows(['successful'])
    rows[0].update({'dark': 1, 'failures': 2})

    result = rollup.prepare(pd.DataFrame(rows))

    group = result['by_job_type'][0]
    assert group['successful_hosts_total'] == 0
    assert group['failed_hosts_total'] == 0
    assert group['unreachable_hosts_total'] == 1


def test_normalize_dataframe_computes_host_outcome_and_flags():
    rollup = JobHostSummaryAnonymizedRollup()
    dataframe = rollup._normalize_dataframe(pd.DataFrame(_outcome_rows(['successful', 'failed', 'unreachable'])))

    assert dataframe['host_outcome'].tolist() == ['successful', 'failed', 'unreachable']
    assert dataframe['is_successful'].tolist() == [1, 0, 0]
    assert dataframe['is_failed'].tolist() == [0, 1, 0]
    assert dataframe['is_unreachable'].tolist() == [0, 0, 1]
    for flag in ['is_successful', 'is_failed', 'is_unreachable']:
        assert dataframe[flag].dtype == np.dtype('int8')


def test_aggregate_by_job_type_directly_without_normalization():
    """``_aggregate_by_job_type_directly`` computes its own outcome flag columns."""
    rollup = JobHostSummaryAnonymizedRollup()
    dataframe = pd.DataFrame(_outcome_rows(['successful', 'failed', 'unreachable', 'successful']))
    dataframe['launch_type'] = ['manual', 'manual', 'scheduled', 'manual']

    result = rollup._aggregate_by_job_type_directly(dataframe)

    manual = result[result['launch_type'] == 'manual']
    assert len(manual) == 1
    assert manual['job_type'].tolist() == ['job']
    assert manual['successful_hosts_total'].tolist() == [2]
    assert manual['failed_hosts_total'].tolist() == [1]
    assert manual['unreachable_hosts_total'].tolist() == [0]

    scheduled = result[result['launch_type'] == 'scheduled']
    assert len(scheduled) == 1
    assert scheduled['successful_hosts_total'].tolist() == [0]
    assert scheduled['failed_hosts_total'].tolist() == [0]
    assert scheduled['unreachable_hosts_total'].tolist() == [1]


def test_aggregate_by_job_remote_id_without_normalization():
    rollup = JobHostSummaryAnonymizedRollup()
    dataframe = pd.DataFrame(_outcome_rows(['successful', 'failed', 'unreachable']))

    result = rollup._aggregate_by_job_remote_id(dataframe)

    assert result['job_remote_id'].tolist() == [1001]
    assert result['successful_hosts_total'].tolist() == [1]
    assert result['failed_hosts_total'].tolist() == [1]
    assert result['unreachable_hosts_total'].tolist() == [1]


def test_prepare_without_job_remote_id_keeps_pairs_and_host_ids():
    """Missing ``job_remote_id`` must not crash; pairs and host ids are still reported."""
    rollup = JobHostSummaryAnonymizedRollup()
    dataframe = pd.DataFrame(_outcome_rows(['successful', 'failed'])).drop(columns=['job_remote_id'])

    result = rollup.prepare(dataframe)

    assert result['job_host_pairs_total'] == 2
    assert result['host_ids'] == ['1', '2']
    # The current implementation groups by the (all-null) job_remote_id column,
    # which pandas drops, so no dimension groups are produced.
    assert result['by_job_type'] == []
    assert result['by_launch_type'] == []
    assert result['by_ansible_version'] == []
