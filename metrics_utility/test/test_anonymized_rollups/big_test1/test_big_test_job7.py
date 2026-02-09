"""
Test for big_test1 - Job 7 scenario based on job7.py description.

This test verifies anonymized rollups for a single job with:
- Job Template: T3
- Playbook: playbook2.yml
- 4 hosts: Host1, Host2, Host3, Host4
- 4 tasks: ansible.builtin.copy, community.general.git, community.general.archive, community.weird.git

Host outcomes:
- Host1: all 4 tasks ok (successful)
- Host2: Task 1 ok, Task 2 ok, Task 3 failed then ok (retry successful), Task 4 ok (successful)
- Host3: Task 1 ok, Task 2 dark (unreachable), Task 3 ok, Task 4 ok (unreachable - has dark task)
- Host4: Task 1 ok, Task 2 ok, Task 3 ok, Task 4 failed (3 attempts, all failed) (failed)

Job Final Outcome: failed (because Host4 failed)
"""

import json
import os
import shutil
from datetime import datetime

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.test_anonymized_rollups.big_test1.job7 import events, jobhostsummary, jobs


def test_big_test7():
    """Test anonymized rollups for Job 7 scenario."""
    # Create temporary directory for CSV files
    test_dir = '/tmp/test_big_test7'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        # Create DataFrames and write to CSV files
        jobs_csv = os.path.join(test_dir, 'jobs.csv')
        events_csv = os.path.join(test_dir, 'events.csv')
        jobhostsummary_csv = os.path.join(test_dir, 'jobhostsummary.csv')
        
        pd.DataFrame(jobs).to_csv(jobs_csv, index=False)
        pd.DataFrame(events).to_csv(events_csv, index=False)
        pd.DataFrame(jobhostsummary).to_csv(jobhostsummary_csv, index=False)

        # Prepare input_data with lists of CSV file paths
        # Empty datasets are represented as empty lists (not created as CSV files)
        input_data = {
            'unified_jobs': [jobs_csv],
            'job_host_summary': [jobhostsummary_csv],
            'main_jobevent': [events_csv],
            'execution_environments': [],  # Empty list for empty data
            'credentials': [],  # Empty list for empty data
        }

        # Set up date range for the test (using job dates)
        since = datetime(2024, 1, 15, 0, 0, 0)
        until = datetime(2024, 1, 16, 0, 0, 0)

        # Compute anonymized rollups
        result = compute_anonymized_rollup_from_raw_data(
            input_data=input_data,
            salt='test_salt',
            since=since,
            until=until,
            base_path=test_dir,
            save_rollups=False,
        )

        # Verify job_host_summary rollup
        # The result structure is flattened, so job host summary data is in statistics and jobs_by_* arrays
        assert 'statistics' in result, 'result should have statistics key'
        statistics = result['statistics']
        assert 'rollup_period_job_host_pairs_total' in statistics, 'statistics should have job_host_pairs_total'
        assert statistics['rollup_period_job_host_pairs_total'] == 4, f'Should have 4 job host pairs, got {statistics["rollup_period_job_host_pairs_total"]}'

        # Verify by_job_type aggregation (in jobs_by_job_type)
        assert 'jobs_by_job_type' in result, 'result should have jobs_by_job_type'
        assert isinstance(result['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
        assert len(result['jobs_by_job_type']) == 1, 'Should have 1 job_type group'
        
        job_type_data = result['jobs_by_job_type'][0]
        assert job_type_data['job_type'] == 'workflowjob', 'job_type should be "workflowjob"'
        assert job_type_data['ok_total'] == 14, f'Should have 14 ok tasks total, got {job_type_data["ok_total"]}'
        assert job_type_data['failures_total'] == 1, f'Should have 1 failure total, got {job_type_data["failures_total"]}'
        assert job_type_data['dark_total'] == 1, f'Should have 1 dark total, got {job_type_data["dark_total"]}'
        assert job_type_data['hosts_successful_total'] == 2, f'Should have 2 successful hosts, got {job_type_data["hosts_successful_total"]}'
        assert job_type_data['hosts_failed_total'] == 1, f'Should have 1 failed host, got {job_type_data["hosts_failed_total"]}'
        assert job_type_data['hosts_unreachable_total'] == 1, f'Should have 1 unreachable host, got {job_type_data["hosts_unreachable_total"]}'

        # Verify by_launch_type aggregation (in jobs_by_launch_type)
        assert 'jobs_by_launch_type' in result, 'result should have jobs_by_launch_type'
        assert isinstance(result['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'
        assert len(result['jobs_by_launch_type']) == 1, 'Should have 1 launch_type group'
        
        launch_type_data = result['jobs_by_launch_type'][0]
        assert launch_type_data['launch_type'] == 'workflow', 'launch_type should be "workflow"'
        assert launch_type_data['job_type_total'] == 1, f'Should have 1 job type, got {launch_type_data["job_type_total"]}'

        # Verify by_ansible_version aggregation (in jobs_by_ansible_version)
        assert 'jobs_by_ansible_version' in result, 'result should have jobs_by_ansible_version'
        assert isinstance(result['jobs_by_ansible_version'], list), 'jobs_by_ansible_version should be a list'
        assert len(result['jobs_by_ansible_version']) == 1, 'Should have 1 ansible_version group'
        
        ansible_version_data = result['jobs_by_ansible_version'][0]
        assert ansible_version_data['ansible_version'] == '2.18.0', 'ansible_version should be "2.18.0"'
        assert ansible_version_data['job_type_total'] == 1, f'Should have 1 job type, got {ansible_version_data["job_type_total"]}'
        assert ansible_version_data['launch_type_workflow_total'] == 1, f'Should have 1 workflow launch type, got {ansible_version_data.get("launch_type_workflow_total", 0)}'

        # Pretty print the anonymized rollup result
        json_content = json.dumps(result, indent=2, default=str)
        print(json_content)

    finally:
        # Cleanup
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
