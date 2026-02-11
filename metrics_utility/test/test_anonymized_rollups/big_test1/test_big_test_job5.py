"""
Test for big_test1 - Job 5 scenario based on job5.py description.

This test verifies anonymized rollups for a single job with:
- Job Template: T1
- Playbook: playbook1.yml
- 4 hosts: Host1, Host2, Host3, Host4
- 3 tasks: ansible.builtin.copy, ansible.builtin.file, ansible.builtin.yum

Host outcomes:
- Host1: all 3 tasks ok (successful)
- Host2: Task 1 failed then ok (retry successful), Task 2 ok, Task 3 ok (successful)
- Host3: Task 1 ok, Task 2 failed (1st attempt) -> failed (2nd attempt) -> ok (retry successful on 3rd attempt), Task 3 ok (successful)
- Host4: Task 1 ok, Task 2 ok, Task 3 failed then ok (retry successful) (successful)

Job Final Outcome: successful (all hosts succeeded)
"""

import json
import os
import shutil

from datetime import datetime

import pandas as pd

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.test_anonymized_rollups.big_test1.credentials import credentials
from metrics_utility.test.test_anonymized_rollups.big_test1.execution_environments import execution_environments
from metrics_utility.test.test_anonymized_rollups.big_test1.job5 import events, jobhostsummary, jobs


def test_big_test5():
    """Test anonymized rollups for Job 5 scenario."""
    # Create temporary directory for CSV files
    test_dir = '/tmp/test_big_test5'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        # Create DataFrames and write to CSV files
        jobs_csv = os.path.join(test_dir, 'jobs.csv')
        events_csv = os.path.join(test_dir, 'events.csv')
        jobhostsummary_csv = os.path.join(test_dir, 'jobhostsummary.csv')
        execution_environments_csv = os.path.join(test_dir, 'execution_environments.csv')
        credentials_csv = os.path.join(test_dir, 'credentials.csv')

        pd.DataFrame(jobs).to_csv(jobs_csv, index=False)
        pd.DataFrame(events).to_csv(events_csv, index=False)
        pd.DataFrame(jobhostsummary).to_csv(jobhostsummary_csv, index=False)
        pd.DataFrame(execution_environments).to_csv(execution_environments_csv, index=False)
        # Filter credentials for job 5 only
        job5_credentials = [c for c in credentials if c['job_id'] == 5]
        pd.DataFrame(job5_credentials).to_csv(credentials_csv, index=False)

        # Prepare input_data with lists of CSV file paths
        input_data = {
            'unified_jobs': [jobs_csv],
            'job_host_summary': [jobhostsummary_csv],
            'main_jobevent': [events_csv],
            'execution_environments': [execution_environments_csv],
            'credentials': [credentials_csv],
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
        assert statistics['rollup_period_job_host_pairs_total'] == 4, (
            f'Should have 4 job host pairs, got {statistics["rollup_period_job_host_pairs_total"]}'
        )
        assert 'rollup_period_successful_hosts_total' in statistics, 'statistics should have successful_hosts_total'
        assert statistics['rollup_period_successful_hosts_total'] == 4, (
            f'Should have 4 successful hosts, got {statistics["rollup_period_successful_hosts_total"]}'
        )
        assert 'rollup_period_failed_hosts_total' in statistics, 'statistics should have failed_hosts_total'
        assert statistics['rollup_period_failed_hosts_total'] == 0, (
            f'Should have 0 failed hosts, got {statistics["rollup_period_failed_hosts_total"]}'
        )
        assert 'rollup_period_unreachable_hosts_total' in statistics, 'statistics should have unreachable_hosts_total'
        assert statistics['rollup_period_unreachable_hosts_total'] == 0, (
            f'Should have 0 unreachable hosts, got {statistics["rollup_period_unreachable_hosts_total"]}'
        )

        # Verify by_job_type aggregation (in jobs_by_job_type)
        assert 'jobs_by_job_type' in result, 'result should have jobs_by_job_type'
        assert isinstance(result['jobs_by_job_type'], list), 'jobs_by_job_type should be a list'
        assert len(result['jobs_by_job_type']) == 1, 'Should have 1 job_type group'

        job_type_data = result['jobs_by_job_type'][0]
        assert job_type_data['job_type'] == 'job', 'job_type should be "job"'
        assert job_type_data['ok_total'] == 12, f'Should have 12 ok tasks total, got {job_type_data["ok_total"]}'
        assert job_type_data['failures_total'] == 0, f'Should have 0 failures total, got {job_type_data["failures_total"]}'
        assert job_type_data['dark_total'] == 0, f'Should have 0 dark total, got {job_type_data["dark_total"]}'
        assert job_type_data['successful_hosts_total'] == 4, f'Should have 4 successful hosts, got {job_type_data["successful_hosts_total"]}'
        assert job_type_data['failed_hosts_total'] == 0, f'Should have 0 failed hosts, got {job_type_data["failed_hosts_total"]}'
        assert job_type_data['unreachable_hosts_total'] == 0, f'Should have 0 unreachable hosts, got {job_type_data["unreachable_hosts_total"]}'

        # Verify by_launch_type aggregation (in jobs_by_launch_type)
        assert 'jobs_by_launch_type' in result, 'result should have jobs_by_launch_type'
        assert isinstance(result['jobs_by_launch_type'], list), 'jobs_by_launch_type should be a list'
        assert len(result['jobs_by_launch_type']) == 1, 'Should have 1 launch_type group'

        launch_type_data = result['jobs_by_launch_type'][0]
        assert launch_type_data['launch_type'] == 'manual', 'launch_type should be "manual"'
        assert launch_type_data['job_type_total'] == 1, f'Should have 1 job type, got {launch_type_data["job_type_total"]}'

        # Verify by_controller_version aggregation (in jobs_by_controller_version)
        assert 'jobs_by_controller_version' in result, 'result should have jobs_by_controller_version'
        assert isinstance(result['jobs_by_controller_version'], list), 'jobs_by_controller_version should be a list'
        assert len(result['jobs_by_controller_version']) == 1, 'Should have 1 controller_version group'

        controller_version_data = result['jobs_by_controller_version'][0]
        assert controller_version_data['controller_version'] == '2.16.0', 'controller_version should be "2.16.0"'
        assert controller_version_data['job_type_total'] == 1, f'Should have 1 job type, got {controller_version_data["job_type_total"]}'
        assert controller_version_data['launch_type_manual_total'] == 1, (
            f'Should have 1 manual launch type, got {controller_version_data.get("launch_type_manual_total", 0)}'
        )

        # Verify execution environments
        assert 'rollup_period_execution_environments_total' in statistics
        assert statistics['rollup_period_execution_environments_total'] == 8
        assert statistics['rollup_period_EE_default_total'] == 4
        assert statistics['rollup_period_EE_custom_total'] == 4

        # Verify credentials (Job 5 has Machine and Amazon Web Services)
        assert 'rollup_period_credential_types' in result
        assert isinstance(result['rollup_period_credential_types'], list)
        assert 'Amazon Web Services' in result['rollup_period_credential_types']
        assert 'Machine' in result['rollup_period_credential_types']
        assert len(result['rollup_period_credential_types']) == 2

        # Pretty print the anonymized rollup result
        json_content = json.dumps(result, indent=2, default=str)
        print(json_content)

    finally:
        # Cleanup
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
