"""
Test for big_test1 - Job 1 scenario based on job1.py description.

This test verifies anonymized rollups for a single job with:
- Job Template: T1
- Playbook: playbook1.yml
- 4 hosts: Host1, Host2, Host3, Host4
- 3 tasks: ansible.builtin.copy, ansible.builtin.file, ansible.builtin.yum

Host outcomes:
- Host1: all 3 tasks ok (successful)
- Host2: Task 1 failed then ok (retry successful), Task 2 ok, Task 3 ok (successful)
- Host3: Task 1 ok, Task 2 failed then ok (retry successful), Task 3 ok (successful)
- Host4: Task 1 ok, Task 2 ok, Task 3 failed (3 attempts, all failed) (failed)

Job Final Outcome: failed (because Host4 failed)
"""

import os
import shutil

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.test_anonymized_rollups.big_test1.job1 import events, jobhostsummary, jobs


def test_big_test1():
    """Test anonymized rollups for Job 1 scenario."""
    # Create temporary directory for CSV files
    test_dir = '/tmp/test_big_test1'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)

    try:
        # Create DataFrames
        jobs_df = pd.DataFrame(jobs)
        events_df = pd.DataFrame(events)
        jobhostsummary_df = pd.DataFrame(jobhostsummary)
        # Empty datasets for other collectors (not used in this test)
        execution_environments_df = pd.DataFrame([])
        credentials_df = pd.DataFrame([])

        # Write to CSV files
        jobs_df.to_csv(os.path.join(test_dir, 'jobs.csv'), index=False)
        events_df.to_csv(os.path.join(test_dir, 'events.csv'), index=False)
        jobhostsummary_df.to_csv(os.path.join(test_dir, 'jobhostsummary.csv'), index=False)
        execution_environments_df.to_csv(os.path.join(test_dir, 'execution_environments.csv'), index=False)
        credentials_df.to_csv(os.path.join(test_dir, 'credentials.csv'), index=False)

        # Compute anonymized rollups
        result = compute_anonymized_rollup_from_raw_data(
            jobs_csv_path=os.path.join(test_dir, 'jobs.csv'),
            events_csv_path=os.path.join(test_dir, 'events.csv'),
            jobhostsummary_csv_path=os.path.join(test_dir, 'jobhostsummary.csv'),
            execution_environments_csv_path=os.path.join(test_dir, 'execution_environments.csv'),
            credentials_csv_path=os.path.join(test_dir, 'credentials.csv'),
        )

        # Verify job_host_summary rollup
        job_host_summary = result.get('job_host_summary', {})
        assert 'json' in job_host_summary, 'job_host_summary should have json key'
        
        json_data = job_host_summary['json']
        assert 'job_host_pairs_total' in json_data, 'json should have job_host_pairs_total'
        assert json_data['job_host_pairs_total'] == 4, f'Should have 4 job host pairs, got {json_data["job_host_pairs_total"]}'

        # Verify by_job_type aggregation
        assert 'by_job_type' in json_data, 'json should have by_job_type'
        assert isinstance(json_data['by_job_type'], list), 'by_job_type should be a list'
        assert len(json_data['by_job_type']) == 1, 'Should have 1 job_type group'
        
        job_type_data = json_data['by_job_type'][0]
        assert job_type_data['job_type'] == 'job', 'job_type should be "job"'
        assert job_type_data['ok_total'] == 11, f'Should have 11 ok tasks total, got {job_type_data["ok_total"]}'
        assert job_type_data['failures_total'] == 1, f'Should have 1 failure total, got {job_type_data["failures_total"]}'
        assert job_type_data['dark_total'] == 0, f'Should have 0 dark total, got {job_type_data["dark_total"]}'
        assert job_type_data['hosts_successful_total'] == 3, f'Should have 3 successful hosts, got {job_type_data["hosts_successful_total"]}'
        assert job_type_data['hosts_failed_total'] == 1, f'Should have 1 failed host, got {job_type_data["hosts_failed_total"]}'
        assert job_type_data['hosts_unreachable_total'] == 0, f'Should have 0 unreachable hosts, got {job_type_data["hosts_unreachable_total"]}'

        # Verify by_launch_type aggregation
        assert 'by_launch_type' in json_data, 'json should have by_launch_type'
        assert isinstance(json_data['by_launch_type'], list), 'by_launch_type should be a list'
        assert len(json_data['by_launch_type']) == 1, 'Should have 1 launch_type group'
        
        launch_type_data = json_data['by_launch_type'][0]
        assert launch_type_data['launch_type'] == 'manual', 'launch_type should be "manual"'
        assert launch_type_data['job_type_total'] == 1, f'Should have 1 job type, got {launch_type_data["job_type_total"]}'

        # Verify by_ansible_version aggregation
        assert 'by_ansible_version' in json_data, 'json should have by_ansible_version'
        assert isinstance(json_data['by_ansible_version'], list), 'by_ansible_version should be a list'
        assert len(json_data['by_ansible_version']) == 1, 'Should have 1 ansible_version group'
        
        ansible_version_data = json_data['by_ansible_version'][0]
        assert ansible_version_data['ansible_version'] == '2.15.0', 'ansible_version should be "2.15.0"'
        assert ansible_version_data['job_type_total'] == 1, f'Should have 1 job type, got {ansible_version_data["job_type_total"]}'
        assert ansible_version_data['launch_type_total'] == 1, f'Should have 1 launch type, got {ansible_version_data["launch_type_total"]}'

        print("Test passed successfully!")
        print(f"Job host pairs total: {json_data['job_host_pairs_total']}")
        print(f"Job type aggregation: {json_data['by_job_type']}")
        print(f"Launch type aggregation: {json_data['by_launch_type']}")
        print(f"Ansible version aggregation: {json_data['by_ansible_version']}")

    finally:
        # Cleanup
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
