"""
This test verifies that data split into multiple CSV files is correctly concatenated.
It uses its own independent dataset that represents a realistic Ansible automation scenario.

The test:
1. Uses its own dataset (jobs, events, execution_environments, jobhostsummary, credentials)
2. Splits each dataset into 2-3 separate CSV files
3. Creates CSV files with the split data
4. Tests that compute_anonymized_rollup_from_raw_data properly loads and concatenates the data
5. Validates the final output matches expected aggregated results

The dataset represents a realistic scenario:
- Job 101: Web server deployment (job type) - succeeds on 3 hosts, fails on 1 host
- Job 102: Database configuration (job type) - all hosts succeed
- Job 103: Security update workflow (workflowjob type) - succeeds on all hosts
- Job 104: Backup operation (adhoccommand type) - fails on 1 host
- Job 105: Application deployment (job type) - succeeds with some skipped tasks
- Job 106: Network configuration (job type) - one host unreachable
- Job 107: Log rotation (job type) - succeeds with ignored errors
- Job 108: Monitoring setup workflow (workflowjob type) - succeeds on all hosts

Job host summaries are aligned with events - failures in events match failures in job host summaries.
"""

import json
import os
import shutil

from datetime import datetime

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data

# ============================================================================
# Test Dataset - Realistic Ansible Automation Scenario
# ============================================================================

# Jobs dataset
jobs = [
    # Job 1: Web server deployment - succeeds on 3 hosts, fails on 1 host
    {
        'id': 101,
        'started': '2024-02-15 10:00:00.000000+00',
        'finished': '2024-02-15 10:05:30.000000+00',  # 5.5 minutes
        'failed': 1,  # Failed because one host had failures
        'job_template_name': 'WebServerDeploy',
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Production',
        'created': '2024-02-15 09:59:30.000000+00',  # 30s wait
        'model': 'job',
        'launch_type': 'manual',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
        'forks': 10,
        'inventory_name': 'web-servers',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'ansible.posix': {'version': '1.5.0'},
            'community.general': {'version': '7.0.0'},
        }),
    },
    # Job 2: Database configuration - all hosts succeed
    {
        'id': 102,
        'started': '2024-02-15 11:00:00.000000+00',
        'finished': '2024-02-15 11:03:15.000000+00',  # 3.25 minutes
        'failed': 0,
        'job_template_name': 'DatabaseConfig',
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Production',
        'created': '2024-02-15 10:59:45.000000+00',  # 15s wait
        'model': 'job',
        'launch_type': 'scheduled',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 5,
        'inventory_name': 'database-servers',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'community.postgresql': {'version': '2.0.0'},
        }),
    },
    # Job 3: Security update workflow - all hosts succeed
    {
        'id': 103,
        'started': '2024-02-15 14:00:00.000000+00',
        'finished': '2024-02-15 14:08:20.000000+00',  # 8.33 minutes
        'failed': 0,
        'job_template_name': 'SecurityUpdate',
        'controller_node': 'controller-02',
        'ansible_version': '2.16.0',
        'organization_name': 'Production',
        'created': '2024-02-15 13:58:00.000000+00',  # 2 minutes wait
        'model': 'workflowjob',
        'launch_type': 'workflow',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 15,
        'inventory_name': 'all-servers',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'ansible.posix': {'version': '1.5.0'},
        }),
    },
    # Job 4: Backup operation - fails on 1 host
    {
        'id': 104,
        'started': '2024-02-15 16:00:00.000000+00',
        'finished': '2024-02-15 16:02:10.000000+00',  # 2.17 minutes
        'failed': 1,
        'job_template_name': 'BackupOperation',
        'controller_node': 'controller-02',
        'ansible_version': '2.15.0',
        'organization_name': 'Production',
        'created': '2024-02-15 15:59:50.000000+00',  # 10s wait
        'model': 'adhoccommand',
        'launch_type': 'callback',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
        'forks': 8,
        'inventory_name': 'backup-servers',
        'scm_type': 'manual',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'community.general': {'version': '7.0.0'},
        }),
    },
    # Job 5: Application deployment - some tasks skipped
    {
        'id': 105,
        'started': '2024-02-15 17:00:00.000000+00',
        'finished': '2024-02-15 17:04:45.000000+00',  # 4.75 minutes
        'failed': 0,
        'job_template_name': 'AppDeployment',
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Production',
        'created': '2024-02-15 16:59:30.000000+00',  # 30s wait
        'model': 'job',
        'launch_type': 'scheduled',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 12,
        'inventory_name': 'app-servers',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'ansible.posix': {'version': '1.5.0'},
            'community.docker': {'version': '3.0.0'},
        }),
    },
    # Job 6: Network configuration - one host unreachable
    {
        'id': 106,
        'started': '2024-02-15 18:00:00.000000+00',
        'finished': '2024-02-15 18:03:20.000000+00',  # 3.33 minutes
        'failed': 1,  # Failed because one host is unreachable
        'job_template_name': 'NetworkConfig',
        'controller_node': 'controller-02',
        'ansible_version': '2.16.0',
        'organization_name': 'Production',
        'created': '2024-02-15 17:59:15.000000+00',  # 45s wait
        'model': 'job',
        'launch_type': 'manual',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 1,
        'number_of_jobs_succeeded': 0,
        'forks': 6,
        'inventory_name': 'network-devices',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'ansible.netcommon': {'version': '4.0.0'},
        }),
    },
    # Job 7: Log rotation - some failures ignored
    {
        'id': 107,
        'started': '2024-02-15 19:00:00.000000+00',
        'finished': '2024-02-15 19:02:30.000000+00',  # 2.5 minutes
        'failed': 0,  # Succeeds because failures are ignored
        'job_template_name': 'LogRotation',
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Production',
        'created': '2024-02-15 18:59:45.000000+00',  # 15s wait
        'model': 'job',
        'launch_type': 'scheduled',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 10,
        'inventory_name': 'log-servers',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'community.general': {'version': '7.0.0'},
        }),
    },
    # Job 8: Monitoring setup workflow - all succeed
    {
        'id': 108,
        'started': '2024-02-15 20:00:00.000000+00',
        'finished': '2024-02-15 20:05:15.000000+00',  # 5.25 minutes
        'failed': 0,
        'job_template_name': 'MonitoringSetup',
        'controller_node': 'controller-02',
        'ansible_version': '2.16.0',
        'organization_name': 'Production',
        'created': '2024-02-15 19:58:30.000000+00',  # 1.5 minutes wait
        'model': 'workflowjob',
        'launch_type': 'workflow',
        'number_of_jobs_executed': 1,
        'number_of_jobs_failed': 0,
        'number_of_jobs_succeeded': 1,
        'forks': 8,
        'inventory_name': 'monitoring-servers',
        'scm_type': 'git',
        'installed_collections': json.dumps({
            'ansible.builtin': {'version': '2.15.0'},
            'community.general': {'version': '7.0.0'},
        }),
    },
]

# Events dataset - aligned with jobs and job host summaries
events = [
    # ================================================================
    # Job 101 - WebServerDeploy - 4 hosts, 3 succeed, 1 fails
    # ================================================================
    # Host web01 - all tasks succeed
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 201,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 201,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 201,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host web02 - all tasks succeed
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 202,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 202,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 202,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host web03 - all tasks succeed
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 203,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 203,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 203,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host web04 - task-002 fails, causing job failure
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 204,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 101,
        'playbook': 'deploy-webserver.yml',
        'host_id': 204,
        'task_uuid': 'task-002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 102 - DatabaseConfig - 2 hosts, all succeed
    # ================================================================
    # Host db01 - all tasks succeed
    {
        'job_id': 102,
        'playbook': 'configure-database.yml',
        'host_id': 301,
        'task_uuid': 'task-101',
        'event': 'runner_on_ok',
        'task_action': 'community.postgresql.postgresql_user',
        'job_created': '2024-02-15 10:59:45+00',
        'job_started': '2024-02-15 11:00:00+00',
        'job_finished': '2024-02-15 11:03:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 102,
        'playbook': 'configure-database.yml',
        'host_id': 301,
        'task_uuid': 'task-102',
        'event': 'runner_on_ok',
        'task_action': 'community.postgresql.postgresql_db',
        'job_created': '2024-02-15 10:59:45+00',
        'job_started': '2024-02-15 11:00:00+00',
        'job_finished': '2024-02-15 11:03:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host db02 - all tasks succeed
    {
        'job_id': 102,
        'playbook': 'configure-database.yml',
        'host_id': 302,
        'task_uuid': 'task-101',
        'event': 'runner_on_ok',
        'task_action': 'community.postgresql.postgresql_user',
        'job_created': '2024-02-15 10:59:45+00',
        'job_started': '2024-02-15 11:00:00+00',
        'job_finished': '2024-02-15 11:03:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 102,
        'playbook': 'configure-database.yml',
        'host_id': 302,
        'task_uuid': 'task-102',
        'event': 'runner_on_ok',
        'task_action': 'community.postgresql.postgresql_db',
        'job_created': '2024-02-15 10:59:45+00',
        'job_started': '2024-02-15 11:00:00+00',
        'job_finished': '2024-02-15 11:03:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 103 - SecurityUpdate - 3 hosts, all succeed
    # ================================================================
    # Host sec01 - all tasks succeed
    {
        'job_id': 103,
        'playbook': 'security-update.yml',
        'host_id': 401,
        'task_uuid': 'task-201',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 13:58:00+00',
        'job_started': '2024-02-15 14:00:00+00',
        'job_finished': '2024-02-15 14:08:20+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 103,
        'playbook': 'security-update.yml',
        'host_id': 401,
        'task_uuid': 'task-202',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.reboot',
        'job_created': '2024-02-15 13:58:00+00',
        'job_started': '2024-02-15 14:00:00+00',
        'job_finished': '2024-02-15 14:08:20+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host sec02 - all tasks succeed
    {
        'job_id': 103,
        'playbook': 'security-update.yml',
        'host_id': 402,
        'task_uuid': 'task-201',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 13:58:00+00',
        'job_started': '2024-02-15 14:00:00+00',
        'job_finished': '2024-02-15 14:08:20+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 103,
        'playbook': 'security-update.yml',
        'host_id': 402,
        'task_uuid': 'task-202',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.reboot',
        'job_created': '2024-02-15 13:58:00+00',
        'job_started': '2024-02-15 14:00:00+00',
        'job_finished': '2024-02-15 14:08:20+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host sec03 - all tasks succeed
    {
        'job_id': 103,
        'playbook': 'security-update.yml',
        'host_id': 403,
        'task_uuid': 'task-201',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 13:58:00+00',
        'job_started': '2024-02-15 14:00:00+00',
        'job_finished': '2024-02-15 14:08:20+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 103,
        'playbook': 'security-update.yml',
        'host_id': 403,
        'task_uuid': 'task-202',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.reboot',
        'job_created': '2024-02-15 13:58:00+00',
        'job_started': '2024-02-15 14:00:00+00',
        'job_finished': '2024-02-15 14:08:20+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 104 - BackupOperation - 2 hosts, 1 succeeds, 1 fails
    # ================================================================
    # Host backup01 - all tasks succeed
    {
        'job_id': 104,
        'playbook': 'backup.yml',
        'host_id': 501,
        'task_uuid': 'task-301',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.archive',
        'job_created': '2024-02-15 15:59:50+00',
        'job_started': '2024-02-15 16:00:00+00',
        'job_finished': '2024-02-15 16:02:10+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 104,
        'playbook': 'backup.yml',
        'host_id': 501,
        'task_uuid': 'task-302',
        'event': 'runner_on_ok',
        'task_action': 'community.general.s3_sync',
        'job_created': '2024-02-15 15:59:50+00',
        'job_started': '2024-02-15 16:00:00+00',
        'job_finished': '2024-02-15 16:02:10+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host backup02 - task-302 fails
    {
        'job_id': 104,
        'playbook': 'backup.yml',
        'host_id': 502,
        'task_uuid': 'task-301',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.archive',
        'job_created': '2024-02-15 15:59:50+00',
        'job_started': '2024-02-15 16:00:00+00',
        'job_finished': '2024-02-15 16:02:10+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 104,
        'playbook': 'backup.yml',
        'host_id': 502,
        'task_uuid': 'task-302',
        'event': 'runner_on_failed',
        'task_action': 'community.general.s3_sync',
        'job_created': '2024-02-15 15:59:50+00',
        'job_started': '2024-02-15 16:00:00+00',
        'job_finished': '2024-02-15 16:02:10+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Warning event for job 101
    {
        'job_id': 101,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'job_created': '2024-02-15 09:59:30+00',
        'job_started': '2024-02-15 10:00:00+00',
        'job_finished': '2024-02-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 105 - AppDeployment - 3 hosts, some tasks skipped
    # ================================================================
    # Host app01 - all tasks succeed
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 601,
        'task_uuid': 'task-401',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 601,
        'task_uuid': 'task-402',
        'event': 'runner_on_ok',
        'task_action': 'community.docker.docker_container',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 601,
        'task_uuid': 'task-403',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host app02 - task-402 skipped (condition not met)
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 602,
        'task_uuid': 'task-401',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 602,
        'task_uuid': 'task-402',
        'event': 'runner_on_skipped',
        'task_action': 'community.docker.docker_container',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 602,
        'task_uuid': 'task-403',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host app03 - all tasks succeed
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 603,
        'task_uuid': 'task-401',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.apt',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 603,
        'task_uuid': 'task-402',
        'event': 'runner_on_ok',
        'task_action': 'community.docker.docker_container',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 105,
        'playbook': 'deploy-app.yml',
        'host_id': 603,
        'task_uuid': 'task-403',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 16:59:30+00',
        'job_started': '2024-02-15 17:00:00+00',
        'job_finished': '2024-02-15 17:04:45+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 106 - NetworkConfig - 2 hosts, 1 unreachable
    # ================================================================
    # Host net01 - all tasks succeed
    {
        'job_id': 106,
        'playbook': 'configure-network.yml',
        'host_id': 701,
        'task_uuid': 'task-501',
        'event': 'runner_on_ok',
        'task_action': 'ansible.netcommon.cli_config',
        'job_created': '2024-02-15 17:59:15+00',
        'job_started': '2024-02-15 18:00:00+00',
        'job_finished': '2024-02-15 18:03:20+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 106,
        'playbook': 'configure-network.yml',
        'host_id': 701,
        'task_uuid': 'task-502',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-02-15 17:59:15+00',
        'job_started': '2024-02-15 18:00:00+00',
        'job_finished': '2024-02-15 18:03:20+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host net02 - unreachable
    {
        'job_id': 106,
        'playbook': 'configure-network.yml',
        'host_id': 702,
        'task_uuid': 'task-501',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.netcommon.cli_config',
        'job_created': '2024-02-15 17:59:15+00',
        'job_started': '2024-02-15 18:00:00+00',
        'job_finished': '2024-02-15 18:03:20+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 107 - LogRotation - 2 hosts, 1 failure ignored
    # ================================================================
    # Host log01 - all tasks succeed
    {
        'job_id': 107,
        'playbook': 'rotate-logs.yml',
        'host_id': 801,
        'task_uuid': 'task-601',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.find',
        'job_created': '2024-02-15 18:59:45+00',
        'job_started': '2024-02-15 19:00:00+00',
        'job_finished': '2024-02-15 19:02:30+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 107,
        'playbook': 'rotate-logs.yml',
        'host_id': 801,
        'task_uuid': 'task-602',
        'event': 'runner_on_ok',
        'task_action': 'community.general.archive',
        'job_created': '2024-02-15 18:59:45+00',
        'job_started': '2024-02-15 19:00:00+00',
        'job_finished': '2024-02-15 19:02:30+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host log02 - task-602 fails but is ignored
    {
        'job_id': 107,
        'playbook': 'rotate-logs.yml',
        'host_id': 802,
        'task_uuid': 'task-601',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.find',
        'job_created': '2024-02-15 18:59:45+00',
        'job_started': '2024-02-15 19:00:00+00',
        'job_finished': '2024-02-15 19:02:30+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 107,
        'playbook': 'rotate-logs.yml',
        'host_id': 802,
        'task_uuid': 'task-602',
        'event': 'runner_on_failed',
        'task_action': 'community.general.archive',
        'job_created': '2024-02-15 18:59:45+00',
        'job_started': '2024-02-15 19:00:00+00',
        'job_finished': '2024-02-15 19:02:30+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': True,  # Error is ignored
    },
    # ================================================================
    # Job 108 - MonitoringSetup - 2 hosts, all succeed
    # ================================================================
    # Host mon01 - all tasks succeed
    {
        'job_id': 108,
        'playbook': 'setup-monitoring.yml',
        'host_id': 901,
        'task_uuid': 'task-701',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.package',
        'job_created': '2024-02-15 19:58:30+00',
        'job_started': '2024-02-15 20:00:00+00',
        'job_finished': '2024-02-15 20:05:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 108,
        'playbook': 'setup-monitoring.yml',
        'host_id': 901,
        'task_uuid': 'task-702',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.template',
        'job_created': '2024-02-15 19:58:30+00',
        'job_started': '2024-02-15 20:00:00+00',
        'job_finished': '2024-02-15 20:05:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 108,
        'playbook': 'setup-monitoring.yml',
        'host_id': 901,
        'task_uuid': 'task-703',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 19:58:30+00',
        'job_started': '2024-02-15 20:00:00+00',
        'job_finished': '2024-02-15 20:05:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # Host mon02 - all tasks succeed
    {
        'job_id': 108,
        'playbook': 'setup-monitoring.yml',
        'host_id': 902,
        'task_uuid': 'task-701',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.package',
        'job_created': '2024-02-15 19:58:30+00',
        'job_started': '2024-02-15 20:00:00+00',
        'job_finished': '2024-02-15 20:05:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 108,
        'playbook': 'setup-monitoring.yml',
        'host_id': 902,
        'task_uuid': 'task-702',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.template',
        'job_created': '2024-02-15 19:58:30+00',
        'job_started': '2024-02-15 20:00:00+00',
        'job_finished': '2024-02-15 20:05:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 108,
        'playbook': 'setup-monitoring.yml',
        'host_id': 902,
        'task_uuid': 'task-703',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.service',
        'job_created': '2024-02-15 19:58:30+00',
        'job_started': '2024-02-15 20:00:00+00',
        'job_finished': '2024-02-15 20:05:15+00',
        'job_failed': False,
        'resolved_action': None,
        'ignore_errors': False,
    },
]

# Job host summary dataset - aligned with events
# Job 101: 4 hosts, 3 succeed (3 ok tasks each), 1 fails (1 ok, 1 failure)
# Job 102: 2 hosts, both succeed (2 ok tasks each)
# Job 103: 3 hosts, all succeed (2 ok tasks each)
# Job 104: 2 hosts, 1 succeeds (2 ok tasks), 1 fails (1 ok, 1 failure)
jobhostsummary = [
    # Job 101 - Host web01 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'web01',
        'job_remote_id': 101,
        'job_template_name': 'WebServerDeploy',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 101 - Host web02 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'web02',
        'job_remote_id': 101,
        'job_template_name': 'WebServerDeploy',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 101 - Host web03 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'web03',
        'job_remote_id': 101,
        'job_template_name': 'WebServerDeploy',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 101 - Host web04 (1 ok, 1 failure - matches events)
    {
        'dark': 0,
        'failures': 1,
        'ok': 1,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'web04',
        'job_remote_id': 101,
        'job_template_name': 'WebServerDeploy',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 102 - Host db01 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'db01',
        'job_remote_id': 102,
        'job_template_name': 'DatabaseConfig',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 102 - Host db02 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'db02',
        'job_remote_id': 102,
        'job_template_name': 'DatabaseConfig',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 103 - Host sec01 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'sec01',
        'job_remote_id': 103,
        'job_template_name': 'SecurityUpdate',
        'model': 'workflowjob',
        'ansible_version': '2.16.0',
        'launch_type': 'workflow',
    },
    # Job 103 - Host sec02 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'sec02',
        'job_remote_id': 103,
        'job_template_name': 'SecurityUpdate',
        'model': 'workflowjob',
        'ansible_version': '2.16.0',
        'launch_type': 'workflow',
    },
    # Job 103 - Host sec03 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'sec03',
        'job_remote_id': 103,
        'job_template_name': 'SecurityUpdate',
        'model': 'workflowjob',
        'ansible_version': '2.16.0',
        'launch_type': 'workflow',
    },
    # Job 104 - Host backup01 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'backup01',
        'job_remote_id': 104,
        'job_template_name': 'BackupOperation',
        'model': 'adhoccommand',
        'ansible_version': '2.15.0',
        'launch_type': 'callback',
    },
    # Job 104 - Host backup02 (1 ok, 1 failure - matches events)
    {
        'dark': 0,
        'failures': 1,
        'ok': 1,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'backup02',
        'job_remote_id': 104,
        'job_template_name': 'BackupOperation',
        'model': 'adhoccommand',
        'ansible_version': '2.15.0',
        'launch_type': 'callback',
    },
    # Job 105 - Host app01 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'app01',
        'job_remote_id': 105,
        'job_template_name': 'AppDeployment',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 105 - Host app02 (2 ok, 1 skipped - matches events)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 1,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'app02',
        'job_remote_id': 105,
        'job_template_name': 'AppDeployment',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 105 - Host app03 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'app03',
        'job_remote_id': 105,
        'job_template_name': 'AppDeployment',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 106 - Host net01 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'net01',
        'job_remote_id': 106,
        'job_template_name': 'NetworkConfig',
        'model': 'job',
        'ansible_version': '2.16.0',
        'launch_type': 'manual',
    },
    # Job 106 - Host net02 (unreachable - matches events)
    {
        'dark': 1,
        'failures': 0,
        'ok': 0,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'net02',
        'job_remote_id': 106,
        'job_template_name': 'NetworkConfig',
        'model': 'job',
        'ansible_version': '2.16.0',
        'launch_type': 'manual',
    },
    # Job 107 - Host log01 (2 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'log01',
        'job_remote_id': 107,
        'job_template_name': 'LogRotation',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 107 - Host log02 (1 ok, 1 failure ignored - matches events)
    {
        'dark': 0,
        'failures': 0,
        'ok': 1,
        'skipped': 0,
        'ignored': 1,  # Failure was ignored
        'rescued': 0,
        'host_name': 'log02',
        'job_remote_id': 107,
        'job_template_name': 'LogRotation',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'scheduled',
    },
    # Job 108 - Host mon01 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'mon01',
        'job_remote_id': 108,
        'job_template_name': 'MonitoringSetup',
        'model': 'workflowjob',
        'ansible_version': '2.16.0',
        'launch_type': 'workflow',
    },
    # Job 108 - Host mon02 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'mon02',
        'job_remote_id': 108,
        'job_template_name': 'MonitoringSetup',
        'model': 'workflowjob',
        'ansible_version': '2.16.0',
        'launch_type': 'workflow',
    },
]

# Execution environments dataset
execution_environments = [
    {'managed': 't'},  # Default EE
    {'managed': 'f'},  # Custom EE
    {'managed': 't'},  # Default EE
    {'managed': 'f'},  # Custom EE
    {'managed': 't'},  # Default EE
    {'managed': 'f'},  # Custom EE
]

# Credentials dataset
credentials = [
    {'credential_type': 'Machine', 'job_id': 101, 'model': 'job'},
    {'credential_type': 'Machine', 'job_id': 102, 'model': 'job'},
    {'credential_type': 'Vault', 'job_id': 101, 'model': 'job'},
    {'credential_type': 'Source Control', 'job_id': 103, 'model': 'workflowjob'},
    {'credential_type': 'Network', 'job_id': 104, 'model': 'adhoccommand'},
    {'credential_type': 'Amazon Web Services', 'job_id': 104, 'model': 'adhoccommand'},
    {'credential_type': 'Machine', 'job_id': 105, 'model': 'job'},
    {'credential_type': 'Source Control', 'job_id': 105, 'model': 'job'},
    {'credential_type': 'Network', 'job_id': 106, 'model': 'job'},
    {'credential_type': 'Machine', 'job_id': 107, 'model': 'job'},
    {'credential_type': 'Source Control', 'job_id': 108, 'model': 'workflowjob'},
]


@pytest.fixture(scope='module')
def cleanup_test_data():
    """Clean up test directories before and after all tests in this module."""
    out_dir = './out'

    # Cleanup before tests
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)

    yield  # Run all tests

    # Cleanup after all tests (commented out for debugging)
    # if os.path.exists(out_dir):
    #     shutil.rmtree(out_dir)


def create_csv_file(data_list, csv_path):
    """
    Create a CSV file from a list of dictionaries.

    Args:
        data_list: List of dictionaries to convert to CSV
        csv_path: Path where to save the CSV file

    Returns:
        The path to the created CSV file, or None if data_list is empty
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Skip creating CSV for empty data
    if not data_list:
        return None

    # Convert list of dicts to DataFrame then to CSV
    df = pd.DataFrame(data_list)
    df.to_csv(csv_path, index=False, encoding='utf-8')

    return csv_path


def test_multiple_csv_files_concatenation2(cleanup_test_data):
    """
    Test that multiple CSV files are properly concatenated and aggregated using dataset 2.

    This test uses its own independent dataset representing a realistic Ansible automation scenario.
    """

    # since = beginning of the day
    # until = beginning of the next day
    since = datetime(2024, 2, 15, 0, 0, 0)
    until = datetime(2024, 2, 16, 0, 0, 0)

    base_path = './out'
    year, month, day = since.year, since.month, since.day
    data_dir = f'{base_path}/data/{year}/{month:02d}/{day:02d}'

    # ========== Split and create CSV files for each collector ==========

    # 1. Jobs data - split into 3 CSV files
    jobs_part1 = jobs[:3]  # First 3 jobs
    jobs_part2 = jobs[3:6]  # Next 3 jobs
    jobs_part3 = jobs[6:]  # Remaining 2 jobs

    jobs_csv_files = []
    csv1 = create_csv_file(jobs_part1, f'{data_dir}/part1_unified_jobs.csv')
    if csv1:
        jobs_csv_files.append(csv1)
    csv2 = create_csv_file(jobs_part2, f'{data_dir}/part2_unified_jobs.csv')
    if csv2:
        jobs_csv_files.append(csv2)
    csv3 = create_csv_file(jobs_part3, f'{data_dir}/part3_unified_jobs.csv')
    if csv3:
        jobs_csv_files.append(csv3)

    # 2. Events data - split into 3 CSV files
    # Split roughly: part1 (jobs 101-102), part2 (jobs 103-105), part3 (jobs 106-108)
    events_part1 = events[:16]  # Jobs 101, 102, warning
    events_part2 = events[16:35]  # Jobs 103, 104, 105
    events_part3 = events[35:]  # Jobs 106, 107, 108

    events_csv_files = []
    csv1 = create_csv_file(events_part1, f'{data_dir}/part1_main_jobevent.csv')
    if csv1:
        events_csv_files.append(csv1)
    csv2 = create_csv_file(events_part2, f'{data_dir}/part2_main_jobevent.csv')
    if csv2:
        events_csv_files.append(csv2)
    csv3 = create_csv_file(events_part3, f'{data_dir}/part3_main_jobevent.csv')
    if csv3:
        events_csv_files.append(csv3)

    # 3. Execution environments - split into 3 CSV files
    ee_part1 = execution_environments[:2]
    ee_part2 = execution_environments[2:4]
    ee_part3 = execution_environments[4:]

    ee_csv_files = []
    csv1 = create_csv_file(ee_part1, f'{data_dir}/part1_execution_environments.csv')
    if csv1:
        ee_csv_files.append(csv1)
    csv2 = create_csv_file(ee_part2, f'{data_dir}/part2_execution_environments.csv')
    if csv2:
        ee_csv_files.append(csv2)
    csv3 = create_csv_file(ee_part3, f'{data_dir}/part3_execution_environments.csv')
    if csv3:
        ee_csv_files.append(csv3)

    # 4. Job host summary - split into 3 CSV files
    jhs_part1 = jobhostsummary[:6]  # Jobs 101, 102
    jhs_part2 = jobhostsummary[6:12]  # Jobs 103, 104, 105
    jhs_part3 = jobhostsummary[12:]  # Jobs 106, 107, 108

    jhs_csv_files = []
    csv1 = create_csv_file(jhs_part1, f'{data_dir}/part1_job_host_summary.csv')
    if csv1:
        jhs_csv_files.append(csv1)
    csv2 = create_csv_file(jhs_part2, f'{data_dir}/part2_job_host_summary.csv')
    if csv2:
        jhs_csv_files.append(csv2)
    csv3 = create_csv_file(jhs_part3, f'{data_dir}/part3_job_host_summary.csv')
    if csv3:
        jhs_csv_files.append(csv3)

    # 5. Credentials - split into 3 CSV files
    cred_part1 = credentials[:4]  # First 4 entries
    cred_part2 = credentials[4:8]  # Next 4 entries
    cred_part3 = credentials[8:]  # Remaining entries

    cred_csv_files = []
    csv1 = create_csv_file(cred_part1, f'{data_dir}/part1_credentials.csv')
    if csv1:
        cred_csv_files.append(csv1)
    csv2 = create_csv_file(cred_part2, f'{data_dir}/part2_credentials.csv')
    if csv2:
        cred_csv_files.append(csv2)
    csv3 = create_csv_file(cred_part3, f'{data_dir}/part3_credentials.csv')
    if csv3:
        cred_csv_files.append(csv3)

    # ========== Run the anonymized rollup computation ==========

    # Create input_data dict with lists of CSV file paths
    input_data = {
        'unified_jobs': jobs_csv_files,
        'job_host_summary': jhs_csv_files,
        'main_jobevent': events_csv_files,
        'execution_environments': ee_csv_files,
        'credentials': cred_csv_files,
    }

    result = compute_anonymized_rollup_from_raw_data(
        input_data=input_data, salt='test_salt', since=since, until=until, base_path=base_path, save_rollups=False
    )

    # print the result with pretty json
    json_content = json.dumps(result, indent=4)
    print('\n' + '=' * 80)
    print('=== ANONYMIZED ROLLUP RESULT (from multiple CSV files - dataset 2) ===')
    print('=' * 80)
    print(json_content)
    print('=' * 80)

    # save the result as json inside rollups/2024/02/15/anonymized.json
    json_path = f'./out/rollups/{year}/{month:02d}/{day:02d}/anonymized_{since.strftime("%Y-%m-%d")}_{until.strftime("%Y-%m-%d")}.json'

    # ensure the directory exists
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    with open(json_path, 'w') as f:
        print(f'Saving result to {json_path}')
        f.write(json_content)

    # ========== Validate the results ==========

    # Validate flattened structure
    assert 'statistics' in result
    assert 'jobs_by_job_type' in result
    assert 'jobs_by_launch_type' in result
    assert 'module_stats' in result
    assert 'collection_name_stats' in result
    assert 'modules_used_per_playbook' in result
    assert 'collections_versions' in result

    # ========== Validate Jobs ==========
    jobs_list = result['jobs_by_job_type']
    assert isinstance(jobs_list, list)
    assert len(jobs_list) == 3  # job, workflowjob, adhoccommand
    assert result['statistics']['jobs_total'] == 8  # Total jobs across all job types (101-108)

    # Validate job type 'job' (jobs 101, 102, 105, 106, 107)
    job_type_jobs = [j for j in jobs_list if j['job_type'] == 'job']
    assert len(job_type_jobs) == 1
    job_type = job_type_jobs[0]
    assert job_type['jobs_total'] == 5  # Jobs 101, 102, 105, 106, 107
    assert job_type['jobs_failed_total'] == 2  # Jobs 101, 106 failed
    assert job_type['jobs_succeeded_total'] == 3  # Jobs 102, 105, 107 succeeded
    assert job_type['jobs_never_started_total'] == 0

    # Validate job type 'workflowjob' (jobs 103, 108)
    workflowjob_type_jobs = [j for j in jobs_list if j['job_type'] == 'workflowjob']
    assert len(workflowjob_type_jobs) == 1
    workflowjob_type = workflowjob_type_jobs[0]
    assert workflowjob_type['jobs_total'] == 2  # Jobs 103, 108
    assert workflowjob_type['jobs_failed_total'] == 0
    assert workflowjob_type['jobs_succeeded_total'] == 2

    # Validate job type 'adhoccommand' (job 104)
    adhoccommand_type_jobs = [j for j in jobs_list if j['job_type'] == 'adhoccommand']
    assert len(adhoccommand_type_jobs) == 1
    adhoccommand_type = adhoccommand_type_jobs[0]
    assert adhoccommand_type['jobs_total'] == 1  # Job 104
    assert adhoccommand_type['jobs_failed_total'] == 1  # Job 104 failed
    assert adhoccommand_type['jobs_succeeded_total'] == 0

    # ========== Validate Job Host Summary (merged into jobs_by_job_type) ==========
    # Job type: 101(4 hosts), 102(2 hosts), 105(3 hosts), 106(2 hosts), 107(2 hosts) = 13 unique hosts
    # Workflowjob type: 103(3 hosts), 108(2 hosts) = 5 unique hosts
    # Adhoccommand type: 104(2 hosts) = 2 unique hosts

    job_type_entry = next((j for j in jobs_list if j['job_type'] == 'job'), None)
    assert job_type_entry is not None
    # Jobs 101, 102, 105, 106, 107: 4+2+3+2+2 = 13 unique hosts total
    assert job_type_entry['unique_hosts_total'] == 13
    # Job 101: 10 ok + 1 failure, Job 102: 4 ok, Job 105: 8 ok + 1 skipped, Job 106: 2 ok, Job 107: 3 ok + 1 ignored
    # Total: 10+4+8+2+3 = 27 ok, 1 failure, 1 skipped, 1 ignored
    assert job_type_entry['ok_total'] == 27
    assert job_type_entry['failures_total'] == 1  # 1 from job 101
    assert job_type_entry['skipped_total'] == 1  # 1 from job 105
    assert job_type_entry['ignored_total'] == 1  # 1 from job 107
    assert job_type_entry['dark_total'] == 1  # 1 from job 106

    workflowjob_type_entry = next((j for j in jobs_list if j['job_type'] == 'workflowjob'), None)
    assert workflowjob_type_entry is not None
    # Jobs 103, 108: 3+2 = 5 unique hosts
    assert workflowjob_type_entry['unique_hosts_total'] == 5
    # Job 103: 6 ok, Job 108: 6 ok = 12 ok total
    assert workflowjob_type_entry['ok_total'] == 12
    assert workflowjob_type_entry['failures_total'] == 0

    adhoccommand_type_entry = next((j for j in jobs_list if j['job_type'] == 'adhoccommand'), None)
    assert adhoccommand_type_entry is not None
    assert adhoccommand_type_entry['unique_hosts_total'] == 2
    assert adhoccommand_type_entry['ok_total'] == 3  # 2 from backup01 + 1 from backup02
    assert adhoccommand_type_entry['failures_total'] == 1  # 1 from backup02

    # ========== Validate Execution Environments ==========
    assert result['statistics']['execution_environments_total'] == 6
    assert result['statistics']['execution_environments_default_total'] == 3
    assert result['statistics']['execution_environments_custom_total'] == 3

    # ========== Validate Events Modules ==========
    # We should have more modules now including: ansible.posix.apt, ansible.builtin.copy, ansible.builtin.service,
    # ansible.builtin.reboot, community.postgresql.postgresql_user, community.postgresql.postgresql_db,
    # ansible.builtin.archive, community.general.s3_sync, community.docker.docker_container, ansible.netcommon.cli_config,
    # ansible.builtin.find, community.general.archive, ansible.builtin.package, ansible.builtin.template
    assert result['statistics']['modules_used_to_automate_total'] >= 8  # At least 8 modules
    assert result['statistics']['warnings_total'] == 1  # 1 warning from job 101
    assert result['statistics']['deprecations_total'] == 0

    # ========== Validate Credentials ==========
    assert result['statistics']['credential_type_machine_total'] == 4  # Jobs 101, 102, 105, 107
    assert result['statistics']['credential_type_vault_total'] == 1  # Job 101
    assert result['statistics']['credential_type_source_control_total'] == 3  # Jobs 103, 105, 108
    assert result['statistics']['credential_type_network_total'] == 2  # Jobs 104, 106
    assert result['statistics']['credential_type_amazon_web_services_total'] == 1  # Job 104

    # ========== Validate Collections Versions ==========
    collections_versions = result['collections_versions']
    assert isinstance(collections_versions, list)
    # Should have: ansible.builtin 2.15.0, ansible.posix 1.5.0, community.general 7.0.0, community.postgresql 2.0.0,
    # community.docker 3.0.0, ansible.netcommon 4.0.0
    collections_dict = {
        (c['name'], c['version']): c['job_count']
        for c in collections_versions
    }
    assert collections_dict.get(('ansible.builtin', '2.15.0')) >= 5  # Used by multiple jobs
    assert collections_dict.get(('ansible.posix', '1.5.0')) >= 2  # Jobs 101, 103, 105
    assert collections_dict.get(('community.general', '7.0.0')) >= 2  # Jobs 101, 104, 107
    assert collections_dict.get(('community.postgresql', '2.0.0')) == 1  # Job 102

    print('\n=== All validations passed for dataset 2 ===')
