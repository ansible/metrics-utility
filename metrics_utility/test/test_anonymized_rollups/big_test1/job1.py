# Job 1 Description
#
# Job Template: T1
#   - SCM type: git
#   - Job type: run
#   - Playbook: playbook1.yml
#   - Tasks (in order):
#     1. ansible.builtin.copy
#     2. ansible.builtin.file
#     3. ansible.builtin.yum
#
# Organization: Organization1
#
# Task Runs on Hosts:
#
# Host1:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (ansible.builtin.file): ok
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: successful (all tasks ok, no failures, no dark)
#
# Host2:
#   - Task 1 (ansible.builtin.copy): failed (1st attempt) -> ok (retry successful)
#   - Task 2 (ansible.builtin.file): ok
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: successful (all tasks eventually ok, no final failures, no dark)
#
# Host3:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (ansible.builtin.file): failed (1st attempt) -> ok (retry successful)
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: successful (all tasks eventually ok, no final failures, no dark)
#
# Host4:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (ansible.builtin.file): ok
#   - Task 3 (ansible.builtin.yum): failed (1st attempt) -> failed (2nd attempt) -> failed (3rd attempt, max retries)
#   Final host outcome: failed (task 3 failed after all retries)
#
# Job Final Outcome: failed
#   Reason: Host4 failed (has failures > 0), so the job is considered failed even though 3 out of 4 hosts succeeded.
#
# Summary:
#   - Total hosts: 4
#   - Successful hosts: 3 (Host1, Host2, Host3)
#   - Failed hosts: 1 (Host4)
#   - Total task runs: 12 (3 tasks × 4 hosts)
#   - Successful task runs: 11
#   - Failed task runs: 1 (Host4, Task 3, final attempt)
#   - Retried tasks: 3 (Host2 Task 1, Host3 Task 2, Host4 Task 3)
#   Note: Unreachable (dark) tasks are NOT retried per common_data.md rules

import json


# Jobs dataset
jobs = [
    {
        'id': 1,
        'started': '2024-01-15 10:00:00.000000+00',
        'finished': '2024-01-15 10:05:30.000000+00',  # 5.5 minutes
        'failed': 1,  # Failed because Host4 had failures
        'job_template_name': 'T1',
        'unified_job_template_id': 1,
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Organization1',
        'created': '2024-01-15 09:59:30.000000+00',  # 30s wait
        'model': 'job',
        'launch_type': 'manual',
        'forks': 10,
        'inventory_name': 'test-inventory',
        'inventory_id': 1,
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.15.0'},
            }
        ),
        'execution_environment_id': 1,  # EE for ansible.builtin 2.15.0 only
    },
]

# Job host summary dataset - aligned with events
# Job 1: 4 hosts
# Host1: 3 ok tasks (successful)
# Host2: 3 ok tasks (successful, Task 1 retried)
# Host3: 3 ok tasks (successful, Task 2 retried)
# Host4: 2 ok, 1 failure (failed, Task 3 failed after retries)
jobhostsummary = [
    # Job 1 - Host1 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host1',
        'job_remote_id': 1,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 1 - Host2 (3 ok tasks, Task 1 retried but eventually succeeded)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host2',
        'job_remote_id': 1,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 1 - Host3 (3 ok tasks, Task 2 failed then retried and eventually succeeded)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host3',
        'job_remote_id': 1,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 1 - Host4 (2 ok, 1 failure - Task 3 failed after all retries)
    {
        'dark': 0,
        'failures': 1,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host4',
        'job_remote_id': 1,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
]

# Events dataset - aligned with jobs and job host summaries
# Task UUIDs:
# - task-001: ansible.builtin.copy
# - task-002: ansible.builtin.file
# - task-003: ansible.builtin.yum
events = [
    # ================================================================
    # Job 1 - T1 - playbook1.yml - 4 hosts, 3 succeed, 1 fails
    # ================================================================
    # Host1 - all tasks succeed
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.file_role',
        'role': 'ansible.builtin.file_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
        'warnings': '["File permissions may be too permissive"]',
        'deprecations': None,
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host2 - Task 1 failed then ok (retry successful), Task 2 ok, Task 3 ok
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.file_role',
        'role': 'ansible.builtin.file_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
        'warnings': None,
        'deprecations': '["The yum module is deprecated, use dnf instead"]',
    },
    # Host3 - Task 1 ok, Task 2 failed then ok (retry successful), Task 3 ok
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.file_role',
        'role': 'ansible.builtin.file_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.file_role',
        'role': 'ansible.builtin.file_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host4 - Task 1 ok, Task 2 ok, Task 3 failed (3 attempts, all failed)
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.file_role',
        'role': 'ansible.builtin.file_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
        'warnings': '["Package installation failed", "Check repository configuration"]',
        'deprecations': '["The yum module is deprecated, use dnf instead"]',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 1,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Job-level warning event
    {
        'job_id': 1,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'job_created': '2024-01-15 09:59:30+00',
        'job_started': '2024-01-15 10:00:00+00',
        'job_finished': '2024-01-15 10:05:30+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
]
