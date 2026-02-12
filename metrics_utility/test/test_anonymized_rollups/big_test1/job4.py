# Job 4 Description
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
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (ansible.builtin.file): failed (1st attempt) -> failed (2nd attempt) -> ok (retry successful on 3rd attempt)
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: successful (all tasks eventually ok, no final failures, no dark)
#
# Host3:
#   - Task 1 (ansible.builtin.copy): dark (unreachable)
#   - Task 2 (ansible.builtin.file): ok
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: unreachable (has dark task, even though failures == 0)
#
# Host4:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (ansible.builtin.file): ok
#   - Task 3 (ansible.builtin.yum): failed (1st attempt) -> failed (2nd attempt) -> failed (3rd attempt, max retries)
#   Final host outcome: failed (task 3 failed after all retries)
#
# Job Final Outcome: failed
#   Reason: Host4 failed (has failures > 0), so the job is considered failed even though 2 out of 4 hosts succeeded (Host3 is unreachable).
#
# Summary:
#   - Total hosts: 4
#   - Successful hosts: 2 (Host1, Host2)
#   - Unreachable hosts: 1 (Host3)
#   - Failed hosts: 1 (Host4)
#   - Total task runs: 12 (3 tasks × 4 hosts)
#   - Successful task runs: 10
#   - Failed task runs: 4 (Host2 Task 2 initial 2 attempts, Host4 Task 3 all 3 attempts)
#   - Dark task runs: 1 (Host3 Task 1)
#   - Retried tasks: 2 (Host2 Task 2, Host4 Task 3)
#   Note: Unreachable (dark) tasks are NOT retried per common_data.md rules

import json


# Jobs dataset
jobs = [
    {
        'id': 4,
        'started': '2024-01-15 13:00:00.000000+00',
        'finished': '2024-01-15 13:06:30.000000+00',  # 6.5 minutes
        'failed': 1,  # Failed because Host4 had failures
        'job_template_name': 'T1',
        'controller_node': 'controller-01',
        'ansible_version': '2.16.0',
        'organization_name': 'Organization1',
        'created': '2024-01-15 12:59:30.000000+00',  # 30s wait
        'model': 'job',
        'launch_type': 'manual',
        'forks': 10,
        'inventory_name': 'test-inventory',
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.16.0'},
            }
        ),
    },
]

# Job host summary dataset - aligned with events
# Job 4: 4 hosts
# Host1: 3 ok tasks (successful)
# Host2: 3 ok tasks (successful, Task 2 retried twice)
# Host3: 2 ok, 1 dark (unreachable, Task 1 dark)
# Host4: 2 ok, 1 failure (failed, Task 3 failed after retries)
jobhostsummary = [
    # Job 4 - Host1 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host1',
        'job_remote_id': 4,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.16.0',
        'launch_type': 'manual',
    },
    # Job 4 - Host2 (3 ok tasks, Task 2 retried twice but eventually succeeded)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host2',
        'job_remote_id': 4,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.16.0',
        'launch_type': 'manual',
    },
    # Job 4 - Host3 (2 ok, 1 dark - Task 1 dark)
    {
        'dark': 1,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host3',
        'job_remote_id': 4,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.16.0',
        'launch_type': 'manual',
    },
    # Job 4 - Host4 (2 ok, 1 failure - Task 3 failed after all retries)
    {
        'dark': 0,
        'failures': 1,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host4',
        'job_remote_id': 4,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.16.0',
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
    # Job 4 - T1 - playbook1.yml - 4 hosts, 2 succeed, 1 unreachable, 1 fails
    # ================================================================
    # Host1 - all tasks succeed
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
        'warnings': '["Yum repository cache may be stale"]',
        'deprecations': '["The yum module is deprecated, use dnf instead"]',
    },
    # Host2 - Task 1 ok, Task 2 failed (1st attempt) -> failed (2nd attempt) -> ok (retry successful on 3rd attempt), Task 3 ok
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    # Host3 - Task 1 dark (unreachable), Task 2 ok, Task 3 ok
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-001',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    # Host4 - Task 1 ok, Task 2 ok, Task 3 failed (3 attempts, all failed)
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 4,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
    # Job-level warning event
    {
        'job_id': 4,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'job_created': '2024-01-15 12:59:30+00',
        'job_started': '2024-01-15 13:00:00+00',
        'job_finished': '2024-01-15 13:06:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.16.0',
    },
]
