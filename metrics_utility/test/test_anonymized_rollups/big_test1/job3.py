# Job 3 Description
#
# Job Template: T2
#   - SCM type: git
#   - Job type: run
#   - Playbook: playbook2.yml
#   - Tasks (in order):
#     1. ansible.builtin.copy
#     2. community.general.git
#     3. community.general.archive
#     4. community.weird.git
#
# Organization: Organization1
#
# Task Runs on Hosts:
#
# Host1:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (community.general.git): ok
#   - Task 3 (community.general.archive): ok
#   - Task 4 (community.weird.git): ok
#   Final host outcome: successful (all tasks ok, no failures, no dark)
#
# Host2:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (community.general.git): failed (1st attempt) -> ok (retry successful)
#   - Task 3 (community.general.archive): ok
#   - Task 4 (community.weird.git): ok
#   Final host outcome: successful (all tasks eventually ok, no final failures, no dark)
#
# Host3:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (community.general.git): ok
#   - Task 3 (community.general.archive): dark (unreachable)
#   - Task 4 (community.weird.git): ok
#   Final host outcome: unreachable (has dark task, even though failures == 0)
#
# Host4:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (community.general.git): ok
#   - Task 3 (community.general.archive): failed (1st attempt) -> failed (2nd attempt) -> failed (3rd attempt, max retries)
#   - Task 4 (community.weird.git): ok
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
#   - Total task runs: 16 (4 tasks × 4 hosts)
#   - Successful task runs: 15
#   - Failed task runs: 4 (Host2 Task 2 initial, Host4 Task 3 all 3 attempts)
#   - Dark task runs: 1 (Host3 Task 3)
#   - Retried tasks: 2 (Host2 Task 2, Host4 Task 3)
#   Note: Unreachable (dark) tasks are NOT retried per common_data.md rules

import json


# Jobs dataset
jobs = [
    {
        'id': 3,
        'started': '2024-01-15 12:00:00.000000+00',
        'finished': '2024-01-15 12:07:00.000000+00',  # 7 minutes
        'failed': 1,  # Failed because Host4 had failures
        'job_template_name': 'T2',
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Organization1',
        'created': '2024-01-15 11:59:30.000000+00',  # 30s wait
        'model': 'job',
        'launch_type': 'manual',
        'forks': 10,
        'inventory_name': 'test-inventory',
        'scm_type': 'git',
        'installed_collections': json.dumps(
            {
                'ansible.builtin': {'version': '2.15.0'},
                'community.general': {'version': '7.0.0'},
                'community.weird': {'version': '1.0.0'},
            }
        ),
    },
]

# Job host summary dataset - aligned with events
# Job 3: 4 hosts
# Host1: 4 ok tasks (successful)
# Host2: 4 ok tasks (successful, Task 2 retried)
# Host3: 3 ok, 1 dark (successful, Task 3 dark)
# Host4: 3 ok, 1 failure (failed, Task 3 failed after retries)
jobhostsummary = [
    # Job 3 - Host1 (4 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 4,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host1',
        'job_remote_id': 3,
        'job_template_name': 'T2',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 3 - Host2 (4 ok tasks, Task 2 retried but eventually succeeded)
    {
        'dark': 0,
        'failures': 0,
        'ok': 4,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host2',
        'job_remote_id': 3,
        'job_template_name': 'T2',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 3 - Host3 (3 ok, 1 dark - Task 3 dark)
    {
        'dark': 1,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host3',
        'job_remote_id': 3,
        'job_template_name': 'T2',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 3 - Host4 (3 ok, 1 failure - Task 3 failed after all retries)
    {
        'dark': 0,
        'failures': 1,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host4',
        'job_remote_id': 3,
        'job_template_name': 'T2',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
]

# Events dataset - aligned with jobs and job host summaries
# Task UUIDs:
# - task-001: ansible.builtin.copy
# - task-002: community.general.git
# - task-003: community.general.archive
# - task-004: community.weird.git
events = [
    # ================================================================
    # Job 3 - T2 - playbook2.yml - 4 hosts, 3 succeed, 1 fails
    # ================================================================
    # Host1 - all tasks succeed
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 1,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 1,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'community.general.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
        'warnings': None,
        'deprecations': '["Git module will be removed in Ansible 2.20"]',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 1,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'community.general.archive',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 1,
        'task_uuid': 'task-004',
        'event': 'runner_on_ok',
        'task_action': 'community.weird.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host2 - Task 1 ok, Task 2 failed then ok (retry successful), Task 3 ok, Task 4 ok
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 2,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_failed',
        'task_action': 'community.general.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'community.general.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 2,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'community.general.archive',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 2,
        'task_uuid': 'task-004',
        'event': 'runner_on_ok',
        'task_action': 'community.weird.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host3 - Task 1 ok, Task 2 ok, Task 3 dark (unreachable), Task 4 ok
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 3,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 3,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'community.general.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 3,
        'task_uuid': 'task-003',
        'event': 'runner_on_unreachable',
        'task_action': 'community.general.archive',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 3,
        'task_uuid': 'task-004',
        'event': 'runner_on_ok',
        'task_action': 'community.weird.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host4 - Task 1 ok, Task 2 ok, Task 3 failed (3 attempts, all failed), Task 4 ok
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 4,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 4,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'community.general.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'community.general.archive',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'community.general.archive',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'community.general.archive',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 3,
        'playbook': 'playbook2.yml',
        'host_id': 4,
        'task_uuid': 'task-004',
        'event': 'runner_on_ok',
        'task_action': 'community.weird.git',
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Job-level deprecated event
    {
        'job_id': 3,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'deprecated',
        'task_action': None,
        'job_created': '2024-01-15 11:59:30+00',
        'job_started': '2024-01-15 12:00:00+00',
        'job_finished': '2024-01-15 12:07:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
]
