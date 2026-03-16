# Job 2 Description
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
#   - Task 2 (ansible.builtin.file): failed (1st attempt) -> ok (retry successful)
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: successful (all tasks eventually ok, no final failures, no dark)
#
# Host3:
#   - Task 1 (ansible.builtin.copy): failed (1st attempt) -> ok (retry successful)
#   - Task 2 (ansible.builtin.file): dark (unreachable)
#   - Task 3 (ansible.builtin.yum): ok
#   Final host outcome: successful (failures == 0, but has dark task)
#
# Host4:
#   - Task 1 (ansible.builtin.copy): ok
#   - Task 2 (ansible.builtin.file): ok
#   - Task 3 (ansible.builtin.yum): failed (1st attempt) -> failed (2nd attempt) -> ok (retry successful on 3rd attempt)
#   Final host outcome: successful (all tasks eventually ok, no final failures, no dark)
#
# Job Final Outcome: successful
#   Reason: All hosts succeeded (all have failures == 0), even though Host3 has a dark task.
#
# Summary:
#   - Total hosts: 4
#   - Successful hosts: 4 (all hosts succeeded)
#   - Failed hosts: 0
#   - Total task runs: 13 (3 tasks × 4 hosts + 1 retry for Host2, 1 retry for Host3, 2 retries for Host4)
#   - Successful task runs: 12 (all tasks eventually succeeded)
#   - Failed task runs: 3 (Host2 Task 2 initial, Host3 Task 1 initial, Host4 Task 3 initial 2 attempts)
#   - Dark task runs: 1 (Host3 Task 2)
#   - Retried tasks: 3 (Host2 Task 2, Host3 Task 1, Host4 Task 3)
#   Note: Unreachable (dark) tasks are NOT retried per common_data.md rules



# Jobs dataset
jobs = [
    {
        'id': 2,
        'started': '2024-01-15 11:00:00.000000+00',
        'finished': '2024-01-15 11:06:00.000000+00',  # 6 minutes
        'failed': 0,  # Successful because all hosts succeeded
        'job_template_name': 'T1',
        'unified_job_template_id': 1,
        'controller_node': 'controller-01',
        'ansible_version': '2.15.0',
        'organization_name': 'Organization1',
        'created': '2024-01-15 10:59:30.000000+00',  # 30s wait
        'model': 'job',
        'launch_type': 'manual',
        'forks': 10,
        'inventory_name': 'test-inventory',
        'inventory_id': 1,
        'scm_type': 'git',
        'installed_collections': {
            'ansible.builtin': {'version': '2.15.0'},
        },
    },
]

# Job host summary dataset - aligned with events
# Job 2: 4 hosts, all successful
# Host1: 3 ok tasks (successful)
# Host2: 3 ok tasks (successful, Task 2 retried)
# Host3: 2 ok, 1 dark (successful, Task 1 retried, Task 2 dark)
# Host4: 3 ok tasks (successful, Task 3 retried twice)
jobhostsummary = [
    # Job 2 - Host1 (3 ok tasks)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host1',
        'job_remote_id': 2,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 2 - Host2 (3 ok tasks, Task 2 retried but eventually succeeded)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host2',
        'job_remote_id': 2,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 2 - Host3 (2 ok, 1 dark - Task 1 retried and succeeded, Task 2 dark)
    {
        'dark': 1,
        'failures': 0,
        'ok': 2,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host3',
        'job_remote_id': 2,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.15.0',
        'launch_type': 'manual',
    },
    # Job 2 - Host4 (3 ok tasks, Task 3 retried twice and eventually succeeded)
    {
        'dark': 0,
        'failures': 0,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'Host4',
        'job_remote_id': 2,
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
    # Job 2 - T1 - playbook1.yml - 4 hosts, all succeed
    # ================================================================
    # Host1 - all tasks succeed
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'job_created': '2024-01-15 10:59:30+00',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
        'warnings': '["Copy operation completed but file size differs"]',
        'deprecations': None,
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 1,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host2 - Task 1 ok, Task 2 failed then ok (retry successful), Task 3 ok
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 2,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host3 - Task 1 failed then ok (retry successful), Task 2 dark (unreachable), Task 3 ok
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-002',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 3,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Host4 - Task 1 ok, Task 2 ok, Task 3 failed (1st attempt) -> failed (2nd attempt) -> ok (retry successful on 3rd attempt)
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': 'ansible.builtin.copy_role',
        'role': 'ansible.builtin.copy_role',
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    {
        'job_id': 2,
        'playbook': 'playbook1.yml',
        'host_id': 4,
        'task_uuid': 'task-003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.yum',
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
    # Job-level warning event
    {
        'job_id': 2,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'job_created': '2024-01-15 10:59:30+00',
        'job_started': '2024-01-15 11:00:00+00',
        'job_finished': '2024-01-15 11:06:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
        'ansible_version': '2.15.0',
    },
]
