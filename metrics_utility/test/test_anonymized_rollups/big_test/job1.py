"""
Consistent test data for a single job run.

This file contains:
- job: Single job entry with all required fields
- job_hostsummaries: Array of job host summaries for this job
- events: Array of events for this job

All data is internally consistent:
- Job host summaries match the job (job_remote_id, job_template_name, model, etc.)
- Events match the job (job_id)
- Task counts and outcomes in job host summaries match the events
- Task reruns, failures, skipped tasks, and unreachable hosts are properly represented
"""

import json

# ============================================================================
# JOB DATA
# ============================================================================
# Job ID: 1001
# Template: T1
# Model: job
# Launch type: manual
# Ansible version: 2.9.0
# Duration: 30 seconds (started to finished)
# Waiting time: 5 seconds (created to started)
# Status: Failed (because h4 has failures/unreachable; h2's task eventually succeeds on rerun)
# 4 hosts, 3 tasks per host = 13 total task executions (h2 has 1 retry, so 4 executions instead of 3)
# ============================================================================

job = {
    'id': 1001,
    'started': '2024-01-01 10:00:00.000000+00',
    'finished': '2024-01-01 10:00:30.000000+00',  # +30s duration
    'failed': 1,  # Job failed because h2 and h4 have failures
    'job_template_name': 'T1',
    'controller_node': 'ctrl-A',
    'ansible_version': '2.9.0',
    'organization_name': 'Org1',
    'created': '2024-01-01 09:59:55.000000+00',  # 5s before started (waiting time)
    'model': 'job',
    'launch_type': 'manual',
    'forks': 5,
    'inventory_name': 'inventory1',
    'scm_type': 'git',
    'installed_collections': json.dumps({
        'ansible.builtin': {'version': '2.9.10'},
        'ansible.windows': {'version': '1.0.0'},
        'ansible.netcommon': {'version': '1.0.0'},
        'community.general': {'version': '1.0.0'},
    }),
}

# ============================================================================
# JOB HOST SUMMARIES
# ============================================================================
# 4 hosts, each with 3 tasks
# 
# h1: All 3 tasks succeed (3 ok, 0 failures, 0 skipped, 0 dark)
# h2: 2 tasks succeed, 1 task fails then succeeds on rerun (3 ok, 1 failure, 0 skipped, 0 dark)
#     Note: The failure execution is counted (1 failure), and the success execution is also counted (1 ok)
#     So for the rerun task: 1 failure + 1 ok. Plus 2 other tasks that succeed = 3 ok total
# h3: 2 tasks succeed, 1 task skipped (2 ok, 0 failures, 1 skipped, 0 dark)
# h4: 1 task succeeds, 1 task fails, 1 task unreachable (1 ok, 1 failure, 0 skipped, 1 dark)
#
# Totals across all hosts:
# - ok: 3 + 3 + 2 + 1 = 9
# - failures: 0 + 1 + 0 + 1 = 2
# - skipped: 0 + 0 + 1 + 0 = 1
# - dark (unreachable): 0 + 0 + 0 + 1 = 1
# ============================================================================

job_hostsummaries = [
    # Host 1: All tasks succeed
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
        'ansible_version': '2.9.0',
        'launch_type': 'manual',
    },
    # Host 2: One task fails then succeeds on rerun
    # The failure execution is counted (1 failure), and the success execution is also counted (1 ok)
    # So we have: 1 failure event + 1 success event (rerun) + 2 other success events = 3 ok, 1 failure
    {
        'dark': 0,
        'failures': 1,
        'ok': 3,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h2',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.9.0',
        'launch_type': 'manual',
    },
    # Host 3: One task skipped
    {
        'dark': 0,
        'failures': 0,
        'ok': 2,
        'skipped': 1,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h3',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.9.0',
        'launch_type': 'manual',
    },
    # Host 4: One task fails, one task unreachable
    {
        'dark': 1,  # unreachable
        'failures': 1,
        'ok': 1,
        'skipped': 0,
        'ignored': 0,
        'rescued': 0,
        'host_name': 'h4',
        'job_remote_id': 1001,
        'job_template_name': 'T1',
        'model': 'job',
        'ansible_version': '2.9.0',
        'launch_type': 'manual',
    },
]

# ============================================================================
# EVENTS
# ============================================================================
# Events for job 1001, matching the job host summaries above
#
# Host 1 (h1): 3 tasks, all succeed
#   - t001: ansible.windows.win_copy (ok)
#   - t002: community.general.yum (ok)
#   - t003: ansible.builtin.copy (ok)
#
# Host 2 (h2): 3 tasks, one fails then succeeds on rerun
#   - t001: ansible.windows.win_copy (failed, then ok on rerun)
#   - t002: community.general.yum (ok)
#   - t003: ansible.builtin.copy (ok)
#
# Host 3 (h3): 3 tasks, one skipped
#   - t001: ansible.windows.win_copy (ok)
#   - t002: community.general.yum (skipped)
#   - t003: ansible.builtin.copy (ok)
#
# Host 4 (h4): 3 tasks, one fails, one unreachable
#   - t001: ansible.windows.win_copy (ok)
#   - t002: community.general.yum (failed)
#   - t004: ansible.netcommon.cli_config (unreachable)
#
# Total events: 3 + 4 + 3 + 3 = 13 events
# ============================================================================

events = [
    # ================================================================
    # Host 1 (h1) - All tasks succeed
    # ================================================================
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 1,  # h1
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 1,  # h1
        'task_uuid': 't002',
        'event': 'runner_on_ok',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 1,  # h1
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Host 2 (h2) - Task t001 fails then succeeds on rerun
    # ================================================================
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 2,  # h2
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 2,  # h2
        'task_uuid': 't001',
        'event': 'runner_on_ok',  # Rerun succeeds
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 2,  # h2
        'task_uuid': 't002',
        'event': 'runner_on_ok',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 2,  # h2
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Host 3 (h3) - Task t002 skipped
    # ================================================================
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 3,  # h3
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 3,  # h3
        'task_uuid': 't002',
        'event': 'runner_on_skipped',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 3,  # h3
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Host 4 (h4) - Task t002 fails, task t004 unreachable
    # ================================================================
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 4,  # h4
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 4,  # h4
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
    {
        'job_id': 1001,
        'playbook': 'deploy.yml',
        'host_id': 4,  # h4
        'task_uuid': 't004',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.netcommon.cli_config',
        'job_created': '2024-01-01 09:59:55+00',
        'job_started': '2024-01-01 10:00:00+00',
        'job_finished': '2024-01-01 10:00:30+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': False,
    },
]
