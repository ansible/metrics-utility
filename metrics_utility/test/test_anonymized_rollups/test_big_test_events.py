"""
Tests for EventModulesAnonymizedRollup using a comprehensive event fixture.

Three jobs covering every Ansible event type tracked by EventModulesAnonymizedRollup:

  Job 1 – site.yml    failed   2024-03-01 10:00–10:15   ansible 2.16.0
  Job 2 – db.yml      success  2024-03-01 11:00–11:20   ansible 2.17.0
  Job 3 – cleanup.yml failed   2024-03-01 12:00–12:05   ansible 2.16.0

Event types exercised:
  runner_on_ok                    runner_item_on_ok
  runner_on_failed                runner_item_on_failed
  runner_on_failed   (ignored)    runner_item_on_failed  (ignored)
  runner_on_unreachable           runner_item_on_unreachable
  runner_on_async_ok              runner_on_skipped  (excluded by design)
  runner_on_async_failed
  warning  (top-level, no module)
  deprecated  (top-level, no module)
  warnings / deprecations fields from event_data.res (module-level annotations)

Expected totals after processing:
  collected_events_total : 47
  warnings_total         : 2   (top-level warning events)
  deprecations_total     : 1   (top-level deprecated event)
"""

import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup


# ---------------------------------------------------------------------------
# Fixture data – flat list of DB rows as returned by main_jobevent_service
# ---------------------------------------------------------------------------

_EVENTS = [
    # =========================================================================
    # Job 1 – site.yml (failed)
    # =========================================================================
    # ── ansible.builtin.copy (t001) ──────────────────────────────────────────
    # h1 ok
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2 failed on first attempt, then retried ok (same task_uuid)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h3 ok, but module returned a warning in event_data.res.warnings
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': ['Consider using the file module with state=link rather than the copy module.'],
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h4 unreachable
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 4,
        'task_uuid': 't001',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.builtin.copy',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h5 ok
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 5,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── ansible.builtin.package (t002, loop over [nginx, httpd, python3]) ────
    # h1: all three items ok, task ok
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't002',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't002',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't002',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2: nginx ok, httpd item unreachable, python3 item failed → task failed
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_item_on_unreachable',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_item_on_failed',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h3: nginx ok, httpd item failed (ignored), python3 ok → task failed (ignored)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't002',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't002',
        'event': 'runner_item_on_failed',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't002',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.package',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── ansible.posix.firewalld (t003) ───────────────────────────────────────
    # h1 ok
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.firewalld',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2 failed but ignored
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't003',
        'event': 'runner_on_failed',
        'task_action': 'ansible.posix.firewalld',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h3 ok, but module returned a deprecation in event_data.res.deprecations
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.firewalld',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': ['The "permanent" parameter default value will change to True in a future release.'],
        'ansible_version': '2.16.0',
    },
    # h5 skipped (when: condition was false) — excluded by design, not in _RUNNER_EVENTS
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 5,
        'task_uuid': 't003',
        'event': 'runner_on_skipped',
        'task_action': 'ansible.posix.firewalld',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── ansible.builtin.systemd (t004, async fire-and-forget) ────────────────
    # h1 async ok
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't004',
        'event': 'runner_on_async_ok',
        'task_action': 'ansible.builtin.systemd',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2 async failed
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't004',
        'event': 'runner_on_async_failed',
        'task_action': 'ansible.builtin.systemd',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── top-level warning (no module, no host) ────────────────────────────────
    {
        'job_id': 1,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 10:00:00+00',
        'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # =========================================================================
    # Job 2 – db.yml (success)
    # =========================================================================
    # ── community.mongodb.mongodb_replicaset (t005, async) ───────────────────
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't005',
        'event': 'runner_on_async_ok',
        'task_action': 'community.mongodb.mongodb_replicaset',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't005',
        'event': 'runner_on_async_ok',
        'task_action': 'community.mongodb.mongodb_replicaset',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    # ── community.general.ini_file (t006, loop over config sections) ──────────
    # h2: both items ok → task ok
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't006',
        'event': 'runner_item_on_ok',
        'task_action': 'community.general.ini_file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't006',
        'event': 'runner_item_on_ok',
        'task_action': 'community.general.ini_file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't006',
        'event': 'runner_on_ok',
        'task_action': 'community.general.ini_file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    # h3: first item ok, second item failed (ignored) → task failed (ignored)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't006',
        'event': 'runner_item_on_ok',
        'task_action': 'community.general.ini_file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't006',
        'event': 'runner_item_on_failed',
        'task_action': 'community.general.ini_file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't006',
        'event': 'runner_on_failed',
        'task_action': 'community.general.ini_file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': True,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    # ── ansible.builtin.template (t007) ──────────────────────────────────────
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't007',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.template',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't007',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.template',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    # ── top-level deprecated event (no module, no host) ──────────────────────
    {
        'job_id': 2,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'deprecated',
        'task_action': None,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 11:00:00+00',
        'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00',
        'job_failed': False,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.17.0',
    },
    # =========================================================================
    # Job 3 – cleanup.yml (failed)
    # =========================================================================
    # ── ansible.builtin.file (t008) ──────────────────────────────────────────
    # h1 ok
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 1,
        'task_uuid': 't008',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h4 unreachable
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 4,
        'task_uuid': 't008',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.builtin.file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h6 ok
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 6,
        'task_uuid': 't008',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.file',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── community.general.yum (t009, loop over packages to remove) ────────────
    # h1: both items ok → task ok
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 1,
        'task_uuid': 't009',
        'event': 'runner_item_on_ok',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 1,
        'task_uuid': 't009',
        'event': 'runner_item_on_ok',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 1,
        'task_uuid': 't009',
        'event': 'runner_on_ok',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h4 skipped (host unreachable above, task skipped by Ansible) — excluded by design
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 4,
        'task_uuid': 't009',
        'event': 'runner_on_skipped',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h6: first item failed, second item ok → task failed
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 6,
        'task_uuid': 't009',
        'event': 'runner_item_on_failed',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 6,
        'task_uuid': 't009',
        'event': 'runner_item_on_ok',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 6,
        'task_uuid': 't009',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── ansible.builtin.debug via a role with no collection prefix ───────────
    # local.cleanup_role has one dot so extract_role_name() keeps it, but only
    # one dot so extract_collection_name() returns None.  These events verify
    # that role_stats uses dropna=False and keeps null-collection-name roles.
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 1,
        'task_uuid': 't010',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.debug',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'local.cleanup_role',
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3,
        'playbook': 'cleanup.yml',
        'host_id': 6,
        'task_uuid': 't010',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.debug',
        'resolved_action': None,
        'resolved_role': None,
        'role': 'local.cleanup_role',
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # ── top-level warning event ───────────────────────────────────────────────
    {
        'job_id': 3,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'job_created': '2024-03-01 12:00:00+00',
        'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00',
        'job_failed': True,
        'ignore_errors': False,
        'warnings': None,
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
]


# ---------------------------------------------------------------------------
# Shared rollup result – parametrized to exercise both the single-batch path
# and the split+merge path so that every assertion also validates merge().
# ---------------------------------------------------------------------------


def _prepare_events(rollup, events):
    """Convert a list of event dicts to a prepared rollup dict."""
    df = pd.DataFrame(events)
    for col in ['host_id', 'job_id', 'playbook']:
        df[col] = df[col].astype(str).replace('None', None)
    return rollup.prepare(df)


@pytest.fixture(scope='module', params=['single_batch', 'two_batches_merged'])
def result(request):
    """Rollup result, parametrized over two strategies:

    single_batch       – all events in one prepare() call.
    two_batches_merged – events split by job, each half prepared separately
                         then merged with rollup.merge().  The final base()
                         output must be identical, proving counters are
                         correctly maintained across batch merges.
    """
    rollup = EventModulesAnonymizedRollup()

    if request.param == 'single_batch':
        prepared = _prepare_events(rollup, _EVENTS)
    else:
        # Split by job_id so each job's events stay in one batch.
        # Per-job deduplication (jobs_total, durations …) requires that a
        # job_id never straddles two batches (same assumption as hourly windows
        # in production).  Splitting job 1 vs jobs 2+3 means ansible.builtin
        # and community.general span both batches, exercising merge.
        first = _prepare_events(rollup, [e for e in _EVENTS if e['job_id'] == 1])
        second = _prepare_events(rollup, [e for e in _EVENTS if e['job_id'] in (2, 3)])
        prepared = rollup.merge(first, second)

    result = rollup.base(prepared)['json']
    import json

    print(f'\n=== result [{request.param}] ===')
    print(json.dumps(result, indent=2, default=str))
    return result


# ---------------------------------------------------------------------------
# Top-level counts
# ---------------------------------------------------------------------------


def test_collected_events_total(result):
    assert result['collected_events_total'] == 49  # all raw rows before any filtering


def test_top_level_warnings_and_deprecations(result):
    assert result['warnings_total'] == 2  # job1 + job3 top-level warning events
    assert result['deprecations_total'] == 1  # job2 top-level deprecated event


def test_no_hosts_automated_total_in_events_output(result):
    assert 'hosts_automated_total' not in result


def test_modules_used_to_automate_total(result):
    assert result['modules_used_to_automate_total'] == 10


def test_modules_used_per_playbook(result):
    assert result['modules_used_per_playbook_total'] == {
        'site.yml': 4,  # copy, package, firewalld, systemd
        'db.yml': 3,  # mongodb_replicaset, ini_file, template
        'cleanup.yml': 3,  # file, yum, debug
    }


# ---------------------------------------------------------------------------
# Module stats
# ---------------------------------------------------------------------------


@pytest.fixture(scope='module')
def modules(result):
    return {m['module']: m for m in result['module_stats']}


def test_module_count(result):
    assert len(result['module_stats']) == 10


def test_ansible_builtin_copy(modules):
    m = modules['ansible.builtin.copy']
    assert m['collection'] == 'ansible.builtin'
    assert m['collection_source'] == 'certified'
    # jobs
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 1
    assert m['jobs_successful_total'] == 0
    assert m['jobs_never_started_total'] == 0
    assert m['jobs_duration_total_seconds'] == pytest.approx(870.0)
    assert m['jobs_waiting_time_total_seconds'] == pytest.approx(30.0)
    assert m['jobs_successful_duration_total_seconds'] == pytest.approx(0.0)
    assert m['jobs_failed_duration_total_seconds'] == pytest.approx(870.0)
    # event counts
    assert m['runner_on_ok_total'] == 4  # h1, h2-retry, h3, h5
    assert m['runner_on_failed_total'] == 1  # h2 first attempt
    assert m['runner_on_failed_ignored_total'] == 0
    assert m['runner_on_unreachable_total'] == 1  # h4
    assert m['runner_on_async_ok_total'] == 0
    assert m['runner_on_async_failed_total'] == 0
    assert m['runner_item_on_ok_total'] == 0
    assert m['runner_item_on_failed_total'] == 0
    assert m['runner_item_on_failed_ignored_total'] == 0
    assert m['runner_item_on_unreachable_total'] == 0
    assert m['warnings_total'] == 1  # h3 module-level warning
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 6
    # host coverage: h1, h2 (two events), h3, h4, h5 → 5 distinct hosts


def test_ansible_builtin_package(modules):
    m = modules['ansible.builtin.package']
    assert m['collection'] == 'ansible.builtin'
    assert m['collection_source'] == 'certified'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 1
    # task-level event counts
    assert m['runner_on_ok_total'] == 1  # h1 task summary ok
    assert m['runner_on_failed_total'] == 1  # h2 task summary failed (not ignored)
    assert m['runner_on_failed_ignored_total'] == 1  # h3 task summary failed (ignored)
    assert m['runner_on_unreachable_total'] == 0
    assert m['runner_on_async_ok_total'] == 0
    assert m['runner_on_async_failed_total'] == 0
    # item-level event counts
    assert m['runner_item_on_ok_total'] == 6  # h1×3, h2×1, h3×2
    assert m['runner_item_on_failed_total'] == 1  # h2 item failed (not ignored)
    assert m['runner_item_on_failed_ignored_total'] == 1  # h3 item failed (ignored)
    assert m['runner_item_on_unreachable_total'] == 1  # h2 item unreachable
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 12


def test_ansible_posix_firewalld(modules):
    m = modules['ansible.posix.firewalld']
    assert m['collection'] == 'ansible.posix'
    assert m['collection_source'] == 'certified'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 1
    assert m['runner_on_ok_total'] == 2  # h1, h3
    assert m['runner_on_failed_total'] == 0
    assert m['runner_on_failed_ignored_total'] == 1  # h2 (ignore_errors=True)
    assert m['runner_on_unreachable_total'] == 0
    assert m['runner_item_on_ok_total'] == 0
    assert m['runner_item_on_failed_total'] == 0
    assert m['runner_item_on_failed_ignored_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 1  # h3 module-level deprecation
    assert m['events_processed_total'] == 3  # h5 runner_on_skipped excluded


def test_ansible_builtin_systemd(modules):
    m = modules['ansible.builtin.systemd']
    assert m['collection'] == 'ansible.builtin'
    assert m['collection_source'] == 'certified'
    assert m['jobs_total'] == 1
    assert m['runner_on_ok_total'] == 0
    assert m['runner_on_failed_total'] == 0
    assert m['runner_on_async_ok_total'] == 1  # h1
    assert m['runner_on_async_failed_total'] == 1  # h2
    assert m['runner_item_on_ok_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 2


def test_community_mongodb_mongodb_replicaset(modules):
    m = modules['community.mongodb.mongodb_replicaset']
    assert m['collection'] == 'community.mongodb'
    assert m['collection_source'] == 'community'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 0
    assert m['jobs_successful_total'] == 1
    assert m['jobs_duration_total_seconds'] == pytest.approx(1180.0)
    assert m['jobs_successful_duration_total_seconds'] == pytest.approx(1180.0)
    assert m['jobs_failed_duration_total_seconds'] == pytest.approx(0.0)
    assert m['runner_on_ok_total'] == 0
    assert m['runner_on_async_ok_total'] == 2  # h2, h3
    assert m['runner_on_async_failed_total'] == 0
    assert m['runner_item_on_ok_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 2


def test_community_general_ini_file(modules):
    m = modules['community.general.ini_file']
    assert m['collection'] == 'community.general'
    assert m['collection_source'] == 'community'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 0
    assert m['jobs_successful_total'] == 1
    assert m['runner_on_ok_total'] == 1  # h2 task summary ok
    assert m['runner_on_failed_total'] == 0
    assert m['runner_on_failed_ignored_total'] == 1  # h3 task summary failed (ignored)
    assert m['runner_item_on_ok_total'] == 3  # h2×2, h3×1
    assert m['runner_item_on_failed_total'] == 0
    assert m['runner_item_on_failed_ignored_total'] == 1  # h3 item failed (ignored)
    assert m['runner_item_on_unreachable_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 6


def test_ansible_builtin_template(modules):
    m = modules['ansible.builtin.template']
    assert m['collection'] == 'ansible.builtin'
    assert m['collection_source'] == 'certified'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 0
    assert m['jobs_successful_total'] == 1
    assert m['runner_on_ok_total'] == 2  # h2, h3
    assert m['runner_on_failed_total'] == 0
    assert m['runner_item_on_ok_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 2


def test_ansible_builtin_file(modules):
    m = modules['ansible.builtin.file']
    assert m['collection'] == 'ansible.builtin'
    assert m['collection_source'] == 'certified'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 1
    assert m['jobs_successful_total'] == 0
    assert m['jobs_duration_total_seconds'] == pytest.approx(290.0)
    assert m['jobs_failed_duration_total_seconds'] == pytest.approx(290.0)
    assert m['runner_on_ok_total'] == 2  # h1, h6
    assert m['runner_on_failed_total'] == 0
    assert m['runner_on_unreachable_total'] == 1  # h4
    assert m['runner_item_on_ok_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 3


def test_ansible_builtin_debug(modules):
    m = modules['ansible.builtin.debug']
    assert m['collection'] == 'ansible.builtin'
    assert m['collection_source'] == 'certified'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 1
    assert m['jobs_successful_total'] == 0
    assert m['runner_on_ok_total'] == 2  # h1, h6
    assert m['runner_on_failed_total'] == 0
    assert m['runner_item_on_ok_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 2


def test_community_general_yum(modules):
    m = modules['community.general.yum']
    assert m['collection'] == 'community.general'
    assert m['collection_source'] == 'community'
    assert m['jobs_total'] == 1
    assert m['jobs_failed_total'] == 1
    assert m['jobs_successful_total'] == 0
    assert m['runner_on_ok_total'] == 1  # h1 task summary ok
    assert m['runner_on_failed_total'] == 1  # h6 task summary failed
    assert m['runner_on_failed_ignored_total'] == 0
    assert m['runner_item_on_ok_total'] == 3  # h1×2, h6×1
    assert m['runner_item_on_failed_total'] == 1  # h6 item failed
    assert m['runner_item_on_failed_ignored_total'] == 0
    assert m['runner_item_on_unreachable_total'] == 0
    assert m['warnings_total'] == 0
    assert m['deprecations_total'] == 0
    assert m['events_processed_total'] == 6  # h4 runner_on_skipped excluded


# ---------------------------------------------------------------------------
# Collection stats
# ---------------------------------------------------------------------------


@pytest.fixture(scope='module')
def collections(result):
    return {c['collection']: c for c in result['collection_stats']}


def test_collection_count(result):
    assert len(result['collection_stats']) == 4


def test_ansible_builtin_collection(collections):
    c = collections['ansible.builtin']
    assert c['collection_source'] == 'certified'
    assert c['jobs_total'] == 3  # jobs 1 (copy/package/systemd), 2 (template), 3 (file)
    assert c['jobs_failed_total'] == 2  # jobs 1 and 3
    assert c['jobs_successful_total'] == 1  # job 2
    assert c['jobs_duration_total_seconds'] == pytest.approx(2340.0)  # 870+1180+290
    assert c['jobs_waiting_time_total_seconds'] == pytest.approx(60.0)  # 30+20+10
    # aggregated event counts (copy + package + systemd + template + file + debug)
    assert c['runner_on_ok_total'] == 11  # 4+1+0+2+2+2
    assert c['runner_on_failed_total'] == 2  # 1+1+0+0+0
    assert c['runner_on_failed_ignored_total'] == 1  # 0+1+0+0+0
    assert c['runner_on_unreachable_total'] == 2  # 1+0+0+0+1
    assert c['runner_on_async_ok_total'] == 1  # 0+0+1+0+0
    assert c['runner_on_async_failed_total'] == 1  # 0+0+1+0+0
    assert c['runner_item_on_ok_total'] == 6  # 0+6+0+0+0
    assert c['runner_item_on_failed_total'] == 1  # 0+1+0+0+0
    assert c['runner_item_on_failed_ignored_total'] == 1  # 0+1+0+0+0
    assert c['runner_item_on_unreachable_total'] == 1  # 0+1+0+0+0
    assert c['warnings_total'] == 1  # copy h3
    assert c['deprecations_total'] == 0
    assert c['events_processed_total'] == 27  # 6+12+2+2+3+2(debug)


def test_ansible_posix_collection(collections):
    c = collections['ansible.posix']
    assert c['collection_source'] == 'certified'
    assert c['jobs_total'] == 1
    assert c['jobs_failed_total'] == 1
    assert c['jobs_duration_total_seconds'] == pytest.approx(870.0)
    assert c['jobs_waiting_time_total_seconds'] == pytest.approx(30.0)
    assert c['runner_on_ok_total'] == 2
    assert c['runner_on_failed_total'] == 0
    assert c['runner_on_failed_ignored_total'] == 1
    assert c['runner_on_unreachable_total'] == 0
    assert c['runner_item_on_ok_total'] == 0
    assert c['warnings_total'] == 0
    assert c['deprecations_total'] == 1
    assert c['events_processed_total'] == 3


def test_community_mongodb_collection(collections):
    c = collections['community.mongodb']
    assert c['collection_source'] == 'community'
    assert c['jobs_total'] == 1
    assert c['jobs_failed_total'] == 0
    assert c['jobs_successful_total'] == 1
    assert c['jobs_duration_total_seconds'] == pytest.approx(1180.0)
    assert c['jobs_waiting_time_total_seconds'] == pytest.approx(20.0)
    assert c['runner_on_async_ok_total'] == 2
    assert c['runner_on_ok_total'] == 0
    assert c['runner_item_on_ok_total'] == 0
    assert c['warnings_total'] == 0
    assert c['deprecations_total'] == 0
    assert c['events_processed_total'] == 2


def test_community_general_collection(collections):
    c = collections['community.general']
    assert c['collection_source'] == 'community'
    assert c['jobs_total'] == 2  # job 2 (ini_file) and job 3 (yum)
    assert c['jobs_failed_total'] == 1  # job 3
    assert c['jobs_successful_total'] == 1  # job 2
    assert c['jobs_duration_total_seconds'] == pytest.approx(1470.0)  # 1180+290
    assert c['jobs_waiting_time_total_seconds'] == pytest.approx(30.0)  # 20+10
    # ini_file + yum
    assert c['runner_on_ok_total'] == 2  # 1+1
    assert c['runner_on_failed_total'] == 1  # 0+1
    assert c['runner_on_failed_ignored_total'] == 1  # 1+0
    assert c['runner_on_unreachable_total'] == 0
    assert c['runner_item_on_ok_total'] == 6  # 3+3
    assert c['runner_item_on_failed_total'] == 1  # 0+1
    assert c['runner_item_on_failed_ignored_total'] == 1  # 1+0
    assert c['runner_item_on_unreachable_total'] == 0
    assert c['warnings_total'] == 0
    assert c['deprecations_total'] == 0
    assert c['events_processed_total'] == 12  # 6+6


# ---------------------------------------------------------------------------
# Role stats
# ---------------------------------------------------------------------------


@pytest.fixture(scope='module')
def roles(result):
    return {r['role']: r for r in result['role_stats']}


def test_role_count(result):
    # Only events with an explicit role are grouped (role=None is excluded).
    # One-dot roles like local.cleanup_role have collection_name=None but are
    # still included thanks to dropna=False on the role groupby.
    assert len(result['role_stats']) == 3


def test_role_web(roles):
    r = roles['acme.app.web_role']
    assert r['collection'] == 'acme.app'
    assert r['collection_source'] == 'Custom'
    assert r['jobs_total'] == 1
    assert r['jobs_failed_total'] == 1
    assert r['runner_on_ok_total'] == 4
    assert r['runner_on_failed_total'] == 1
    assert r['runner_on_failed_ignored_total'] == 0
    assert r['runner_on_unreachable_total'] == 1
    assert r['runner_item_on_ok_total'] == 0
    assert r['warnings_total'] == 1
    assert r['deprecations_total'] == 0
    assert r['events_processed_total'] == 6
    # copy task: h1, h2 (two events), h3, h4, h5 → 5 distinct hosts


def test_role_firewall(roles):
    r = roles['acme.app.firewall_role']
    assert r['collection'] == 'acme.app'
    assert r['collection_source'] == 'Custom'
    assert r['jobs_total'] == 1
    assert r['jobs_failed_total'] == 1
    assert r['runner_on_ok_total'] == 2
    assert r['runner_on_failed_total'] == 0
    assert r['runner_on_failed_ignored_total'] == 1
    assert r['runner_on_unreachable_total'] == 0
    assert r['runner_item_on_ok_total'] == 0
    assert r['warnings_total'] == 0
    assert r['deprecations_total'] == 1
    assert r['events_processed_total'] == 3
    # firewalld task: h1, h2, h3 → 3 distinct hosts


def test_role_local_cleanup(roles):
    # local.cleanup_role has one dot so extract_role_name() keeps it, but
    # extract_collection_name() returns None — this entry only exists because
    # the role groupby uses dropna=False.
    r = roles['local.cleanup_role']
    assert r['collection'] is None
    assert r['collection_source'] == 'Custom'
    assert r['jobs_total'] == 1
    assert r['jobs_failed_total'] == 1
    assert r['runner_on_ok_total'] == 2  # h1, h6
    assert r['runner_on_failed_total'] == 0
    assert r['events_processed_total'] == 2
