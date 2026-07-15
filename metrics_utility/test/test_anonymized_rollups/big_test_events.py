"""
Big event fixture: flat list of DB rows as returned by main_jobevent_service.

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

events = [
    # =========================================================================
    # Job 1 – site.yml (failed)
    # =========================================================================

    # ── ansible.builtin.copy (t001) ──────────────────────────────────────────
    # h1 ok
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't001',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.copy',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2 failed on first attempt, then retried ok (same task_uuid)
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't001',
        'event': 'runner_on_failed', 'task_action': 'ansible.builtin.copy',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't001',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.copy',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h3 ok, but module returned a warning in event_data.res.warnings
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 3, 'task_uuid': 't001',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.copy',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False,
        'warnings': ['Consider using the file module with state=link rather than the copy module.'],
        'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h4 unreachable
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 4, 'task_uuid': 't001',
        'event': 'runner_on_unreachable', 'task_action': 'ansible.builtin.copy',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h5 ok
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 5, 'task_uuid': 't001',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.copy',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.web_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # ── ansible.builtin.package (t002, loop over [nginx, httpd, python3]) ────
    # h1: all three items ok, task ok
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't002',
        'event': 'runner_item_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't002',
        'event': 'runner_item_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't002',
        'event': 'runner_item_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't002',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2: nginx ok, httpd item unreachable, python3 item failed → task failed
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't002',
        'event': 'runner_item_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't002',
        'event': 'runner_item_on_unreachable', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't002',
        'event': 'runner_item_on_failed', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't002',
        'event': 'runner_on_failed', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h3: nginx ok, httpd item failed (ignored), python3 ok → task failed (ignored)
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 3, 'task_uuid': 't002',
        'event': 'runner_item_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 3, 'task_uuid': 't002',
        'event': 'runner_item_on_failed', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 3, 'task_uuid': 't002',
        'event': 'runner_item_on_ok', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 3, 'task_uuid': 't002',
        'event': 'runner_on_failed', 'task_action': 'ansible.builtin.package',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # ── ansible.posix.firewalld (t003) ───────────────────────────────────────
    # h1 ok
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't003',
        'event': 'runner_on_ok', 'task_action': 'ansible.posix.firewalld',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2 failed but ignored
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't003',
        'event': 'runner_on_failed', 'task_action': 'ansible.posix.firewalld',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h3 ok, but module returned a deprecation in event_data.res.deprecations
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 3, 'task_uuid': 't003',
        'event': 'runner_on_ok', 'task_action': 'ansible.posix.firewalld',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None,
        'deprecations': ['The "permanent" parameter default value will change to True in a future release.'],
        'ansible_version': '2.16.0',
    },
    # h5 skipped (when: condition was false) — excluded by design, not in _RUNNER_EVENTS
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 5, 'task_uuid': 't003',
        'event': 'runner_on_skipped', 'task_action': 'ansible.posix.firewalld',
        'resolved_action': None, 'resolved_role': None, 'role': 'acme.app.firewall_role',
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # ── ansible.builtin.systemd (t004, async fire-and-forget) ────────────────
    # h1 async ok
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 1, 'task_uuid': 't004',
        'event': 'runner_on_async_ok', 'task_action': 'ansible.builtin.systemd',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h2 async failed
    {
        'job_id': 1, 'playbook': 'site.yml', 'host_id': 2, 'task_uuid': 't004',
        'event': 'runner_on_async_failed', 'task_action': 'ansible.builtin.systemd',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # ── top-level warning (no module, no host) ────────────────────────────────
    {
        'job_id': 1, 'playbook': None, 'host_id': None, 'task_uuid': None,
        'event': 'warning', 'task_action': None,
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 10:00:00+00', 'job_started': '2024-03-01 10:00:30+00',
        'job_finished': '2024-03-01 10:15:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # =========================================================================
    # Job 2 – db.yml (success)
    # =========================================================================

    # ── community.mongodb.mongodb_replicaset (t005, async) ───────────────────
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 2, 'task_uuid': 't005',
        'event': 'runner_on_async_ok', 'task_action': 'community.mongodb.mongodb_replicaset',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 3, 'task_uuid': 't005',
        'event': 'runner_on_async_ok', 'task_action': 'community.mongodb.mongodb_replicaset',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },

    # ── community.general.ini_file (t006, loop over config sections) ──────────
    # h2: both items ok → task ok
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 2, 'task_uuid': 't006',
        'event': 'runner_item_on_ok', 'task_action': 'community.general.ini_file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 2, 'task_uuid': 't006',
        'event': 'runner_item_on_ok', 'task_action': 'community.general.ini_file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 2, 'task_uuid': 't006',
        'event': 'runner_on_ok', 'task_action': 'community.general.ini_file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    # h3: first item ok, second item failed (ignored) → task failed (ignored)
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 3, 'task_uuid': 't006',
        'event': 'runner_item_on_ok', 'task_action': 'community.general.ini_file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 3, 'task_uuid': 't006',
        'event': 'runner_item_on_failed', 'task_action': 'community.general.ini_file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 3, 'task_uuid': 't006',
        'event': 'runner_on_failed', 'task_action': 'community.general.ini_file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': True, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },

    # ── ansible.builtin.template (t007) ──────────────────────────────────────
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 2, 'task_uuid': 't007',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.template',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },
    {
        'job_id': 2, 'playbook': 'db.yml', 'host_id': 3, 'task_uuid': 't007',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.template',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },

    # ── top-level deprecated event (no module, no host) ──────────────────────
    {
        'job_id': 2, 'playbook': None, 'host_id': None, 'task_uuid': None,
        'event': 'deprecated', 'task_action': None,
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 11:00:00+00', 'job_started': '2024-03-01 11:00:20+00',
        'job_finished': '2024-03-01 11:20:00+00', 'job_failed': False,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.17.0',
    },

    # =========================================================================
    # Job 3 – cleanup.yml (failed)
    # =========================================================================

    # ── ansible.builtin.file (t008) ──────────────────────────────────────────
    # h1 ok
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 1, 'task_uuid': 't008',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h4 unreachable
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 4, 'task_uuid': 't008',
        'event': 'runner_on_unreachable', 'task_action': 'ansible.builtin.file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h6 ok
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 6, 'task_uuid': 't008',
        'event': 'runner_on_ok', 'task_action': 'ansible.builtin.file',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # ── community.general.yum (t009, loop over packages to remove) ────────────
    # h1: both items ok → task ok
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 1, 'task_uuid': 't009',
        'event': 'runner_item_on_ok', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 1, 'task_uuid': 't009',
        'event': 'runner_item_on_ok', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 1, 'task_uuid': 't009',
        'event': 'runner_on_ok', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h4 skipped (host unreachable above, task skipped by Ansible) — excluded by design
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 4, 'task_uuid': 't009',
        'event': 'runner_on_skipped', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    # h6: first item failed, second item ok → task failed
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 6, 'task_uuid': 't009',
        'event': 'runner_item_on_failed', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 6, 'task_uuid': 't009',
        'event': 'runner_item_on_ok', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
    {
        'job_id': 3, 'playbook': 'cleanup.yml', 'host_id': 6, 'task_uuid': 't009',
        'event': 'runner_on_failed', 'task_action': 'community.general.yum',
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },

    # ── top-level warning event ───────────────────────────────────────────────
    {
        'job_id': 3, 'playbook': None, 'host_id': None, 'task_uuid': None,
        'event': 'warning', 'task_action': None,
        'resolved_action': None, 'resolved_role': None, 'role': None,
        'job_created': '2024-03-01 12:00:00+00', 'job_started': '2024-03-01 12:00:10+00',
        'job_finished': '2024-03-01 12:05:00+00', 'job_failed': True,
        'ignore_errors': False, 'warnings': None, 'deprecations': None,
        'ansible_version': '2.16.0',
    },
]
