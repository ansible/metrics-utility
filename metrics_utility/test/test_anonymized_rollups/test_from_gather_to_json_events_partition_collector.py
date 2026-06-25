from django.db import connection

from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.library.collectors.controller import main_jobevent_service_partition
from metrics_utility.test.util import utcdt


def test_events_partition_collector_two_hours():
    """
    Collect job events separately for each hourly partition, run the rollup
    prepare → merge → base pipeline, and assert the result.

    Data seeded by main_jobhostsummary.sql produces two partitions:
      - main_jobevent_20250613_10  (2025-06-13 10:00 – 11:00)
      - main_jobevent_20250613_11  (2025-06-13 11:00 – 12:00)

    Each partition contains:
      - 3 jobs × 2 hosts × 2 runner_on_ok events = 12 task events
      - 2 warning events + 1 deprecated event = 3 annotation events
      - 15 total rows per partition, 30 across both

    Jobs:
      - 6 total (3 per hour)
      - 1 failed: job 3 in the 10:00 hour (status='failed', duration=90s)
      - 5 successful

    Durations (seconds):
      - Hour 10: 120 + 180 + 90 = 390
      - Hour 11: 100 + 150 + 80 = 330
      - Total: 720

    Waiting times (seconds):
      - Each hour: jobs start at created+10s, +20s, +30s → 60s per hour
      - Total: 120

    Warnings/deprecations (top-level, counted before task filtering):
      - 2 warning events per hour × 2 hours = 4 total
      - 1 deprecated event per hour × 2 hours = 2 total
    """
    rollup = EventModulesAnonymizedRollup()

    # --- Collection 1: 10:00 – 11:00 hour (partition main_jobevent_20250613_10) ---
    df_10 = main_jobevent_service_partition(
        db=connection,
        since=utcdt('2025-06-13T10:00:00'),
        until=utcdt('2025-06-13T11:00:00'),
    ).gather()
    # 3 jobs × 2 hosts × 2 task events + 2 warning + 1 deprecated = 15
    assert len(df_10) == 15, f'Expected 15 rows for hour 10, got {len(df_10)}'

    prepared_10 = rollup.prepare(df_10)
    assert prepared_10['collected_events_total'] == 15
    assert prepared_10['warnings_total'] == 2
    assert prepared_10['deprecations_total'] == 1

    # --- Collection 2: 11:00 – 12:00 hour (partition main_jobevent_20250613_11) ---
    df_11 = main_jobevent_service_partition(
        db=connection,
        since=utcdt('2025-06-13T11:00:00'),
        until=utcdt('2025-06-13T12:00:00'),
    ).gather()
    # same structure as hour 10
    assert len(df_11) == 15, f'Expected 15 rows for hour 11, got {len(df_11)}'

    prepared_11 = rollup.prepare(df_11)
    assert prepared_11['collected_events_total'] == 15
    assert prepared_11['warnings_total'] == 2
    assert prepared_11['deprecations_total'] == 1

    # --- Merge both rollup results ---
    merged = rollup.merge(None, prepared_10)
    merged = rollup.merge(merged, prepared_11)
    assert merged['collected_events_total'] == 30
    assert merged['warnings_total'] == 4
    assert merged['deprecations_total'] == 2

    # --- Base: produce the final report ---
    result = rollup.base(merged)
    data = result['json']

    # Top-level counts
    assert data['collected_events_total'] == 30
    assert data['warnings_total'] == 4
    assert data['deprecations_total'] == 2
    assert data['modules_used_to_automate_total'] == 2
    assert data['hosts_automated_total'] == 2
    assert data['modules_used_per_playbook_total'] == {'default_playbook.yml': 2}

    # Module stats
    module_stats = {m['module_name']: m for m in data['module_stats']}
    assert set(module_stats.keys()) == {'ansible.builtin.yum', 'a10.acos_axapi.a10_slb_virtual_server'}

    for module_name, m in module_stats.items():
        assert m['jobs_total'] == 6, f'{module_name}: expected 6 jobs'
        assert m['jobs_successful_total'] == 5, f'{module_name}: expected 5 successful jobs'
        assert m['jobs_failed_total'] == 1, f'{module_name}: expected 1 failed job (hour 10, job 3)'
        assert m['jobs_duration_total_seconds'] == 720.0, f'{module_name}: expected 720s total duration'
        assert m['jobs_successful_duration_total_seconds'] == 630.0, f'{module_name}: expected 630s successful duration'
        assert m['jobs_failed_duration_total_seconds'] == 90.0, f'{module_name}: expected 90s failed duration (job 3)'
        assert m['jobs_waiting_time_total_seconds'] == 120.0, f'{module_name}: expected 120s total waiting time'
        assert m['task_ok_total'] == 12, f'{module_name}: expected 12 ok tasks (6 jobs × 2 hosts)'
        assert m['task_failed_total'] == 0, f'{module_name}: expected 0 failed tasks (all events are runner_on_ok)'
        assert m['task_ok_with_retries_total'] == 0
        assert m['task_unreachable_total'] == 0
        assert m['task_skipped_total'] == 0
        assert m['processed_events_total'] == 12
        assert m['tasks_total'] == 12
        assert m['unique_hosts_total'] == 2
        assert m['jobs_failed_because_of_module_failure_total'] == 0
        assert m['ansible_versions'] == ['2.9.10']
        # warnings/deprecations are 0 per module: warning/deprecated events have no host_id
        # or task_action so they are filtered out before task aggregation
        assert m['warnings_total'] == 0
        assert m['deprecations_total'] == 0

    assert module_stats['ansible.builtin.yum']['collection_source'] == 'certified'
    assert module_stats['ansible.builtin.yum']['collection_name'] == 'ansible.builtin'
    assert module_stats['a10.acos_axapi.a10_slb_virtual_server']['collection_source'] == 'community'
    assert module_stats['a10.acos_axapi.a10_slb_virtual_server']['collection_name'] == 'a10.acos_axapi'

    # Collection stats — same numeric values as module stats (one module per collection)
    collection_stats = {c['collection_name']: c for c in data['collection_stats']}
    assert set(collection_stats.keys()) == {'ansible.builtin', 'a10.acos_axapi'}

    assert collection_stats['ansible.builtin']['collection_source'] == 'certified'
    assert collection_stats['ansible.builtin']['jobs_total'] == 6
    assert collection_stats['ansible.builtin']['task_ok_total'] == 12
    assert collection_stats['ansible.builtin']['unique_hosts_total'] == 2

    assert collection_stats['a10.acos_axapi']['collection_source'] == 'community'
    assert collection_stats['a10.acos_axapi']['jobs_total'] == 6
    assert collection_stats['a10.acos_axapi']['task_ok_total'] == 12
    assert collection_stats['a10.acos_axapi']['unique_hosts_total'] == 2

    # No role stats: events have role='default_role' which is not a collection-qualified role
    assert data['role_stats'] == []
