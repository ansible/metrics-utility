import pandas as pd
import pytest

from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup


events = [
    # ================================================================
    # Job 1 – site.yml – partial failures → job_failed=True
    # ================================================================
    # Job 1 Host 1 – t001 (win_copy failed then recovered)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.windows.win_copy_role',
        'role': 'ansible.windows.win_copy_role',
        'ignore_errors': False,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't121',
        'event': 'runner_on_ok',
        'task_action': 'custom.user.collection',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'custom.standalone_role',
        'role': 'custom.standalone_role',
        'ignore_errors': False,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.windows.win_copy_role',
        'role': 'ansible.windows.win_copy_role',
        'ignore_errors': False,
    },
    # Job 1 Host 2 – t002 (yum failed final)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 1 Host 3 – t003 (mongodb insert async success)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't003',
        'event': 'runner_on_async_ok',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 1 Host 4 – t004 (cli_config unreachable)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 4,
        'task_uuid': 't004',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.netcommon.cli_config',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.netcommon.cli_config_role',
        'role': 'ansible.netcommon.cli_config_role',
        'ignore_errors': False,
    },
    # ================================================================
    # Job 2 – db.yml – async failure on one host → job_failed=True
    # ================================================================
    # Job 2 Host 1 – t003 (mongodb failed, then ok)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't003',
        'event': 'runner_on_failed',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 2 Host 2 – t002 (yum async failed final)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_on_async_failed',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 2 Host 3 – t001 (win_copy ok)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 3 – infra.yml – all success → job_failed=False
    # ================================================================
    # Job 3 Host 1 – t008 (firewalld ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 1,
        'task_uuid': 't008',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.firewalld',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:05:00+00',
        'job_finished': '2024-01-03 08:18:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': 'ansible.posix.firewalld_role',
        'role': 'ansible.posix.firewalld_role',
        'ignore_errors': False,
    },
    # Job 3 Host 2 – t009 (ec2 provision ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 2,
        'task_uuid': 't009',
        'event': 'runner_on_ok',
        'task_action': 'community.aws.ec2',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:05:00+00',
        'job_finished': '2024-01-03 08:18:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 3 Host 3 – t004 (cli_config ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 3,
        'task_uuid': 't004',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.netcommon.cli_config',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:05:00+00',
        'job_finished': '2024-01-03 08:18:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': 'ansible.netcommon.cli_config_role',
        'role': 'ansible.netcommon.cli_config_role',
        'ignore_errors': False,
    },
    # ================================================================
    # Job 4 – deploy.yml – one host failed → job_failed=True
    # ================================================================
    # Job 4 Host 4 – t008 (firewalld fail final)
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 4,
        'task_uuid': 't008',
        'event': 'runner_on_failed',
        'task_action': 'ansible.posix.firewalld',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.posix.firewalld_role',
        'role': 'ansible.posix.firewalld_role',
        'ignore_errors': False,
    },
    # Job 4 Host 5 – t001 (win_copy retried and success)
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 5,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': 'ansible.windows.win_copy_role',
        'role': 'ansible.windows.win_copy_role',
        'ignore_errors': False,
    },
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 5,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.windows.win_copy',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        # ignore_errors is not set
    },
    # Job 4 Host 6 – t009 (ec2 ok)  ← changed from mongodb.insert to ec2 to satisfy multi-host rule
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 6,
        'task_uuid': 't009',
        'event': 'runner_on_ok',
        'task_action': 'community.aws.ec2',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 4 Host 7 – t009 failed, but ignored
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 7,
        'task_uuid': 't009',
        'event': 'runner_on_failed',
        'task_action': 'community.aws.ec2',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'ignore_errors': True,
    },
    # Job 4 Host 8 – t009 skipped
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 8,
        'task_uuid': 't009',
        'event': 'runner_on_skipped',
        'task_action': 'community.aws.ec2',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Job 5 – maintenance.yml – job never started → job_started=None
    # ================================================================
    # Job 5 Host 1 – t002 (yum task that was queued but job never started, cancelled immediately)
    {
        'job_id': 5,
        'playbook': 'maintenance.yml',
        'host_id': 9,
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-06 10:00:00+00',
        'job_started': None,
        'job_finished': '2024-01-06 10:00:05+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # ================================================================
    # Warning and Deprecated events (job-level annotation events)
    # These don't have task_uuid, host_id, module_name, etc.
    # ================================================================
    # Job 1 - warning event
    {
        'job_id': 1,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 2 - warning event
    {
        'job_id': 2,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'warning',
        'task_action': None,
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
    # Job 3 - deprecated event
    {
        'job_id': 3,
        'playbook': None,
        'host_id': None,
        'task_uuid': None,
        'event': 'deprecated',
        'task_action': None,
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:05:00+00',
        'job_finished': '2024-01-03 08:18:00+00',
        'job_failed': False,
        'resolved_action': None,
        'resolved_role': None,
        'role': None,
        'ignore_errors': False,
    },
]


def test_events_modules_aggregations_basic():
    df = pd.DataFrame(events)
    # ensure string-typed columns for .str-based filtering in prepare_data
    # Handle None values for warning/deprecated events which don't have these fields
    for col in ['host_id', 'job_id', 'playbook']:
        df[col] = df[col].astype(str).replace('None', None)
    # provide default event_data for ignore_errors lookup in prepare_data
    df['event_data'] = [{}] * len(df)
    df['event_data_length'] = 10
    events_modules_anonymized_rollup = EventModulesAnonymizedRollup()
    prepared = events_modules_anonymized_rollup.prepare(df)
    result = events_modules_anonymized_rollup.base(prepared)
    result = result['json']

    import pprint

    pprint.pprint(result)

    # Assert total modules used to automate
    assert result['modules_used_to_automate_total'] == 7

    # total modules used per playbook (current aggregation)
    assert result['modules_used_per_playbook_total'] == {
        'db.yml': 3,
        'deploy.yml': 3,
        'infra.yml': 3,
        'site.yml': 5,
        'maintenance.yml': 1,
    }

    assert 'hosts_automated_total' not in result

    # collection stats assertions (current aggregation schema)
    coll_by_name = {row['collection']: row for row in result['collection_stats']}

    # Verify per-module stats (aligned to current aggregation output)
    stats_by_module = {row['module']: row for row in result['module_stats']}

    # Verify per-role stats (aligned to current aggregation output)
    stats_by_role = {row['role'] if row['role'] is not None else 'None': row for row in result['role_stats']}

    # Verify role_stats exists and has data
    assert 'role_stats' in result
    assert len(result['role_stats']) > 0

    # Verify a specific role has stats (ansible.windows.win_copy_role)
    if 'ansible.windows.win_copy_role' in stats_by_role:
        win_copy_role_stats = stats_by_role['ansible.windows.win_copy_role']
        assert win_copy_role_stats['collected_events_total'] > 0
        assert 'jobs_total' in win_copy_role_stats
        # Verify collection and collection_source are present
        assert 'collection' in win_copy_role_stats, 'role_stats should have collection field'
        assert 'collection_source' in win_copy_role_stats, 'role_stats should have collection_source field'
        # For collection-based roles, collection should be extracted (ansible.windows.win_copy_role -> ansible.windows)
        assert win_copy_role_stats['collection'] == 'ansible.windows', (
            f"Expected collection 'ansible.windows', got {win_copy_role_stats['collection']}"
        )
        assert win_copy_role_stats['collection_source'] == 'certified', (
            f"Expected collection_source 'certified', got {win_copy_role_stats['collection_source']}"
        )

    # Verify standalone role (custom.standalone_role) has Custom collection_source
    if 'custom.standalone_role' in stats_by_role:
        standalone_role_stats = stats_by_role['custom.standalone_role']
        # Standalone roles should have None collection and Custom collection_source
        assert standalone_role_stats.get('collection') is None or standalone_role_stats.get('collection') == '', (
            f'Standalone role should have None collection, got {standalone_role_stats.get("collection")}'
        )
        assert standalone_role_stats['collection_source'] == 'Custom', (
            f"Standalone role should have 'Custom' collection_source, got {standalone_role_stats['collection_source']}"
        )

    # ansible.windows.win_copy (certified)
    # Events: Job1/H1 failed+ok (retry), Job2/H3 ok, Job4/H5 failed+ok (retry) → 5 events
    copy_stats = stats_by_module['ansible.windows.win_copy']
    assert copy_stats['collection_source'] == 'certified'
    assert copy_stats['runner_on_ok_total'] == 3  # Job1/H1 ok, Job2/H3 ok, Job4/H5 ok
    assert copy_stats['runner_on_failed_total'] == 2  # Job1/H1 failed, Job4/H5 failed
    assert copy_stats['ignore_errors_total'] == 0
    assert copy_stats['runner_on_unreachable_total'] == 0
    assert copy_stats['runner_on_async_ok_total'] == 0
    assert copy_stats['runner_on_async_failed_total'] == 0
    assert copy_stats['runner_item_on_ok_total'] == 0
    assert copy_stats['runner_item_on_failed_total'] == 0
    assert copy_stats['jobs_total'] == 3
    assert copy_stats['jobs_never_started_total'] == 0
    assert copy_stats['collected_events_total'] == 5
    assert 'ansible_versions' in copy_stats, 'Should have ansible_versions field'
    assert isinstance(copy_stats['ansible_versions'], list), 'ansible_versions should be a list'

    # ansible.netcommon.cli_config (certified)
    # Events: Job1/H4 unreachable, Job3/H3 item_ok → 2 events
    template_stats = stats_by_module['ansible.netcommon.cli_config']
    assert template_stats['collection_source'] == 'certified'
    assert template_stats['runner_on_ok_total'] == 0
    assert template_stats['runner_on_failed_total'] == 0
    assert template_stats['runner_on_unreachable_total'] == 1
    assert template_stats['runner_item_on_ok_total'] == 1
    assert template_stats['jobs_total'] == 2
    assert template_stats['jobs_never_started_total'] == 0
    assert template_stats['collected_events_total'] == 2
    assert 'ansible_versions' in template_stats, 'Should have ansible_versions field'
    assert isinstance(template_stats['ansible_versions'], list), 'ansible_versions should be a list'

    # ansible.posix.firewalld (certified)
    # Events: Job3/H1 ok, Job4/H4 failed → 2 events
    firewalld_stats = stats_by_module['ansible.posix.firewalld']
    assert firewalld_stats['collection_source'] == 'certified'
    assert firewalld_stats['runner_on_ok_total'] == 1
    assert firewalld_stats['runner_on_failed_total'] == 1
    assert firewalld_stats['ignore_errors_total'] == 0
    assert firewalld_stats['runner_on_unreachable_total'] == 0
    assert firewalld_stats['jobs_total'] == 2
    assert firewalld_stats['jobs_never_started_total'] == 0
    assert firewalld_stats['collected_events_total'] == 2

    # community.aws.ec2 (community)
    # Events: Job3/H2 ok, Job4/H6 ok, Job4/H7 failed(ignored) → 3 events
    # Job4/H8 runner_on_skipped is excluded from analysis by design
    ec2_stats = stats_by_module['community.aws.ec2']
    assert ec2_stats['collection_source'] == 'community'
    assert ec2_stats['runner_on_ok_total'] == 2
    assert ec2_stats['runner_on_failed_total'] == 1  # Job4/H7 (ignore_errors=True, still counted unconditionally)
    assert ec2_stats['ignore_errors_total'] == 1  # Job4/H7 ignore_errors=True
    assert ec2_stats['runner_on_unreachable_total'] == 0
    assert ec2_stats['jobs_total'] == 2
    assert ec2_stats['jobs_never_started_total'] == 0
    assert ec2_stats['collected_events_total'] == 3  # skipped event is excluded

    # community.general.yum (community)
    # Events: Job1/H2 failed, Job2/H2 async_failed, Job5/H9 failed → 3 events
    yum_stats = stats_by_module['community.general.yum']
    assert yum_stats['collection_source'] == 'community'
    assert yum_stats['runner_on_ok_total'] == 0
    assert yum_stats['runner_on_failed_total'] == 2  # Job1/H2, Job5/H9
    assert yum_stats['runner_on_async_failed_total'] == 1  # Job2/H2
    assert yum_stats['ignore_errors_total'] == 0
    assert yum_stats['jobs_total'] == 3
    assert yum_stats['jobs_never_started_total'] == 1  # Job5 has job_started=None
    assert yum_stats['collected_events_total'] == 3

    # community.mongodb.insert (community)
    # Events: Job1/H3 async_ok, Job2/H1 failed+ok (retry) → 3 events
    mongo_stats = stats_by_module['community.mongodb.insert']
    assert mongo_stats['collection_source'] == 'community'
    assert mongo_stats['runner_on_ok_total'] == 1  # Job2/H1 ok
    assert mongo_stats['runner_on_failed_total'] == 1  # Job2/H1 failed
    assert mongo_stats['ignore_errors_total'] == 0
    assert mongo_stats['runner_on_async_ok_total'] == 1  # Job1/H3 async_ok
    assert mongo_stats['runner_on_async_failed_total'] == 0
    assert mongo_stats['jobs_total'] == 2
    assert mongo_stats['jobs_never_started_total'] == 0
    assert mongo_stats['collected_events_total'] == 3

    # custom.user.collection (Custom)
    # Events: Job1/H1 ok → 1 event
    custom_stats = stats_by_module['custom.user.collection']
    assert custom_stats['collection_source'] == 'Custom'
    assert custom_stats['runner_on_ok_total'] == 1
    assert custom_stats['runner_on_failed_total'] == 0
    assert custom_stats['jobs_total'] == 1
    assert custom_stats['jobs_never_started_total'] == 0
    assert custom_stats['collected_events_total'] == 1

    # collection_stats assertions

    # ansible.netcommon
    netcommon_coll = coll_by_name['ansible.netcommon']
    assert netcommon_coll['collection_source'] == 'certified'
    assert netcommon_coll['jobs_total'] == 2
    assert netcommon_coll['jobs_never_started_total'] == 0
    assert netcommon_coll['jobs_duration_total_seconds'] == pytest.approx(1320.0)
    assert netcommon_coll['jobs_waiting_time_total_seconds'] == pytest.approx(360.0)
    assert netcommon_coll['jobs_failed_total'] == 1
    assert netcommon_coll['runner_on_ok_total'] == 0
    assert netcommon_coll['runner_on_unreachable_total'] == 1
    assert netcommon_coll['runner_item_on_ok_total'] == 1
    assert netcommon_coll['collected_events_total'] == 2
    assert 'ansible_versions' in netcommon_coll, 'Should have ansible_versions field'
    assert isinstance(netcommon_coll['ansible_versions'], list), 'ansible_versions should be a list'

    # ansible.posix
    posix_coll = coll_by_name['ansible.posix']
    assert posix_coll['collection_source'] == 'certified'
    assert posix_coll['jobs_total'] == 2
    assert posix_coll['jobs_never_started_total'] == 0
    assert posix_coll['jobs_duration_total_seconds'] == pytest.approx(1380.0)
    assert posix_coll['jobs_waiting_time_total_seconds'] == pytest.approx(900.0)
    assert posix_coll['jobs_failed_total'] == 1
    assert posix_coll['runner_on_ok_total'] == 1
    assert posix_coll['runner_on_failed_total'] == 1
    assert posix_coll['collected_events_total'] == 2

    # ansible.windows
    windows_coll = coll_by_name['ansible.windows']
    assert windows_coll['collection_source'] == 'certified'
    assert windows_coll['jobs_total'] == 3
    assert windows_coll['jobs_never_started_total'] == 0
    assert windows_coll['jobs_duration_total_seconds'] == pytest.approx(2100.0)
    assert windows_coll['jobs_waiting_time_total_seconds'] == pytest.approx(900.0)
    assert windows_coll['jobs_failed_total'] == 3
    assert windows_coll['runner_on_ok_total'] == 3
    assert windows_coll['runner_on_failed_total'] == 2
    assert windows_coll['ignore_errors_total'] == 0
    assert windows_coll['collected_events_total'] == 5

    # community.aws
    aws_coll = coll_by_name['community.aws']
    assert aws_coll['collection_source'] == 'community'
    assert aws_coll['jobs_total'] == 2
    assert aws_coll['jobs_never_started_total'] == 0
    assert aws_coll['jobs_duration_total_seconds'] == pytest.approx(1380.0)
    assert aws_coll['jobs_waiting_time_total_seconds'] == pytest.approx(900.0)
    assert aws_coll['jobs_failed_total'] == 1
    assert aws_coll['runner_on_ok_total'] == 2
    assert aws_coll['runner_on_failed_total'] == 1  # Job4/H7 (ignore_errors=True, still counted unconditionally)
    assert aws_coll['ignore_errors_total'] == 1
    assert aws_coll['collected_events_total'] == 3  # skipped event excluded

    # community.general
    general_coll = coll_by_name['community.general']
    assert general_coll['collection_source'] == 'community'
    assert general_coll['jobs_total'] == 3
    assert general_coll['jobs_never_started_total'] == 1
    assert general_coll['jobs_duration_total_seconds'] == pytest.approx(1500.0)
    assert general_coll['jobs_waiting_time_total_seconds'] == pytest.approx(300.0)
    assert general_coll['jobs_failed_total'] == 3
    assert general_coll['runner_on_ok_total'] == 0
    assert general_coll['runner_on_failed_total'] == 2
    assert general_coll['runner_on_async_failed_total'] == 1
    assert general_coll['collected_events_total'] == 3

    # community.mongodb
    mongodb_coll = coll_by_name['community.mongodb']
    assert mongodb_coll['collection_source'] == 'community'
    assert mongodb_coll['jobs_total'] == 2
    assert mongodb_coll['jobs_never_started_total'] == 0
    assert mongodb_coll['jobs_duration_total_seconds'] == pytest.approx(1500.0)
    assert mongodb_coll['jobs_waiting_time_total_seconds'] == pytest.approx(300.0)
    assert mongodb_coll['jobs_failed_total'] == 2
    assert mongodb_coll['runner_on_ok_total'] == 1
    assert mongodb_coll['runner_on_failed_total'] == 1
    assert mongodb_coll['runner_on_async_ok_total'] == 1
    assert mongodb_coll['collected_events_total'] == 3

    # custom.user
    custom_coll = coll_by_name['custom.user']
    assert custom_coll['collection_source'] == 'Custom'
    assert custom_coll['jobs_total'] == 1
    assert custom_coll['jobs_never_started_total'] == 0
    assert custom_coll['jobs_duration_total_seconds'] == pytest.approx(540.0)
    assert custom_coll['jobs_waiting_time_total_seconds'] == pytest.approx(60.0)
    assert custom_coll['jobs_failed_total'] == 1
    assert custom_coll['runner_on_ok_total'] == 1
    assert custom_coll['runner_on_failed_total'] == 0
    assert custom_coll['collected_events_total'] == 1

    # Verify warnings_total and deprecations_total
    # We added 2 warning events (job 1 and job 2) and 1 deprecated event (job 3)
    # Total events: 20 task events + 2 warnings + 1 deprecated = 23 events
    assert result['collected_events_total'] == 23, (
        f'Expected 23 total events (20 task events + 2 warnings + 1 deprecated), got {result["collected_events_total"]}'
    )
    assert result['warnings_total'] == 2, f'Expected 2 warnings, got {result["warnings_total"]}'
    assert result['deprecations_total'] == 1, f'Expected 1 deprecated event, got {result["deprecations_total"]}'

    assert 'playbook_events' not in result

    # event_data_size_total is sum of event_data_length for events in each group
    for module in result['module_stats']:
        assert module['event_data_size_total'] == 10 * module['collected_events_total']
    for collection in result['collection_stats']:
        assert collection['event_data_size_total'] == 10 * collection['collected_events_total']
    for role_stat in result['role_stats']:
        assert role_stat['event_data_size_total'] == 10 * role_stat['collected_events_total']


def test_base_renames_module_name_and_collection_name():
    """base() renames module_name->module and collection_name->collection in all stats lists."""
    rollup = EventModulesAnonymizedRollup()
    data = {
        'collected_events_total': 1,
        'warnings_total': 0,
        'deprecations_total': 0,
        'module_stats': [
            {
                'module_name': 'cisco.ios.ios_command',
                'collection_source': 'certified',
                'collection_name': 'cisco.ios',
                'jobs_total': 1,
                'collected_events_total': 1,
            },
        ],
        'collection_stats': [
            {'collection_name': 'cisco.ios', 'jobs_total': 1, 'collection_source': 'certified', 'collected_events_total': 1},
        ],
        'role_stats': [
            {
                'role': 'my_role',
                'jobs_total': 1,
                'collection_name': 'cisco.ios',
                'collection_source': 'certified',
                'collected_events_total': 1,
            },
        ],
        'unique_modules': ['cisco.ios.ios_command'],
        'modules_per_playbook': {},
        'unique_hosts': ['host1'],
    }
    result = rollup.base(data)['json']

    assert 'module' in result['module_stats'][0]
    assert 'module_name' not in result['module_stats'][0]
    assert result['module_stats'][0]['module'] == 'cisco.ios.ios_command'

    assert 'collection' in result['module_stats'][0]
    assert 'collection_name' not in result['module_stats'][0]
    assert result['module_stats'][0]['collection'] == 'cisco.ios'

    assert 'collection' in result['collection_stats'][0]
    assert 'collection_name' not in result['collection_stats'][0]

    assert 'collection' in result['role_stats'][0]
    assert 'collection_name' not in result['role_stats'][0]

    # Identity keys stay at the front after rename.
    assert list(result['module_stats'][0].keys())[:3] == ['module', 'collection', 'collection_source']
    assert list(result['collection_stats'][0].keys())[:2] == ['collection', 'collection_source']
    assert list(result['role_stats'][0].keys())[:3] == ['role', 'collection', 'collection_source']


def test_base_handles_items_without_module_name():
    """base() does not fail when module_name or collection_name are absent."""
    rollup = EventModulesAnonymizedRollup()
    data = {
        'collected_events_total': 1,
        'warnings_total': 0,
        'deprecations_total': 0,
        'module_stats': [{'collection_source': 'certified', 'jobs_total': 1}],
        'collection_stats': [{'collection_source': 'certified', 'jobs_total': 1}],
        'role_stats': [],
        'unique_modules': [],
        'modules_per_playbook': {},
        'unique_hosts': [],
    }
    result = rollup.base(data)['json']
    assert 'module_name' not in result['module_stats'][0]
    assert 'collection_name' not in result['collection_stats'][0]


def test_bare_role_name_kept_in_role_stats():
    """A plain, non-namespaced role name (e.g. "webserver") must not be dropped from role_stats.

    Ansible reports task._role._role_name verbatim, and most locally-authored
    roles are a single bare word with no dot -- unlike Galaxy-style
    "namespace.role" / "namespace.collection.role" names. extract_role_name()
    only recognises the dotted forms, so the rollup must keep the raw name
    instead of silently discarding the role.
    """
    bare_role_events = [
        {
            'job_id': 100,
            'playbook': 'webserver.yml',
            'host_id': 1,
            'task_uuid': 't100',
            'event': 'runner_on_ok',
            'task_action': 'ansible.builtin.service',
            'job_created': '2024-02-01 00:00:00+00',
            'job_started': '2024-02-01 00:01:00+00',
            'job_finished': '2024-02-01 00:05:00+00',
            'job_failed': False,
            'resolved_action': None,
            'resolved_role': None,
            'role': 'webserver',
            'ignore_errors': False,
        },
    ]

    df = pd.DataFrame(bare_role_events)
    for col in ['host_id', 'job_id', 'playbook']:
        df[col] = df[col].astype(str)
    df['event_data'] = [{}] * len(df)
    df['event_data_length'] = 10

    rollup = EventModulesAnonymizedRollup()
    prepared = rollup.prepare(df)
    result = rollup.base(prepared)['json']

    stats_by_role = {row['role']: row for row in result['role_stats']}
    assert 'webserver' in stats_by_role, f'Expected bare role "webserver" in role_stats, got {list(stats_by_role)}'

    webserver_stats = stats_by_role['webserver']
    assert webserver_stats['collection'] is None
    assert webserver_stats['collection_source'] == 'Custom'
    assert webserver_stats['runner_on_ok_total'] == 1
    assert webserver_stats['jobs_total'] == 1


@pytest.mark.parametrize(
    'value,expected',
    [
        (None, False),
        (float('nan'), False),
        ([], False),
        (['a warning'], True),
        ({}, False),
        ({'msg': 'deprecated'}, True),
        ('[]', False),
        ('["a"]', True),
        ('null', False),
        ('not-json', False),
    ],
)
def test_parse_and_check_json_array(value, expected):
    """List/dict annotations must not hit pd.isnull (empty list raises ValueError)."""
    assert EventModulesAnonymizedRollup._parse_and_check_json_array(value) is expected


@pytest.mark.parametrize(
    'dataframe',
    [
        None,
        pd.DataFrame(),
        pd.DataFrame({'event': pd.Series(dtype=object)}),
    ],
)
def test_prepare_empty_dataframe_returns_empty_stats(dataframe):
    """No-event batches must not KeyError on missing columns during filtering."""
    result = EventModulesAnonymizedRollup().prepare(dataframe)
    assert result == {
        'collected_events_total': 0,
        'warnings_total': 0,
        'deprecations_total': 0,
        'module_stats': [],
        'collection_stats': [],
        'role_stats': [],
        'unique_modules': [],
        'modules_per_playbook': {},
    }
