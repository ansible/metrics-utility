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

    assert result['hosts_automated_total'] == 9

    # collection stats assertions (current aggregation schema)
    coll_by_name = {row['collection_name']: row for row in result['collection_name_stats']}

    # Verify per-module stats (aligned to current aggregation output)
    stats_by_module = {row['module_name']: row for row in result['module_stats']}
    # ansible.windows.win_copy (certified)
    copy_stats = stats_by_module['ansible.windows.win_copy']
    assert copy_stats['collection_source'] == 'certified'
    assert copy_stats['task_clean_success_total'] == 1
    assert copy_stats['task_success_with_reruns_total'] == 2
    assert copy_stats['task_failed_total'] == 0
    assert copy_stats['task_failed_and_ignored_total'] == 0
    assert copy_stats['task_skipped_total'] == 0
    assert copy_stats['task_unreachable_total'] == 0
    assert copy_stats['jobs_total'] == 3
    assert copy_stats['jobs_never_started_total'] == 0
    assert copy_stats['hosts_total'] == 3
    assert copy_stats['jobs_failed_because_of_module_failure_total'] == 0

    # ansible.netcommon.cli_config (certified)
    template_stats = stats_by_module['ansible.netcommon.cli_config']
    assert template_stats['collection_source'] == 'certified'
    assert template_stats['task_clean_success_total'] == 1
    assert template_stats['task_success_with_reruns_total'] == 0
    assert template_stats['task_failed_total'] == 0
    assert template_stats['task_failed_and_ignored_total'] == 0
    assert template_stats['task_skipped_total'] == 0
    assert template_stats['task_unreachable_total'] == 1
    assert template_stats['jobs_total'] == 2
    assert template_stats['jobs_never_started_total'] == 0
    assert template_stats['hosts_total'] == 2
    assert template_stats['jobs_failed_because_of_module_failure_total'] == 0

    # ansible.posix.firewalld (certified)
    firewalld_stats = stats_by_module['ansible.posix.firewalld']
    assert firewalld_stats['collection_source'] == 'certified'
    assert firewalld_stats['task_clean_success_total'] == 1
    assert firewalld_stats['task_success_with_reruns_total'] == 0
    assert firewalld_stats['task_failed_total'] == 1
    assert firewalld_stats['task_failed_and_ignored_total'] == 0
    assert firewalld_stats['task_skipped_total'] == 0
    assert firewalld_stats['task_unreachable_total'] == 0
    assert firewalld_stats['jobs_total'] == 2
    assert firewalld_stats['jobs_never_started_total'] == 0
    assert firewalld_stats['hosts_total'] == 2
    assert firewalld_stats['jobs_failed_because_of_module_failure_total'] == 1

    # community.aws.ec2 (community)
    ec2_stats = stats_by_module['community.aws.ec2']
    assert ec2_stats['collection_source'] == 'community'
    assert ec2_stats['task_clean_success_total'] == 2
    assert ec2_stats['task_success_with_reruns_total'] == 0
    assert ec2_stats['task_failed_total'] == 0
    assert ec2_stats['task_failed_and_ignored_total'] == 1
    assert ec2_stats['task_skipped_total'] == 1
    assert ec2_stats['task_unreachable_total'] == 0
    assert ec2_stats['jobs_total'] == 2
    assert ec2_stats['jobs_never_started_total'] == 0
    assert ec2_stats['hosts_total'] == 4
    assert ec2_stats['jobs_failed_because_of_module_failure_total'] == 0

    # community.general.yum (community)
    yum_stats = stats_by_module['community.general.yum']
    assert yum_stats['collection_source'] == 'community'
    assert yum_stats['task_clean_success_total'] == 0
    assert yum_stats['task_success_with_reruns_total'] == 0
    assert yum_stats['task_failed_total'] == 3
    assert yum_stats['task_failed_and_ignored_total'] == 0
    assert yum_stats['task_skipped_total'] == 0
    assert yum_stats['task_unreachable_total'] == 0
    assert yum_stats['jobs_total'] == 3
    assert yum_stats['jobs_never_started_total'] == 1
    assert yum_stats['hosts_total'] == 2
    assert yum_stats['jobs_failed_because_of_module_failure_total'] == 3

    # community.mongodb.insert (community)
    mongo_stats = stats_by_module['community.mongodb.insert']
    assert mongo_stats['collection_source'] == 'community'
    assert mongo_stats['task_clean_success_total'] == 1
    assert mongo_stats['task_success_with_reruns_total'] == 1
    assert mongo_stats['task_failed_total'] == 0
    assert mongo_stats['task_failed_and_ignored_total'] == 0
    assert mongo_stats['task_skipped_total'] == 0
    assert mongo_stats['task_unreachable_total'] == 0
    assert mongo_stats['jobs_total'] == 2
    assert mongo_stats['jobs_never_started_total'] == 0
    assert mongo_stats['hosts_total'] == 2
    assert mongo_stats['jobs_failed_because_of_module_failure_total'] == 0

    # custom.user.collection (Unknown)
    custom_stats = stats_by_module['custom.user.collection']
    assert custom_stats['collection_source'] == 'Unknown'
    assert custom_stats['task_clean_success_total'] == 1
    assert custom_stats['task_success_with_reruns_total'] == 0
    assert custom_stats['task_failed_total'] == 0
    assert custom_stats['task_failed_and_ignored_total'] == 0
    assert custom_stats['task_skipped_total'] == 0
    assert custom_stats['task_unreachable_total'] == 0
    assert custom_stats['jobs_total'] == 1
    assert custom_stats['jobs_never_started_total'] == 0
    assert custom_stats['hosts_total'] == 1
    assert custom_stats['jobs_failed_because_of_module_failure_total'] == 0

    # collection_name_stats assertions

    # ansible.netcommon
    netcommon_coll = coll_by_name['ansible.netcommon']
    assert netcommon_coll['collection_source'] == 'certified'
    assert netcommon_coll['jobs_total'] == 2
    assert netcommon_coll['jobs_never_started_total'] == 0
    assert netcommon_coll['hosts_total'] == 2
    assert netcommon_coll['job_duration_total_seconds'] == pytest.approx(1320.0)
    assert netcommon_coll['job_waiting_time_total_seconds'] == pytest.approx(360.0)
    assert netcommon_coll['jobs_containing_collection_name_failed_total'] == 1
    assert netcommon_coll['jobs_failed_because_of_module_failure_total'] == 0
    assert netcommon_coll['task_clean_success_total'] == 1
    assert netcommon_coll['task_success_with_reruns_total'] == 0
    assert netcommon_coll['task_failed_total'] == 0
    assert netcommon_coll['task_failed_and_ignored_total'] == 0
    assert netcommon_coll['task_skipped_total'] == 0
    assert netcommon_coll['task_unreachable_total'] == 1

    # ansible.posix
    posix_coll = coll_by_name['ansible.posix']
    assert posix_coll['collection_source'] == 'certified'
    assert posix_coll['jobs_total'] == 2
    assert posix_coll['jobs_never_started_total'] == 0
    assert posix_coll['hosts_total'] == 2
    assert posix_coll['job_duration_total_seconds'] == pytest.approx(1380.0)
    assert posix_coll['job_waiting_time_total_seconds'] == pytest.approx(900.0)
    assert posix_coll['jobs_containing_collection_name_failed_total'] == 1
    assert posix_coll['jobs_failed_because_of_module_failure_total'] == 1
    assert posix_coll['task_clean_success_total'] == 1
    assert posix_coll['task_success_with_reruns_total'] == 0
    assert posix_coll['task_failed_total'] == 1
    assert posix_coll['task_failed_and_ignored_total'] == 0
    assert posix_coll['task_skipped_total'] == 0
    assert posix_coll['task_unreachable_total'] == 0

    # ansible.windows
    windows_coll = coll_by_name['ansible.windows']
    assert windows_coll['collection_source'] == 'certified'
    assert windows_coll['jobs_total'] == 3
    assert windows_coll['jobs_never_started_total'] == 0
    assert windows_coll['hosts_total'] == 3
    assert windows_coll['job_duration_total_seconds'] == pytest.approx(2100.0)
    assert windows_coll['job_waiting_time_total_seconds'] == pytest.approx(900.0)
    assert windows_coll['jobs_containing_collection_name_failed_total'] == 3
    assert windows_coll['jobs_failed_because_of_module_failure_total'] == 0
    assert windows_coll['task_clean_success_total'] == 1
    assert windows_coll['task_success_with_reruns_total'] == 2
    assert windows_coll['task_failed_total'] == 0
    assert windows_coll['task_failed_and_ignored_total'] == 0
    assert windows_coll['task_skipped_total'] == 0
    assert windows_coll['task_unreachable_total'] == 0

    # community.aws
    aws_coll = coll_by_name['community.aws']
    assert aws_coll['collection_source'] == 'community'
    assert aws_coll['jobs_total'] == 2
    assert aws_coll['jobs_never_started_total'] == 0
    assert aws_coll['hosts_total'] == 4
    assert aws_coll['job_duration_total_seconds'] == pytest.approx(1380.0)
    assert aws_coll['job_waiting_time_total_seconds'] == pytest.approx(900.0)
    assert aws_coll['jobs_containing_collection_name_failed_total'] == 1
    assert aws_coll['jobs_failed_because_of_module_failure_total'] == 0
    assert aws_coll['task_clean_success_total'] == 2
    assert aws_coll['task_success_with_reruns_total'] == 0
    assert aws_coll['task_failed_total'] == 0
    assert aws_coll['task_failed_and_ignored_total'] == 1
    assert aws_coll['task_skipped_total'] == 1
    assert aws_coll['task_unreachable_total'] == 0

    # community.general
    general_coll = coll_by_name['community.general']
    assert general_coll['collection_source'] == 'community'
    assert general_coll['jobs_total'] == 3
    assert general_coll['jobs_never_started_total'] == 1
    assert general_coll['hosts_total'] == 2
    assert general_coll['job_duration_total_seconds'] == pytest.approx(1500.0)
    assert general_coll['job_waiting_time_total_seconds'] == pytest.approx(300.0)
    assert general_coll['jobs_containing_collection_name_failed_total'] == 3
    assert general_coll['jobs_failed_because_of_module_failure_total'] == 3
    assert general_coll['task_clean_success_total'] == 0
    assert general_coll['task_success_with_reruns_total'] == 0
    assert general_coll['task_failed_total'] == 3
    assert general_coll['task_failed_and_ignored_total'] == 0
    assert general_coll['task_skipped_total'] == 0
    assert general_coll['task_unreachable_total'] == 0

    # community.mongodb
    mongodb_coll = coll_by_name['community.mongodb']
    assert mongodb_coll['collection_source'] == 'community'
    assert mongodb_coll['jobs_total'] == 2
    assert mongodb_coll['jobs_never_started_total'] == 0
    assert mongodb_coll['hosts_total'] == 2
    assert mongodb_coll['job_duration_total_seconds'] == pytest.approx(1500.0)
    assert mongodb_coll['job_waiting_time_total_seconds'] == pytest.approx(300.0)
    assert mongodb_coll['jobs_containing_collection_name_failed_total'] == 2
    assert mongodb_coll['jobs_failed_because_of_module_failure_total'] == 0
    assert mongodb_coll['task_clean_success_total'] == 1
    assert mongodb_coll['task_success_with_reruns_total'] == 1
    assert mongodb_coll['task_failed_total'] == 0
    assert mongodb_coll['task_failed_and_ignored_total'] == 0
    assert mongodb_coll['task_skipped_total'] == 0
    assert mongodb_coll['task_unreachable_total'] == 0

    # custom.user
    custom_coll = coll_by_name['custom.user']
    assert custom_coll['collection_source'] == 'Unknown'
    assert custom_coll['jobs_total'] == 1
    assert custom_coll['jobs_never_started_total'] == 0
    assert custom_coll['hosts_total'] == 1
    assert custom_coll['job_duration_total_seconds'] == pytest.approx(540.0)
    assert custom_coll['job_waiting_time_total_seconds'] == pytest.approx(60.0)
    assert custom_coll['jobs_containing_collection_name_failed_total'] == 1
    assert custom_coll['jobs_failed_because_of_module_failure_total'] == 0
    assert custom_coll['task_clean_success_total'] == 1
    assert custom_coll['task_success_with_reruns_total'] == 0
    assert custom_coll['task_failed_total'] == 0
    assert custom_coll['task_failed_and_ignored_total'] == 0
    assert custom_coll['task_skipped_total'] == 0
    assert custom_coll['task_unreachable_total'] == 0

    # Verify warnings_total and deprecations_total
    # We added 2 warning events (job 1 and job 2) and 1 deprecated event (job 3)
    # Total events: 20 task events + 2 warnings + 1 deprecated = 23 events
    assert result['event_total'] == 23, f'Expected 23 total events (20 task events + 2 warnings + 1 deprecated), got {result["event_total"]}'
    assert result['warnings_total'] == 2, f'Expected 2 warnings, got {result["warnings_total"]}'
    assert result['deprecations_total'] == 1, f'Expected 1 deprecated event, got {result["deprecations_total"]}'
