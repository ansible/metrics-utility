import pandas as pd

from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollups


events = [
    # ================================================================
    # Job 1 – site.yml – partial failures → job_failed=True
    # ================================================================
    # Job 1 Host 1 – t001 (copy failed then recovered)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
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
    },
    # Job 1 Host 4 – t004 (template unreachable)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 4,
        'task_uuid': 't004',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.builtin.template',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
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
    },
    # Job 2 Host 3 – t001 (copy ok)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
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
    },
    # Job 3 Host 3 – t004 (template ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 3,
        'task_uuid': 't004',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.template',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:05:00+00',
        'job_finished': '2024-01-03 08:18:00+00',
        'job_failed': False,
        'resolved_action': None,
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
    },
    # Job 4 Host 5 – t001 (copy retried and success)
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 5,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 5,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
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
    },
]


def test_events_modules_aggregations_basic():
    df = pd.DataFrame(events)
    # ensure string-typed columns for .str-based filtering in prepare_data
    for col in ['host_id', 'job_id', 'playbook']:
        df[col] = df[col].astype(str)
    # provide default event_data for ignore_errors lookup in prepare_data
    df['event_data'] = [{}] * len(df)
    prepared = EventModulesAnonymizedRollups.prepare_data(df.copy())
    result = EventModulesAnonymizedRollups.events_modules_aggregations(prepared)

    import pprint

    pprint.pprint(result)

    expected_modules = {
        'ansible.builtin.copy',
        'community.general.yum',
        'community.mongodb.insert',
        'ansible.builtin.template',
        'ansible.posix.firewalld',
        'community.aws.ec2',
    }

    # list and count of unique modules
    assert set(result['list_of_modules_used_to_automate']) == expected_modules
    assert result['modules_used_to_automate_total'] == len(expected_modules)

    # average number of modules per playbook based on current aggregation
    assert result['avg_number_of_modules_used_in_a_playbooks'] == 3.25

    # total modules used per playbook (current aggregation)
    assert result['modules_used_per_playbook_total'] == {
        'db.yml': 3,
        'deploy.yml': 3,
        'infra.yml': 3,
        'site.yml': 4,
    }

    # collection stats assertions (current aggregation schema)
    coll_by_source = {row['collection_source']: row for row in result['collection_stats']}
    community_coll = coll_by_source['community']
    assert community_coll['jobs_total'] == 4
    assert community_coll['hosts_total'] == 5
    assert community_coll['job_duration_total_seconds'] == 2880.0
    assert community_coll['job_waiting_time_total_seconds'] == 1200.0
    assert community_coll['avg_job_duration_seconds'] == 720.0
    assert community_coll['avg_job_waiting_time_seconds'] == 300.0
    assert community_coll['avg_hosts_per_job'] == 2.0
    assert community_coll['jobs_containing_collection_source_failed_total'] == 3
    assert community_coll['jobs_failed_because_of_collection_source_failure_total'] == 3

    validated_coll = coll_by_source['validated']
    assert validated_coll['jobs_total'] == 4
    assert validated_coll['hosts_total'] == 4
    assert validated_coll['job_duration_total_seconds'] == 2880.0
    assert validated_coll['job_waiting_time_total_seconds'] == 1200.0
    assert validated_coll['avg_job_duration_seconds'] == 720.0
    assert validated_coll['avg_job_waiting_time_seconds'] == 300.0
    assert validated_coll['avg_hosts_per_job'] == 1.25
    assert validated_coll['jobs_containing_collection_source_failed_total'] == 3
    assert validated_coll['jobs_failed_because_of_collection_source_failure_total'] == 0

    # Verify per-module stats (aligned to current aggregation output)
    stats_by_module = {row['module_name']: row for row in result['module_stats']}
    # ansible.builtin.copy (validated)
    copy_stats = stats_by_module['ansible.builtin.copy']
    assert copy_stats['collection_source'] == 'validated'
    assert copy_stats['task_clean_success_total'] == 1
    assert copy_stats['task_success_with_reruns_total'] == 2
    assert copy_stats['task_failed_total'] == 0
    assert copy_stats['jobs_total'] == 3
    assert copy_stats['hosts_total'] == 3

    # community.general.yum (community)
    yum_stats = stats_by_module['community.general.yum']
    assert yum_stats['collection_source'] == 'community'
    assert yum_stats['task_clean_success_total'] == 0
    assert yum_stats['task_success_with_reruns_total'] == 0
    assert yum_stats['task_failed_total'] == 2
    assert yum_stats['jobs_total'] == 2
    assert yum_stats['hosts_total'] == 1

    # community.mongodb.insert (community)
    mongo_stats = stats_by_module['community.mongodb.insert']
    assert mongo_stats['collection_source'] == 'community'
    assert mongo_stats['task_clean_success_total'] == 1
    assert mongo_stats['task_success_with_reruns_total'] == 1
    assert mongo_stats['task_failed_total'] == 0
    assert mongo_stats['jobs_total'] == 2
    assert mongo_stats['hosts_total'] == 2

    # ansible.builtin.template (validated)
    template_stats = stats_by_module['ansible.builtin.template']
    assert template_stats['collection_source'] == 'validated'
    assert template_stats['task_clean_success_total'] == 1
    assert template_stats['task_unreachable_total'] == 1
    assert template_stats['jobs_total'] == 2
    assert template_stats['hosts_total'] == 2

    # ansible.posix.firewalld (community)
    firewalld_stats = stats_by_module['ansible.posix.firewalld']
    assert firewalld_stats['collection_source'] == 'community'
    assert firewalld_stats['task_clean_success_total'] == 1
    assert firewalld_stats['task_failed_total'] == 1
    assert firewalld_stats['jobs_total'] == 2
    assert firewalld_stats['hosts_total'] == 2

    # community.aws.ec2 (community)
    ec2_stats = stats_by_module['community.aws.ec2']
    assert ec2_stats['collection_source'] == 'community'
    assert ec2_stats['task_clean_success_total'] == 2
    assert ec2_stats['jobs_total'] == 2
    assert ec2_stats['hosts_total'] == 2
