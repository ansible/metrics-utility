import pandas as pd

from metrics_utility.anonymized_rollups.events_modules_anonymized_rollups import Event_Modules_Anonymized_Rollups


events = [
    # ================================================================
    # Job 1 – site.yml – partial failures → job_failed=True
    # ================================================================
    # Job 1 Host 1 – task_uuid t001 (failed then recovered)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:01:00+00',
        'job_finished': '2024-01-01 00:05:00+00',
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
        'job_started': '2024-01-01 00:05:30+00',
        'job_finished': '2024-01-01 00:06:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 1 Host 2 – task_uuid t002 (yum failed final)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:03:00+00',
        'job_finished': '2024-01-01 00:07:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 1 Host 3 – task_uuid t003 (mongodb insert success)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't003',
        'event': 'runner_on_async_ok',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:04:00+00',
        'job_finished': '2024-01-01 00:08:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 1 Host 4 – task_uuid t004 (unreachable)
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 4,
        'task_uuid': 't004',
        'event': 'runner_on_unreachable',
        'task_action': 'ansible.builtin.template',
        'job_created': '2024-01-01 00:00:00+00',
        'job_started': '2024-01-01 00:02:00+00',
        'job_finished': '2024-01-01 00:10:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # ================================================================
    # Job 2 – db.yml – async failure on one host → job_failed=True
    # ================================================================
    # Job 2 Host 1 – task_uuid t005 (failed, then retried and ok)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't005',
        'event': 'runner_on_failed',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:04:00+00',
        'job_finished': '2024-01-02 12:10:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't005',
        'event': 'runner_on_ok',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:11:00+00',
        'job_finished': '2024-01-02 12:13:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 2 Host 2 – task_uuid t006 (async failed final)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't006',
        'event': 'runner_on_async_failed',
        'task_action': 'community.general.yum',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:07:00+00',
        'job_finished': '2024-01-02 12:15:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 2 Host 3 – task_uuid t007 (copy ok)
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 3,
        'task_uuid': 't007',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-02 12:00:00+00',
        'job_started': '2024-01-02 12:09:00+00',
        'job_finished': '2024-01-02 12:20:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # ================================================================
    # Job 3 – infra.yml – all success → job_failed=False
    # ================================================================
    # Job 3 Host 1 – task_uuid t008 (firewalld ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 1,
        'task_uuid': 't008',
        'event': 'runner_on_ok',
        'task_action': 'ansible.posix.firewalld',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:05:00+00',
        'job_finished': '2024-01-03 08:10:00+00',
        'job_failed': False,
        'resolved_action': None,
    },
    # Job 3 Host 2 – task_uuid t009 (ec2 provision ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 2,
        'task_uuid': 't009',
        'event': 'runner_on_ok',
        'task_action': 'community.aws.ec2',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:06:00+00',
        'job_finished': '2024-01-03 08:15:00+00',
        'job_failed': False,
        'resolved_action': None,
    },
    # Job 3 Host 3 – task_uuid t010 (template ok)
    {
        'job_id': 3,
        'playbook': 'infra.yml',
        'host_id': 3,
        'task_uuid': 't010',
        'event': 'runner_item_on_ok',
        'task_action': 'ansible.builtin.template',
        'job_created': '2024-01-03 08:00:00+00',
        'job_started': '2024-01-03 08:07:00+00',
        'job_finished': '2024-01-03 08:18:00+00',
        'job_failed': False,
        'resolved_action': None,
    },
    # ================================================================
    # Job 4 – deploy.yml – one host failed → job_failed=True
    # ================================================================
    # Job 4 Host 4 – task_uuid t011 (firewalld fail final)
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 4,
        'task_uuid': 't011',
        'event': 'runner_on_failed',
        'task_action': 'ansible.posix.firewalld',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:10:00+00',
        'job_finished': '2024-01-05 18:20:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 4 Host 5 – task_uuid t012 (copy retried and success)
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 5,
        'task_uuid': 't012',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:12:00+00',
        'job_finished': '2024-01-05 18:15:00+00',
        'job_failed': True,
        'resolved_action': None,
    },
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 5,
        'task_uuid': 't012',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:16:00+00',
        'job_finished': '2024-01-05 18:17:30+00',
        'job_failed': True,
        'resolved_action': None,
    },
    # Job 4 Host 6 – task_uuid t013 (mongodb insert ok)
    {
        'job_id': 4,
        'playbook': 'deploy.yml',
        'host_id': 6,
        'task_uuid': 't013',
        'event': 'runner_on_ok',
        'task_action': 'community.mongodb.insert',
        'job_created': '2024-01-05 18:00:00+00',
        'job_started': '2024-01-05 18:14:00+00',
        'job_finished': '2024-01-05 18:18:00+00',
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
    prepared = Event_Modules_Anonymized_Rollups.prepare_data(df.copy())
    result = Event_Modules_Anonymized_Rollups.events_modules_aggregations(prepared)

    import pprint

    pprint.pprint(result)

    expected_modules = {
        'ansible.builtin.copy',
        'community.general.yum',
        'community.mongodb.insert',
    }

    # list and count of unique modules
    assert set(result['list_of_modules_used_to_automate']) == expected_modules
    assert result['modules_used_to_automate_total'] == len(expected_modules)

    # average number of modules per playbook: both playbooks use 3 modules → avg 3
    assert result['avg_number_of_modules_used_in_a_playbooks'] == 3

    # total modules used per playbook
    assert result['modules_used_per_playbook_total'] == {
        'site.yml': 3,
        'db.yml': 3,
    }

    # collection stats assertions
    coll_by_source = {row['collection_source']: row for row in result['collection_stats']}
    community_coll = coll_by_source['community']
    assert community_coll['jobs_total'] == 2
    assert community_coll['hosts_total'] == 2
    assert community_coll['failed_total'] == 2

    validated_coll = coll_by_source['validated']
    assert validated_coll['jobs_total'] == 2
    assert validated_coll['hosts_total'] == 4
    assert validated_coll['failed_total'] == 0

    # job time stats per collection source
    time_by_source = {row['collection_source']: row for row in result['job_time_stats_per_collection_source']}
    community_time = time_by_source['community']
    assert community_time['jobs_total'] == 2
    assert community_time['job_duration_total_seconds'] == 1800.0
    assert community_time['job_waiting_time_total_seconds'] == 660.0
    assert community_time['avg_job_duration_seconds'] == 900.0
    assert community_time['avg_job_waiting_time_seconds'] == 330.0
    assert community_time['avg_hosts_per_job'] == 1.5

    validated_time = time_by_source['validated']
    assert validated_time['jobs_total'] == 2
    assert validated_time['job_duration_total_seconds'] == 1875.0
    assert validated_time['job_waiting_time_total_seconds'] == 510.0
    assert validated_time['avg_job_duration_seconds'] == 937.5
    assert validated_time['avg_job_waiting_time_seconds'] == 255.0
    assert validated_time['avg_hosts_per_job'] == 3.0

    # Verify a few per-module stats (aligned to current aggregation output)
    stats_by_module = {row['module_name']: row for row in result['module_stats']}

    # ansible.builtin.copy: six task runs (t1@h1, t1@h2, t3@h1, t4@h2, t14@h3, t15@h4), all success; 2 failed attempts total
    copy_stats = stats_by_module['ansible.builtin.copy']
    copy_total_tasks = (
        copy_stats['task_clean_success_total']
        + copy_stats['task_success_with_reruns_total']
        + copy_stats['task_failed_total']
        + copy_stats['task_unreachable_total']
        + copy_stats['task_failed_and_ignored_total']
    )
    assert copy_total_tasks == 6
    assert copy_stats['task_clean_success_total'] == 4
    assert copy_stats['task_success_with_reruns_total'] == 2
    assert copy_stats['task_failed_total'] == 0
    assert copy_stats['jobs_total'] == 2
    assert copy_stats['hosts_total'] == 4

    # community.general.yum: three task runs; one success, two failed; three failed attempts
    yum_stats = stats_by_module['community.general.yum']
    yum_total_tasks = (
        yum_stats['task_clean_success_total']
        + yum_stats['task_success_with_reruns_total']
        + yum_stats['task_failed_total']
        + yum_stats['task_unreachable_total']
        + yum_stats['task_failed_and_ignored_total']
    )
    assert yum_total_tasks == 3
    assert yum_stats['task_clean_success_total'] == 1
    assert yum_stats['task_success_with_reruns_total'] == 0
    assert yum_stats['task_failed_total'] == 2
    assert yum_stats['jobs_total'] == 2
    assert yum_stats['hosts_total'] == 2

    # community.mongodb.insert: two task runs; one success, one failed; three failed attempts
    mongo_stats = stats_by_module['community.mongodb.insert']
    mongo_total_tasks = (
        mongo_stats['task_clean_success_total']
        + mongo_stats['task_success_with_reruns_total']
        + mongo_stats['task_failed_total']
        + mongo_stats['task_unreachable_total']
        + mongo_stats['task_failed_and_ignored_total']
    )
    assert mongo_total_tasks == 2
    assert mongo_stats['task_clean_success_total'] == 1
    assert mongo_stats['task_success_with_reruns_total'] == 0
    assert mongo_stats['task_failed_total'] == 1
    assert mongo_stats['jobs_total'] == 2
    assert mongo_stats['hosts_total'] == 2
