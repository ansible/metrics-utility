from datetime import datetime

import pandas as pd

from metrics_utility.anonymized_rollups.events_anonymized_rollups import Event_Anonymized_Rollups


events = [
    # ansible.builtin.copy
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't001',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_on_failed',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't002',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't003',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 2,
        'task_uuid': 't004',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 3,
        'task_uuid': 't014',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 4,
        'task_uuid': 't015',
        'event': 'runner_on_ok',
        'task_action': 'ansible.builtin.copy',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    # community.general.yum
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't005',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 1,
        'task_uuid': 't005',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't006',
        'event': 'runner_on_failed',
        'task_action': 'community.general.yum',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't007',
        'event': 'runner_on_ok',
        'task_action': 'community.general.yum',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    # community.mongodb.insert
    {
        'job_id': 1,
        'playbook': 'site.yml',
        'host_id': 2,
        'task_uuid': 't008',
        'event': 'runner_on_async_ok',
        'task_action': 'community.mongodb.insert',
        'job_created': datetime(2024, 1, 1, 0, 0, 0),
        'job_started': datetime(2024, 1, 1, 0, 1, 0),
        'job_finished': datetime(2024, 1, 1, 0, 10, 0),
        'job_failed': False,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't009',
        'event': 'runner_on_failed',
        'task_action': 'community.mongodb.insert',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': True,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't009',
        'event': 'runner_on_failed',
        'task_action': 'community.mongodb.insert',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': True,
        'resolved_action': None,
    },
    {
        'job_id': 2,
        'playbook': 'db.yml',
        'host_id': 1,
        'task_uuid': 't009',
        'event': 'runner_on_failed',
        'task_action': 'community.mongodb.insert',
        'job_created': datetime(2024, 1, 2, 12, 0, 0),
        'job_started': datetime(2024, 1, 2, 12, 5, 0),
        'job_finished': datetime(2024, 1, 2, 12, 30, 0),
        'job_failed': True,
        'resolved_action': None,
    },
]


def test_events_modules_aggregations_basic():
    df = pd.DataFrame(events)
    # ensure string-typed columns for .str-based filtering in prepare_data
    for col in ['host_id', 'job_id', 'playbook']:
        df[col] = df[col].astype(str)
    prepared = Event_Anonymized_Rollups.prepare_data(df.copy())
    result = Event_Anonymized_Rollups.events_modules_aggregations(prepared)

    import pprint

    pprint.pprint(result)

    expected_modules = {
        'ansible.builtin.copy',
        'community.general.yum',
        'community.mongodb.insert',
    }

    # list and count of unique modules
    assert set(result['list_of_modules_used_to_automate']) == expected_modules
    assert result['total_modules_used_to_automate'] == len(expected_modules)

    # average number of modules per playbook: both playbooks use 3 modules → avg 3
    assert result['avg_number_of_modules_used_in_a_playbooks'] == 3

    # total modules used per playbook
    assert result['total_modules_used_per_playbook'] == {
        'site.yml': 3,
        'db.yml': 3,
    }

    # Verify a few per-module stats
    stats_by_module = {row['module_name']: row for row in result['module_stats']}

    # ansible.builtin.copy: six task runs (t1@h1, t1@h2, t3@h1, t4@h2, t14@h3, t15@h4), all success; 2 failed attempts total
    copy_stats = stats_by_module['ansible.builtin.copy']
    assert copy_stats['runs_total'] == 6
    assert copy_stats['runs_success'] == 6
    assert copy_stats['runs_failed'] == 0
    assert copy_stats['runs_other'] == 0
    assert copy_stats['total_failed_attempts'] == 2
    assert copy_stats['jobs_total'] == 2
    assert copy_stats['hosts_total'] == 4

    # community.general.yum: three task runs; one success, two failed; three failed attempts
    yum_stats = stats_by_module['community.general.yum']
    assert yum_stats['runs_total'] == 3
    assert yum_stats['runs_success'] == 1
    assert yum_stats['runs_failed'] == 2
    assert yum_stats['runs_other'] == 0
    assert yum_stats['total_failed_attempts'] == 3

    # community.mongodb.insert: two task runs; one success, one failed; three failed attempts
    mongo_stats = stats_by_module['community.mongodb.insert']
    assert mongo_stats['runs_total'] == 2
    assert mongo_stats['runs_success'] == 1
    assert mongo_stats['runs_failed'] == 1
    assert mongo_stats['runs_other'] == 0
    assert mongo_stats['total_failed_attempts'] == 3
