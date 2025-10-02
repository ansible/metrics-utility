from datetime import datetime

import pandas as pd

from pytest import approx

from metrics_utility.anonymized_rollups.events_anonymized_rollups import Event_Anonymized_Rollups


# Synthetic event stream for testing
events = [
    # --- Job 100: Community collection, two hosts, success ---
    {
        'resolved_action': None,  # will fall back to task_action
        'task_action': 'community.general.ping',
        'job_failed': False,
        'host_id': 0,
        'playbook': 'site.yml',
        'job_id': 0,
        'job_created': datetime(2025, 9, 1, 9, 55, 0),
        'job_started': datetime(2025, 9, 1, 10, 0, 0),
        'job_finished': datetime(2025, 9, 1, 10, 2, 0),
        'event': 'runner_on_ok',
    },
    {
        'resolved_action': None,
        'task_action': 'community.general.ping',
        'job_failed': False,
        'host_id': 2,
        'playbook': 'site.yml',
        'job_id': 0,
        'job_created': datetime(2025, 9, 1, 9, 55, 0),
        'job_started': datetime(2025, 9, 1, 10, 0, 0),
        'job_finished': datetime(2025, 9, 1, 10, 2, 0),
        'event': 'runner_on_ok',
    },
    # --- Job 101: Community collection, single host, failure ---
    {
        'resolved_action': 'community.mongodb.insert',  # explicit resolved_action used
        'task_action': 'community.mongodb.insert',
        'job_failed': True,
        'host_id': 'db-01',
        'playbook': 'db.yml',
        'job_id': 1,
        'job_created': datetime(2025, 9, 2, 13, 0, 0),
        'job_started': datetime(2025, 9, 2, 13, 5, 0),
        'job_finished': datetime(2025, 9, 2, 13, 20, 0),
        'task_failed_event': True,
        'task_success_event': False,
        'event': 'runner_on_ok',
    },
    # --- Job 102: Red Hat collection, two hosts, success ---
    {
        'resolved_action': None,
        'task_action': 'redhat.insights.scan',
        'job_failed': False,
        'host_id': 'edge-01',
        'playbook': 'insights.yml',
        'job_id': 2,
        'job_created': datetime(2025, 9, 3, 8, 30, 0),
        'job_started': datetime(2025, 9, 3, 8, 45, 0),
        'job_finished': datetime(2025, 9, 3, 9, 0, 0),
        'task_success_event': True,
        'event': 'runner_on_ok',
    },
    {
        'resolved_action': None,
        'task_action': 'redhat.insights.scan',
        'job_failed': False,
        'host_id': 'edge-02',
        'playbook': 'insights.yml',
        'job_id': 2,
        'job_created': datetime(2025, 9, 3, 8, 30, 0),
        'job_started': datetime(2025, 9, 3, 8, 45, 0),
        'job_finished': datetime(2025, 9, 3, 9, 0, 0),
        'task_success_event': True,
        'event': 'runner_on_ok',
    },
    # --- Job 103: Builtin collection, single host, success; resolved_action missing, task_action used ---
    {
        'resolved_action': None,
        'task_action': 'ansible.builtin.shell',
        'job_failed': None,  # will be filled to False
        'host_id': 'util-01',
        'playbook': 'util.yml',
        'job_id': 3,
        'job_created': datetime(2025, 9, 4, 14, 0, 0),
        'job_started': datetime(2025, 9, 4, 14, 1, 0),
        'job_finished': datetime(2025, 9, 4, 14, 1, 5),
        'event': 'runner_on_ok',
        # no task_* flags -> test default False fill
    },
    # --- Job 104: Partner collection, two hosts, failure ---
    {
        'resolved_action': 'partnerA.database.backup',
        'task_action': 'partnerA.database.backup',
        'job_failed': True,
        'host_id': 'db-02',
        'playbook': 'backup.yml',
        'job_id': 4,
        'job_created': datetime(2025, 9, 5, 1, 0, 0),
        'job_started': datetime(2025, 9, 5, 3, 0, 0),  # long waiting time
        'job_finished': datetime(2025, 9, 5, 3, 45, 0),
        'task_failed_event': True,
        'event': 'runner_on_ok',
    },
    {
        'resolved_action': 'partnerA.database.backup',
        'task_action': 'partnerA.database.backup',
        'job_failed': True,
        'host_id': 'db-03',
        'playbook': 'backup.yml',
        'job_id': 4,
        'job_created': datetime(2025, 9, 5, 1, 0, 0),
        'job_started': datetime(2025, 9, 5, 3, 0, 0),
        'job_finished': datetime(2025, 9, 5, 3, 45, 0),
        'task_failed_event': True,
        'event': 'runner_on_ok',
    },
]


"""
        *Breakdown of total jobs executed by collection source (e.g., Red Hat, Partner A, Community).
          *Average job duration for collection sources
          *Average number of hosts automated per job for each collection source.
          *Number of jobs per collection source that have failed.
          *Success/failure rate of jobs per collection source.
"""


def test_events_collections_anonymized_rollups():
    df = pd.DataFrame(events)
    df = Event_Anonymized_Rollups.prepare_data(df)

    data = Event_Anonymized_Rollups.event_collections_aggregations(df)

    from pprint import pprint

    print('\n\n\n')
    pprint(data)

    # times are in seconds
    # First dictionary
    assert data[0]['avg_job_duration_seconds'] == approx(510.0)
    assert data[0]['avg_job_waiting_time_seconds'] == approx(300.0)
    assert data[0]['collection_source'] == 'community'
    assert data[0]['success_rate'] == approx(0.5)
    assert data[0]['avg_hosts_per_job'] == approx(1.5)
    assert data[0]['job_duration_total_seconds'] == approx(1020.0)
    assert data[0]['job_waiting_time_total_seconds'] == approx(600.0)
    assert data[0]['jobs_total'] == 2
    assert data[0]['jobs_failed_total'] == 1

    # Second dictionary
    assert data[1]['avg_job_duration_seconds'] == approx(1201.6666666666667)
    assert data[1]['avg_job_waiting_time_seconds'] == approx(2720.0)
    assert data[1]['collection_source'] == 'validated'
    assert data[1]['success_rate'] == approx(0.6666666666666667)
    assert data[1]['avg_hosts_per_job'] == approx(1.6666666666666667)
    assert data[1]['job_duration_total_seconds'] == approx(3605.0)
    assert data[1]['job_waiting_time_total_seconds'] == approx(8160.0)
    assert data[1]['jobs_total'] == 3
    assert data[1]['jobs_failed_total'] == 1
