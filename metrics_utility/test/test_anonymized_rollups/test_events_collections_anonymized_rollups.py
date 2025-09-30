from datetime import datetime, timedelta
import pandas as pd
from metrics_utility.anonymized_rollups.events_anonymized_rollups import Event_Anonymized_Rollups

# Synthetic event stream for testing
events = [
    # --- Job 100: Community collection, two hosts, success ---
    {
        "resolved_action": None,  # will fall back to task_action
        "task_action": "community.general.ping",
        "job_failed": False,
        "host_id": 0,
        "playbook": "site.yml",
        "job_id": 0,
        "job_created": datetime(2025, 9, 1, 9, 55, 0),
        "job_started": datetime(2025, 9, 1, 10, 0, 0),
        "job_finished": datetime(2025, 9, 1, 10, 2, 0),
        # task_success_event/failed_event intentionally omitted to test default False fill
    },
    {
        "resolved_action": None,
        "task_action": "community.general.ping",
        "job_failed": False,
        "host_id": 2,
        "playbook": "site.yml",
        "job_id": 0,
        "job_created": datetime(2025, 9, 1, 9, 55, 0),
        "job_started": datetime(2025, 9, 1, 10, 0, 0),
        "job_finished": datetime(2025, 9, 1, 10, 2, 0),
    },

    # --- Job 101: Community collection, single host, failure ---
    {
        "resolved_action": "community.mongodb.insert",  # explicit resolved_action used
        "task_action": "community.mongodb.insert",
        "job_failed": True,
        "host_id": "db-01",
        "playbook": "db.yml",
        "job_id": 1,
        "job_created": datetime(2025, 9, 2, 13, 0, 0),
        "job_started": datetime(2025, 9, 2, 13, 5, 0),
        "job_finished": datetime(2025, 9, 2, 13, 20, 0),
        "task_failed_event": True,
        "task_success_event": False,
    },

    # --- Job 102: Red Hat collection, two hosts, success ---
    {
        "resolved_action": None,
        "task_action": "redhat.insights.scan",
        "job_failed": False,
        "host_id": "edge-01",
        "playbook": "insights.yml",
        "job_id": 2,
        "job_created": datetime(2025, 9, 3, 8, 30, 0),
        "job_started": datetime(2025, 9, 3, 8, 45, 0),
        "job_finished": datetime(2025, 9, 3, 9, 0, 0),
        "task_success_event": True,
    },
    {
        "resolved_action": None,
        "task_action": "redhat.insights.scan",
        "job_failed": False,
        "host_id": "edge-02",
        "playbook": "insights.yml",
        "job_id": 2,
        "job_created": datetime(2025, 9, 3, 8, 30, 0),
        "job_started": datetime(2025, 9, 3, 8, 45, 0),
        "job_finished": datetime(2025, 9, 3, 9, 0, 0),
        "task_success_event": True,
    },

    # --- Job 103: Builtin collection, single host, success; resolved_action missing, task_action used ---
    {
        "resolved_action": None,
        "task_action": "ansible.builtin.shell",
        "job_failed": None,  # will be filled to False
        "host_id": "util-01",
        "playbook": "util.yml",
        "job_id": 3,
        "job_created": datetime(2025, 9, 4, 14, 0, 0),
        "job_started": datetime(2025, 9, 4, 14, 1, 0),
        "job_finished": datetime(2025, 9, 4, 14, 1, 5),
        # no task_* flags -> test default False fill
    },

    # --- Job 104: Partner collection, two hosts, failure ---
    {
        "resolved_action": "partnerA.database.backup",
        "task_action": "partnerA.database.backup",
        "job_failed": True,
        "host_id": "db-02",
        "playbook": "backup.yml",
        "job_id": 4,
        "job_created": datetime(2025, 9, 5, 1, 0, 0),
        "job_started": datetime(2025, 9, 5, 3, 0, 0),   # long waiting time
        "job_finished": datetime(2025, 9, 5, 3, 45, 0),
        "task_failed_event": True,
    },
    {
        "resolved_action": "partnerA.database.backup",
        "task_action": "partnerA.database.backup",
        "job_failed": True,
        "host_id": "db-03",
        "playbook": "backup.yml",
        "job_id": 4,
        "job_created": datetime(2025, 9, 5, 1, 0, 0),
        "job_started": datetime(2025, 9, 5, 3, 0, 0),
        "job_finished": datetime(2025, 9, 5, 3, 45, 0),
        "task_failed_event": True,
    },

    # --- Job 105: Mix to create duplicate (job_id, collection_source) pairs across multiple modules ---
    # Same job uses two modules from the same collection to ensure drop_duplicates path matters
    {
        "resolved_action": None,
        "task_action": "community.general.copy",
        "job_failed": False,
        "host_id": "app-01",
        "playbook": "deploy.yml",
        "job_id": 5,
        "job_created": datetime(2025, 9, 6, 11, 0, 0),
        "job_started": datetime(2025, 9, 6, 11, 2, 0),
        "job_finished": datetime(2025, 9, 6, 11, 10, 0),
        "task_success_event": True,
    },
    {
        "resolved_action": "community.general.template",
        "task_action": "community.general.template",
        "job_failed": False,
        "host_id": "app-02",
        "playbook": "deploy.yml",
        "job_id": 5,
        "job_created": datetime(2025, 9, 6, 11, 0, 0),
        "job_started": datetime(2025, 9, 6, 11, 2, 0),
        "job_finished": datetime(2025, 9, 6, 11, 10, 0),
        "task_success_event": True,
    },

    # --- Job 106: Bad rows that should be filtered out by prepare_data() ---
    {
        "resolved_action": None,
        "task_action": "",  # empty -> filtered out
        "job_failed": False,
        "host_id": "bad-01",
        "playbook": "oops.yml",
        "job_id": 6,
        "job_created": datetime(2025, 9, 7, 9, 0, 0),
        "job_started": datetime(2025, 9, 7, 9, 1, 0),
        "job_finished": datetime(2025, 9, 7, 9, 2, 0),
    },
    {
        "resolved_action": None,
        "task_action": "community.general.debug",
        "job_failed": False,
        "host_id": "   ",  # whitespace -> filtered out
        "playbook": "debug.yml",
        "job_id": 6,
        "job_created": datetime(2025, 9, 7, 9, 0, 0),
        "job_started": datetime(2025, 9, 7, 9, 1, 0),
        "job_finished": datetime(2025, 9, 7, 9, 2, 0),
    },
    {
        "resolved_action": None,
        "task_action": "community.general.debug",
        "job_failed": False,
        "host_id": "bad-02",
        "playbook": "",  # empty -> filtered out
        "job_id": 6,
        "job_created": datetime(2025, 9, 7, 9, 0, 0),
        "job_started": datetime(2025, 9, 7, 9, 1, 0),
        "job_finished": datetime(2025, 9, 7, 9, 2, 0),
    },
    {
        "resolved_action": None,
        "task_action": "community.general.debug",
        "job_failed": False,
        "host_id": "bad-03",
        "playbook": "debug.yml",
        "job_id": "   ",  # whitespace -> filtered out
        "job_created": datetime(2025, 9, 7, 9, 0, 0),
        "job_started": datetime(2025, 9, 7, 9, 1, 0),
        "job_finished": datetime(2025, 9, 7, 9, 2, 0),
    },
]

def test_events_collections_anonymized_rollups():
    df = pd.DataFrame(events)
    df = Event_Anonymized_Rollups.prepare_data(df)
    metrics = Event_Anonymized_Rollups.event_collections_aggregations(df)
    
    from pprint import pprint
    print('\n\n\n')
    pprint(metrics)

    # Assertions
    import math
    from pandas import Timedelta

    # Floats → use isclose
    assert math.isclose(metrics['avg_hosts_per_job_by_collection_source']['community'], 1.6667, rel_tol=1e-4)
    assert math.isclose(metrics['avg_hosts_per_job_by_collection_source']['validated'], 1.6667, rel_tol=1e-4)

    assert math.isclose(metrics['success_rate_by_collection_source']['community'], 2/3, rel_tol=1e-9)
    assert math.isclose(metrics['success_rate_by_collection_source']['validated'], 2/3, rel_tol=1e-9)

    # Timedelta → exact
    assert metrics['avg_job_duration_by_collection_source']['community'] == Timedelta(minutes=8, seconds=20)
    assert metrics['avg_job_duration_by_collection_source']['validated'] == Timedelta(minutes=20, seconds=1.666666666)

    assert metrics['avg_job_waiting_time_by_collection_source']['community'] == Timedelta(minutes=4)
    assert metrics['avg_job_waiting_time_by_collection_source']['validated'] == Timedelta(minutes=45, seconds=20)

    # Ints → exact
    assert metrics['failed_jobs_by_collection_source']['community'] == 1
    assert metrics['failed_jobs_by_collection_source']['validated'] == 1

    assert metrics['success_jobs_by_collection_source']['community'] == 2
    assert metrics['success_jobs_by_collection_source']['validated'] == 2

    assert metrics['total_jobs_by_collection_source']['community'] == 3
    assert metrics['total_jobs_by_collection_source']['validated'] == 3