import sys
import types

import pandas as pd


def _import_rollups_module_with_stub():
    # Ensure the target module is re-imported fresh after injecting stub
    sys.modules.pop('metrics_utility.rollups.events_collections_anonymized_rollups', None)

    # Provide a stub so `import collections_types` does not fail during module import
    if 'collections_types' not in sys.modules:
        sys.modules['collections_types'] = types.ModuleType('collections_types')

    import importlib

    return importlib.import_module('metrics_utility.rollups.events_collections_anonymized_rollups')


def test_extract_collection_name():
    rollups_mod = _import_rollups_module_with_stub()
    assert rollups_mod.extract_collection_name('ansible.builtin.copy') == 'ansible.builtin'
    assert rollups_mod.extract_collection_name('ns1.coll1.modx.task') == 'ns1.coll1'
    assert rollups_mod.extract_collection_name('invalidname') is None
    assert rollups_mod.extract_collection_name(None) is None


def test_events_collections_base_aggregation():
    rollups_mod = _import_rollups_module_with_stub()

    # Replace the module-level mapping with a dict accepted by Series.map
    rollups_mod.collections_types = {
        'ns1.coll1': 'validated',
        'ns1.coll2': 'community',
    }

    # Build a DataFrame with required columns
    # Jobs per collection source:
    # - validated: jobs 1 (success, 2 hosts), 2 (failed, 1 host)
    # - community: jobs 3 (success, 1 host), 4 (failed, 2 hosts)
    t0 = pd.Timestamp('2025-01-01T10:00:00Z')

    rows = [
        # validated, job 1, two hosts (2 unique hosts)
        {
            'module_name': 'ns1.coll1.modx.task',
            'job_failed': False,
            'job_id': 1,
            'host_id': 101,
            'job_created': t0 - pd.Timedelta(minutes=10),
            'job_started': t0,
            'job_finished': t0 + pd.Timedelta(minutes=10),  # 10m
        },
        {
            'module_name': 'ns1.coll1.modx.other',
            'job_failed': None,  # will be coerced to False
            'job_id': 1,
            'host_id': 102,
            'job_created': t0 - pd.Timedelta(minutes=10),
            'job_started': t0,
            'job_finished': t0 + pd.Timedelta(minutes=10),
        },
        # validated, job 2, failed, duplicate host rows but 1 unique host
        {
            'module_name': 'ns1.coll1.modx.task',
            'job_failed': True,
            'job_id': 2,
            'host_id': 201,
            'job_created': t0,
            'job_started': t0 + pd.Timedelta(minutes=0),
            'job_finished': t0 + pd.Timedelta(minutes=5),  # 5m
        },
        {
            'module_name': 'ns1.coll1.modx.task',
            'job_failed': True,
            'job_id': 2,
            'host_id': 201,  # duplicate host
            'job_created': t0,
            'job_started': t0 + pd.Timedelta(minutes=0),
            'job_finished': t0 + pd.Timedelta(minutes=5),
        },
        # community, job 3, success, 1 host
        {
            'module_name': 'ns1.coll2.modx.task',
            'job_failed': False,
            'job_id': 3,
            'host_id': 301,
            'job_created': t0 + pd.Timedelta(minutes=10),
            'job_started': t0 + pd.Timedelta(minutes=15),
            'job_finished': t0 + pd.Timedelta(minutes=45),  # 30m
        },
        # community, job 4, failed, 2 hosts
        {
            'module_name': 'ns1.coll2.modx.task',
            'job_failed': True,
            'job_id': 4,
            'host_id': 401,
            'job_created': t0 + pd.Timedelta(minutes=55),
            'job_started': t0 + pd.Timedelta(minutes=60),
            'job_finished': t0 + pd.Timedelta(minutes=80),  # 20m
        },
        {
            'module_name': 'ns1.coll2.modx.task2',
            'job_failed': True,
            'job_id': 4,
            'host_id': 402,
            'job_created': t0 + pd.Timedelta(minutes=55),
            'job_started': t0 + pd.Timedelta(minutes=60),
            'job_finished': t0 + pd.Timedelta(minutes=80),
        },
    ]

    df = pd.DataFrame(rows)

    result = rollups_mod.Events_Collections_Anonymized_Rollups.base(df)

    # Validate result structure
    assert isinstance(result, dict)
    for key in [
        'total_jobs_by_collection_source',
        'avg_job_duration_by_collection_source',
        'failed_jobs_by_collection_source',
        'success_rate_by_collection_source',
        'avg_hosts_per_job_by_collection_source',
    ]:
        assert key in result

    total_jobs = result['total_jobs_by_collection_source']
    failed_jobs = result['failed_jobs_by_collection_source']
    success_rate = result['success_rate_by_collection_source']
    avg_duration = result['avg_job_duration_by_collection_source']
    avg_hosts = result['avg_hosts_per_job_by_collection_source']

    # Totals
    assert int(total_jobs['validated']) == 2
    assert int(total_jobs['community']) == 2

    # Failed counts
    assert int(failed_jobs['validated']) == 1
    assert int(failed_jobs['community']) == 1

    # Success rate
    assert float(success_rate['validated']) == 0.5
    assert float(success_rate['community']) == 0.5

    # Average durations
    assert avg_duration['validated'] == pd.Timedelta(minutes=7.5)  # mean of 10m and 5m
    assert avg_duration['community'] == pd.Timedelta(minutes=25)   # mean of 30m and 20m

    # Average unique hosts per job
    assert float(avg_hosts['validated']) == 1.5  # (2 + 1) / 2
    assert float(avg_hosts['community']) == 1.5  # (1 + 2) / 2
