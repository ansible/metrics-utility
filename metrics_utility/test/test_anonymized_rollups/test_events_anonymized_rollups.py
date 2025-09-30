import pandas as pd

from metrics_utility.anonymized_rollups.events_anonymized_rollups import Event_Anonymized_Rollups


def test_event_rollups_base_aggregation():
    # Build a DataFrame matching the new implementation's expected columns
    data = [
        # pb1 uses modules copy (one success, one failure) and file (success)
        {'resolved_action': 'copy', 'task_action': None, 'playbook': 'pb1.yml', 'job_failed': False, 'job_id': 1},
        {'resolved_action': 'copy', 'task_action': None, 'playbook': 'pb1.yml', 'job_failed': True, 'job_id': 2},
        {'resolved_action': None, 'task_action': 'file', 'playbook': 'pb1.yml', 'job_failed': False, 'job_id': 1},
        # pb2 uses modules template (two successes) and copy (failure)
        {'resolved_action': 'template', 'task_action': None, 'playbook': 'pb2.yml', 'job_failed': False, 'job_id': 5},
        {'resolved_action': 'copy', 'task_action': None, 'playbook': 'pb2.yml', 'job_failed': True, 'job_id': 6},
        {'resolved_action': 'template', 'task_action': None, 'playbook': 'pb2.yml', 'job_failed': False, 'job_id': 7},
    ]
    df = pd.DataFrame(data)

    result = Event_Anonymized_Rollups.base(df)

    # Ensure new keys are present
    assert isinstance(result, dict)
    assert 'aggregations_by_playbook_module' in result
    assert 'list_of_modules_used_to_automate' in result
    assert 'total_modules_used_to_automate' in result

    # Validate modules list and total
    expected_modules = {'copy', 'file', 'template'}
    modules_list = set(result['list_of_modules_used_to_automate'])
    assert expected_modules.issubset(modules_list)
    assert result['total_modules_used_to_automate'] == len(expected_modules)

    # Validate aggregation structure
    aggs = result['aggregations_by_playbook_module']
    assert isinstance(aggs, list)
    # We expect one group per (playbook, module): (pb1,copy), (pb1,file), (pb2,template), (pb2,copy)
    assert len(aggs) == 4

    # Each aggregation record should include failure flag and job count
    sample = aggs[0]
    assert 'job_failed' in sample
    assert 'job_total' in sample

    # At least one group should have failed and all groups should have at least one job
    assert any(bool(row.get('job_failed')) for row in aggs)
    assert all(int(row.get('job_total')) >= 1 for row in aggs)
