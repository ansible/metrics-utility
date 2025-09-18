import pandas as pd
import pytest

from metrics_utility.rollups.events_anonymized_rollups import Event_Rollups


def test_event_rollups_base_aggregation():
    # Build a DataFrame mimicking main_jobevent_service output columns we use
    data = [
        # playbook pb1 uses modules copy (success+fail) and file (success)
        {'resolved_action': 'copy', 'task_action': None, 'playbook': 'pb1.yml', 'event': 'runner_on_ok'},
        {'resolved_action': 'copy', 'task_action': None, 'playbook': 'pb1.yml', 'event': 'runner_on_failed'},
        {'resolved_action': None, 'task_action': 'file', 'playbook': 'pb1.yml', 'event': 'runner_on_ok'},
        # playbook pb2 uses modules template (success + skipped) and copy (failure)
        {'resolved_action': 'template', 'task_action': None, 'playbook': 'pb2.yml', 'event': 'runner_on_ok'},
        {'resolved_action': 'copy', 'task_action': None, 'playbook': 'pb2.yml', 'event': 'runner_on_unreachable'},
        {'resolved_action': 'template', 'task_action': None, 'playbook': 'pb2.yml', 'event': 'runner_on_skipped'},
    ]
    df = pd.DataFrame(data)

    result = Event_Rollups.base(df)

    # avg distinct modules per playbook: pb1 -> {copy, file} = 2, pb2 -> {template, copy} = 2; mean = 2.0
    assert result['avg_modules_per_playbook'] == pytest.approx(2.0)

    # total distinct modules across all events
    assert result['modules_used'] == 3

    # per-module stats and failure rates
    # copy: successes=1 (ok), failures=2 (failed + unreachable) -> total=3, failure_rate=2/3
    # file: successes=1, failures=0 -> total=1, failure_rate=0
    # template: successes=1, failures=0 (skipped does not count) -> total=1, failure_rate=0
    stats_by_module = {row['module_name']: row for row in result['modules_failure_rate']}

    assert 'copy' in stats_by_module and 'file' in stats_by_module and 'template' in stats_by_module

    copy_stats = stats_by_module['copy']
    assert copy_stats['successes'] == 1
    assert copy_stats['failures'] == 2
    assert copy_stats['total_runs'] == 3
    assert copy_stats['failure_rate'] == pytest.approx(2 / 3)

    file_stats = stats_by_module['file']
    assert file_stats['successes'] == 1
    assert file_stats['failures'] == 0
    assert file_stats['total_runs'] == 1
    assert file_stats['failure_rate'] == pytest.approx(0.0)

    template_stats = stats_by_module['template']
    assert template_stats['successes'] == 1
    assert template_stats['failures'] == 0
    assert template_stats['total_runs'] == 1
    assert template_stats['failure_rate'] == pytest.approx(0.0)
