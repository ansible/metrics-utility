from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.exceptions import CollectorDisabled
from metrics_utility.gather.collectors import (
    cli_controller_version_service,
    cli_credentials_service,
    cli_dashboard_jobs,
    cli_execution_environments,
    cli_feature_flags_service,
    cli_job_host_summary,
    cli_job_host_summary_service,
    cli_main_host,
    cli_main_host_daily,
    cli_main_hostmetric,
    cli_main_jobevent,
    cli_main_jobevent_service,
    cli_table_metadata,
    cli_task_executions_service,
    cli_unified_jobs,
)
from metrics_utility.test.util import utcdt


since = utcdt('2024-01-01')
until = utcdt('2024-02-01')


def _mock_output():
    output = MagicMock()
    output.as_files.return_value = ['test.csv']
    output.dict.return_value = {'data': True}
    return output


def _enable(collectors):
    return patch('metrics_utility.gather.collectors.get_optional_collectors', return_value=set(collectors))


# --- job_host_summary (on by default, disable via env) ---


@patch('metrics_utility.gather.collectors.job_host_summary')
@patch('metrics_utility.gather.collectors.connection')
def test_cli_job_host_summary_enabled(mock_conn, mock_lib):
    mock_lib.return_value = MagicMock()
    output = _mock_output()

    result = cli_job_host_summary(since=since, until=until, output=output)

    assert result == ['test.csv']
    mock_lib.assert_called_once()


@patch('metrics_utility.gather.collectors.bool_from_env', return_value=True)
def test_cli_job_host_summary_disabled(mock_bool):
    with pytest.raises(CollectorDisabled):
        cli_job_host_summary(since=since, until=until, output=_mock_output())


# --- optional collectors with enabled path ---


@pytest.mark.parametrize(
    'cli_fn,lib_name,key',
    [
        (cli_main_host, 'main_host', 'main_host'),
        (cli_main_host_daily, 'main_host_daily', 'main_host_daily'),
        (cli_main_hostmetric, 'main_hostmetric', 'main_hostmetric'),
        (cli_main_jobevent, 'main_jobevent', 'main_jobevent'),
        (cli_controller_version_service, 'controller_version_service', 'controller_version_service'),
        (cli_credentials_service, 'credentials_service', 'credentials_service'),
        (cli_execution_environments, 'execution_environments', 'execution_environments'),
        (cli_job_host_summary_service, 'job_host_summary_service', 'job_host_summary_service'),
        (cli_main_jobevent_service, 'main_jobevent_service', 'main_jobevent_service'),
        (cli_table_metadata, 'table_metadata', 'table_metadata'),
        (cli_unified_jobs, 'unified_jobs', 'unified_jobs'),
        (cli_feature_flags_service, 'feature_flags_service', 'feature_flags_service'),
        (cli_task_executions_service, 'task_executions_service', 'task_executions_service'),
    ],
)
def test_cli_optional_collector_enabled(cli_fn, lib_name, key):
    output = _mock_output()

    with _enable({key}):
        with patch(f'metrics_utility.gather.collectors.{lib_name}') as mock_lib:
            with patch('metrics_utility.gather.collectors.connection'):
                mock_lib.return_value = MagicMock()
                result = cli_fn(since=since, until=until, output=output)

    assert result is not None
    mock_lib.assert_called_once()


# --- dashboard_jobs (returns dict, not files) ---


@patch('metrics_utility.gather.collectors.connection')
def test_cli_dashboard_jobs_enabled(mock_conn):
    output = _mock_output()
    mock_collector = MagicMock()
    mock_collector.gather.return_value = {'jobs': []}

    with _enable({'dashboard_jobs'}):
        with patch('metrics_utility.gather.collectors.dashboard_jobs', return_value=mock_collector):
            result = cli_dashboard_jobs(since=since, until=until, output=output)

    assert result is not None
