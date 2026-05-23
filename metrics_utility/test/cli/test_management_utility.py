from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.exceptions import MetricsError
from metrics_utility.management_utility import ManagementUtility


def test_execute_help_default(capsys):
    util = ManagementUtility(['manage.py'])
    util.execute()
    out = capsys.readouterr().out
    assert 'gather_automation_controller_billing_data' in out


def test_execute_help_explicit(capsys):
    util = ManagementUtility(['manage.py', 'help'])
    util.execute()
    out = capsys.readouterr().out
    assert 'gather_automation_controller_billing_data' in out


def test_execute_help_commands_only(capsys):
    util = ManagementUtility(['manage.py', 'help', '--commands'])
    util.execute()
    out = capsys.readouterr().out
    assert 'Commands:' in out


def test_execute_help_subcommand(capsys):
    util = ManagementUtility(['manage.py', 'help', 'gather_automation_controller_billing_data'])
    util.execute()
    out = capsys.readouterr().out
    assert 'gather' in out.lower() or 'billing' in out.lower()


def test_execute_version(capsys):
    util = ManagementUtility(['manage.py', 'version'])
    util.execute()
    out = capsys.readouterr().out
    assert out.strip()  # some version string


def test_execute_double_dash_version(capsys):
    util = ManagementUtility(['manage.py', '--version'])
    util.execute()
    out = capsys.readouterr().out
    assert out.strip()


def test_execute_dash_h(capsys):
    util = ManagementUtility(['manage.py', '--help'])
    util.execute()
    out = capsys.readouterr().out
    assert 'gather_automation_controller_billing_data' in out


def test_execute_dash_h_short(capsys):
    util = ManagementUtility(['manage.py', '-h'])
    util.execute()
    out = capsys.readouterr().out
    assert 'gather_automation_controller_billing_data' in out


def test_main_help_text():
    util = ManagementUtility(['manage.py'])
    text = util.main_help_text()
    assert 'gather_automation_controller_billing_data' in text
    assert 'Usage:' in text


def test_main_help_text_commands_only():
    util = ManagementUtility(['manage.py'])
    text = util.main_help_text(commands_only=True)
    assert 'Commands:' in text
    assert 'Usage:' not in text


def test_fetch_command():
    util = ManagementUtility(['manage.py'])
    cmd = util.fetch_command('gather_automation_controller_billing_data')
    assert cmd is not None


def test_fetch_command_invalid(capsys):
    util = ManagementUtility(['manage.py'])
    with pytest.raises(Exception, match='nonexistent_command_xyz'):
        util.fetch_command('nonexistent_command_xyz')


def test_get_commands():
    commands = ManagementUtility.get_commands()
    assert 'gather_automation_controller_billing_data' in commands
    assert commands['gather_automation_controller_billing_data'] == 'metrics_utility'


def test_execute_dispatches_subcommand():
    util = ManagementUtility(['manage.py', 'gather_automation_controller_billing_data', '--dry-run'])
    with patch.object(util, 'run_subcommand') as mock_run:
        util.execute()
    mock_run.assert_called_once_with('gather_automation_controller_billing_data', util.argv)


def test_run_subcommand_metrics_exception():
    util = ManagementUtility(['manage.py', 'gather_automation_controller_billing_data'])

    with patch.object(util, 'fetch_command') as mock_fetch:
        mock_cmd = mock_fetch.return_value
        mock_cmd.run_from_argv.side_effect = MetricsError('test error')

        with pytest.raises(SystemExit) as exc:
            util.run_subcommand('gather_automation_controller_billing_data', util.argv)
        assert exc.value.code == 1


def test_run_subcommand_generic_exception():
    util = ManagementUtility(['manage.py', 'gather_automation_controller_billing_data'])

    with patch.object(util, 'fetch_command') as mock_fetch:
        mock_cmd = mock_fetch.return_value
        mock_cmd.run_from_argv.side_effect = RuntimeError('unexpected')

        with pytest.raises(SystemExit) as exc:
            util.run_subcommand('gather_automation_controller_billing_data', util.argv)
        assert exc.value.code == 1


# --- metrics_utility.__init__ ---


def test_prepare_finds_awx_via_awx_path():
    from metrics_utility import prepare

    mock_spec = MagicMock()
    with patch('metrics_utility.importlib.util.find_spec', side_effect=[None, mock_spec]):
        with patch('metrics_utility.sys.path'):
            prepare()


def test_prepare_warns_about_db_env_vars(capsys):
    from metrics_utility import prepare
    from metrics_utility.test.util import temporary_env

    with temporary_env({'METRICS_UTILITY_DB_HOST': 'localhost'}):
        prepare()

    err = capsys.readouterr().err
    assert 'METRICS_UTILITY_DB_HOST' in err
    assert 'ignored' in err


def test_manage_runs():
    mock_util = MagicMock()
    with patch('metrics_utility.ManagementUtility', return_value=mock_util):
        with patch('metrics_utility.prepare'):
            from metrics_utility import manage

            manage()
    mock_util.execute.assert_called_once()
