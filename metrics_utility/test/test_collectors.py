from unittest.mock import MagicMock, Mock, patch

import pytest

from django.db.utils import OperationalError, ProgrammingError

from metrics_utility.automation_controller_billing.collectors import (
    cli_main_indirectmanagednodeaudit,
)


class TestMainIndirectManagedNodeAuditTable:
    """Test cases for the cli_main_indirectmanagednodeaudit function"""

    @patch('metrics_utility.automation_controller_billing.collectors.main_indirectmanagednodeaudit')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    @patch('metrics_utility.automation_controller_billing.collectors.connection')
    def test_main_indirectmanagednodeaudit_table_success(self, mock_connection, mock_get_optional_collectors, mock_main_indirectmanagednodeaudit):
        """Test successful execution when table exists"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}

        mock_collector = MagicMock()
        mock_main_indirectmanagednodeaudit.return_value = mock_collector

        mock_output = MagicMock()
        mock_output.as_files.return_value = ['test_file.csv']

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute
        result = cli_main_indirectmanagednodeaudit(since=since, until=until, output=mock_output)

        # Assert
        assert result == ['test_file.csv']
        mock_main_indirectmanagednodeaudit.assert_called_once_with(db=mock_connection, since=since, until=until)
        mock_output.as_files.assert_called_once_with(mock_collector)

    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    def test_main_indirectmanagednodeaudit_table_not_in_optional_collectors(self, mock_get_optional_collectors):
        """Test returns None when collector is not in optional collectors"""
        # Setup
        mock_get_optional_collectors.return_value = {'other_collector'}

        # Execute (output not used since function returns early)
        result = cli_main_indirectmanagednodeaudit(since=None, until=None, output=None)

        # Assert
        assert result is None

    @patch('metrics_utility.automation_controller_billing.collectors.logger')
    @patch('metrics_utility.automation_controller_billing.collectors.main_indirectmanagednodeaudit')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    @patch('metrics_utility.automation_controller_billing.collectors.connection')
    def test_main_indirectmanagednodeaudit_table_programming_error(
        self,
        mock_connection,
        mock_get_optional_collectors,
        mock_main_indirectmanagednodeaudit,
        mock_logger,
    ):
        """Test graceful handling when table doesn't exist (ProgrammingError)"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        error_message = 'relation "main_indirectmanagednodeaudit" does not exist'
        mock_main_indirectmanagednodeaudit.side_effect = ProgrammingError(error_message)

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute (output not used when exception is raised)
        result = cli_main_indirectmanagednodeaudit(since=since, until=until, output=None)

        # Assert
        assert result is None
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args
        assert 'main_indirectmanagednodeaudit table missing in the database schema: %s.' in warning_call[0][0]
        assert 'Falling back to behavior without indirect managed node audit data.' in warning_call[0][0]
        assert warning_call[0][1] is mock_main_indirectmanagednodeaudit.side_effect

    @patch('metrics_utility.automation_controller_billing.collectors.logger')
    @patch('metrics_utility.automation_controller_billing.collectors.main_indirectmanagednodeaudit')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    @patch('metrics_utility.automation_controller_billing.collectors.connection')
    def test_main_indirectmanagednodeaudit_table_logs_specific_error(
        self,
        mock_connection,
        mock_get_optional_collectors,
        mock_main_indirectmanagednodeaudit,
        mock_logger,
    ):
        """Test that the specific error message is logged correctly"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        specific_error = ProgrammingError('table "main_indirectmanagednodeaudit" does not exist')
        mock_main_indirectmanagednodeaudit.side_effect = specific_error

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute (output not used when exception is raised)
        result = cli_main_indirectmanagednodeaudit(since=since, until=until, output=None)

        # Assert
        assert result is None
        mock_logger.warning.assert_called_once_with(
            'main_indirectmanagednodeaudit table missing in the database schema: %s.'
            ' Falling back to behavior without indirect managed node audit data.',
            specific_error,
        )

    @patch('metrics_utility.automation_controller_billing.collectors.main_indirectmanagednodeaudit')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    @patch('metrics_utility.automation_controller_billing.collectors.connection')
    def test_main_indirectmanagednodeaudit_operational_error_propagates(
        self,
        mock_connection,
        mock_get_optional_collectors,
        mock_main_indirectmanagednodeaudit,
    ):
        """OperationalError (e.g. connection refused) is not caught and propagates up.

        This is distinct from ProgrammingError (missing table), which is caught and logged.
        """
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        mock_main_indirectmanagednodeaudit.side_effect = OperationalError('connection refused')

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        with pytest.raises(OperationalError):
            cli_main_indirectmanagednodeaudit(since=since, until=until, output=None)
