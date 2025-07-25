from unittest.mock import Mock, patch

from django.db.utils import ProgrammingError

from metrics_utility.automation_controller_billing.collectors import (
    main_indirectmanagednodeaudit_table,
)


class TestMainIndirectManagedNodeAuditTable:
    """Test cases for the main_indirectmanagednodeaudit_table function"""

    @patch('metrics_utility.automation_controller_billing.collectors._copy_table')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    def test_main_indirectmanagednodeaudit_table_success(self, mock_get_optional_collectors, mock_copy_table):
        """Test successful execution when table exists"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        mock_copy_table.return_value = ['test_file.csv']

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute
        result = main_indirectmanagednodeaudit_table(since=since, full_path='/test/path', until=until)

        # Assert
        assert result == ['test_file.csv']
        mock_copy_table.assert_called_once()
        call_args = mock_copy_table.call_args
        assert call_args[1]['table'] == 'main_indirectmanagednodeaudit'
        assert 'COPY' in call_args[1]['query']
        assert call_args[1]['path'] == '/test/path'

    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    def test_main_indirectmanagednodeaudit_table_not_in_optional_collectors(self, mock_get_optional_collectors):
        """Test returns None when collector is not in optional collectors"""
        # Setup
        mock_get_optional_collectors.return_value = {'other_collector'}

        # Execute
        result = main_indirectmanagednodeaudit_table(since=Mock(), full_path='/test/path', until=Mock())

        # Assert
        assert result is None

    @patch('metrics_utility.automation_controller_billing.collectors.logger')
    @patch('metrics_utility.automation_controller_billing.collectors._copy_table')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    def test_main_indirectmanagednodeaudit_table_programming_error(self, mock_get_optional_collectors, mock_copy_table, mock_logger):
        """Test graceful handling when table doesn't exist (ProgrammingError)"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        error_message = 'relation "main_indirectmanagednodeaudit" does not exist'
        mock_copy_table.side_effect = ProgrammingError(error_message)

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute
        result = main_indirectmanagednodeaudit_table(since=since, full_path='/test/path', until=until)

        # Assert
        assert result is None
        mock_logger.warning.assert_called_once()
        warning_call = mock_logger.warning.call_args
        assert 'main_indirectmanagednodeaudit table missing in the database schema: %s.' in warning_call[0][0]
        assert 'Falling back to behavior without indirect managed node audit data.' in warning_call[0][0]
        assert warning_call[0][1] is mock_copy_table.side_effect

    @patch('metrics_utility.automation_controller_billing.collectors._copy_table')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    def test_main_indirectmanagednodeaudit_table_query_format(self, mock_get_optional_collectors, mock_copy_table):
        """Test that the SQL query contains expected elements"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        mock_copy_table.return_value = ['test_file.csv']

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute
        main_indirectmanagednodeaudit_table(since=since, full_path='/test/path', until=until)

        # Assert
        mock_copy_table.assert_called_once()
        call_args = mock_copy_table.call_args
        query = call_args[1]['query']

        # Check that query contains expected table references
        assert 'main_indirectmanagednodeaudit' in query
        assert 'main_job' in query
        assert 'main_unifiedjob' in query
        assert 'main_inventory' in query
        assert 'main_organization' in query

        # Check date filtering
        assert '2024-01-01T00:00:00' in query
        assert '2024-01-02T00:00:00' in query

    @patch('metrics_utility.automation_controller_billing.collectors.logger')
    @patch('metrics_utility.automation_controller_billing.collectors._copy_table')
    @patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors')
    def test_main_indirectmanagednodeaudit_table_logs_specific_error(self, mock_get_optional_collectors, mock_copy_table, mock_logger):
        """Test that the specific error message is logged correctly"""
        # Setup
        mock_get_optional_collectors.return_value = {'main_indirectmanagednodeaudit'}
        specific_error = ProgrammingError('table "main_indirectmanagednodeaudit" does not exist')
        mock_copy_table.side_effect = specific_error

        since = Mock()
        since.isoformat.return_value = '2024-01-01T00:00:00'
        until = Mock()
        until.isoformat.return_value = '2024-01-02T00:00:00'

        # Execute
        result = main_indirectmanagednodeaudit_table(since=since, full_path='/test/path', until=until)

        # Assert
        assert result is None
        mock_logger.warning.assert_called_once_with(
            'main_indirectmanagednodeaudit table missing in the database schema: %s.'
            ' Falling back to behavior without indirect managed node audit data.',
            specific_error,
        )
