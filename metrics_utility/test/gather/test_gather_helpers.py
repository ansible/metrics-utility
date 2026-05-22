from unittest.mock import MagicMock, patch

from django.db import DatabaseError

from metrics_utility.gather.utils import get_last_entries_from_db
from metrics_utility.test.util import utcdt


class TestGetLastEntriesFromDb:
    """Test cases for get_last_entries_from_db function"""

    @patch('metrics_utility.gather.utils.connection')
    def test_successful_entries_retrieval(self, mock_connection):
        """Test successful last entries retrieval"""
        # Setup
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        test_json = '"{\\"config\\": \\"2024-01-01T00:00:00Z\\", \\"hosts\\": \\"2024-01-03T00:00:00Z\\", \\"jobs\\": \\"2024-01-02T00:00:00Z\\"}"'
        mock_cursor.fetchone.return_value = (test_json,)
        # Execute
        result = get_last_entries_from_db()

        # Assert - _datetime_hook parses datetime strings to datetime objects
        expected_result = {
            'config': utcdt('2024-01-01'),
            'hosts': utcdt('2024-01-03'),
            'jobs': utcdt('2024-01-02'),
        }
        assert result == expected_result
        mock_cursor.execute.assert_called_once()
        # Verify correct SQL query
        sql_call = mock_cursor.execute.call_args[0][0]
        assert 'AUTOMATION_ANALYTICS_LAST_ENTRIES' in sql_call

    @patch('metrics_utility.gather.utils.connection')
    def test_no_entries_or_empty_value(self, mock_connection):
        """Test when no entries found or value is empty"""
        # Setup
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # Could be no row or (None,)

        # Execute
        result = get_last_entries_from_db()

        # Assert
        assert result == {}

    @patch('metrics_utility.gather.utils.logger')
    @patch('metrics_utility.gather.utils.connection')
    def test_database_error_handling(self, mock_connection, mock_logger):
        """Test error handling when database query fails"""
        # Setup
        mock_connection.cursor.side_effect = DatabaseError('Query failed')

        # Execute
        result = get_last_entries_from_db()

        # Assert
        assert result == {}
        mock_logger.error.assert_called_once()
        assert 'Error getting AUTOMATION_ANALYTICS_LAST_ENTRIES from database' in str(mock_logger.error.call_args)


class TestIntegration:
    """Integration tests for helper functions working together"""

    @patch('metrics_utility.gather.utils.connection')
    def test_functions_work_with_real_data(self, mock_connection):
        """Test that all helper functions work with realistic data"""
        # Setup realistic database responses
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        test_json = '"{\\"config\\": \\"2024-01-01T00:00:00Z\\", \\"jobs\\": \\"2024-01-02T00:00:00Z\\"}"'  # Last entries result
        mock_cursor.fetchone.return_value = (test_json,)

        # Execute
        entries = get_last_entries_from_db()

        # Assert all return expected realistic data
        # _datetime_hook parses datetime strings to datetime objects
        expected_entries = {
            'config': utcdt('2024-01-01'),
            'jobs': utcdt('2024-01-02'),
        }
        assert entries == expected_entries
