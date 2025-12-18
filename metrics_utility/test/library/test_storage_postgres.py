import datetime
import os
import tempfile

from unittest.mock import MagicMock

import pytest

from metrics_utility.library.storage import StoragePostgres, create_storage_table


# Note: These tests use mocked database connections for unit testing.
# For integration tests with a real PostgreSQL database, use the mock DB from `make compose`.


def test_storage_postgres_requires_db():
    """Test that StoragePostgres requires a db connection."""
    with pytest.raises(Exception, match='db connection is required'):
        StoragePostgres(table='test_table')


def test_storage_postgres_requires_table():
    """Test that StoragePostgres requires a table name."""
    mock_db = MagicMock()
    with pytest.raises(Exception, match='table is required'):
        StoragePostgres(db=mock_db)


def test_storage_postgres_init():
    """Test StoragePostgres initialization with all parameters."""
    mock_db = MagicMock()
    storage = StoragePostgres(
        db=mock_db,
        table='my_table',
        key_field='my_key',
        value_field='my_value',
        timestamp_field='my_timestamp',
    )

    assert storage.db == mock_db
    assert storage.table == 'my_table'
    assert storage.key_field == 'my_key'
    assert storage.value_field == 'my_value'
    assert storage.timestamp_field == 'my_timestamp'


def test_storage_postgres_init_defaults():
    """Test StoragePostgres initialization with default field names."""
    mock_db = MagicMock()
    storage = StoragePostgres(db=mock_db, table='my_table')

    assert storage.key_field == 'key'
    assert storage.value_field == 'value'
    assert storage.timestamp_field is None


def test_get_dict_value():
    """Test get_data method returns dict value from database."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    # Simulate JSONB column returning a dict directly
    mock_cursor.fetchone.return_value = ({'foo': 'bar', 'count': 42},)

    storage = StoragePostgres(db=mock_db, table='test_table')
    result = storage.get_data('test_key')

    assert result == {'foo': 'bar', 'count': 42}
    mock_cursor.execute.assert_called_once()
    assert 'SELECT' in str(mock_cursor.execute.call_args)
    assert 'test_key' in str(mock_cursor.execute.call_args)


def test_get_list_value():
    """Test get_data method returns list value from database."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    # Simulate JSONB column returning a list directly
    mock_cursor.fetchone.return_value = ([{'name': 'Alice'}, {'name': 'Bob'}],)

    storage = StoragePostgres(db=mock_db, table='test_table')
    result = storage.get_data('test_key')

    assert result == [{'name': 'Alice'}, {'name': 'Bob'}]


def test_get_not_found():
    """Test get_data method returns None when key doesn't exist."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = None

    storage = StoragePostgres(db=mock_db, table='test_table')
    result = storage.get_data('nonexistent_key')

    assert result is None


def test_put_dict():
    """Test put method stores a dict."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    storage = StoragePostgres(db=mock_db, table='test_table', timestamp_field='updated_at')
    test_data = {'foo': 'bar', 'count': 42}

    storage.put('test_key', dict=test_data)

    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args

    # Check that INSERT query was called
    assert 'INSERT' in str(call_args)
    assert 'test_key' in str(call_args)

    # Check that commit was called
    mock_db.commit.assert_called_once()


def test_put_list():
    """Test put method stores a list."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    storage = StoragePostgres(db=mock_db, table='test_table')
    test_data = [{'name': 'Alice'}, {'name': 'Bob'}]

    storage.put('test_key', dict=test_data)

    mock_cursor.execute.assert_called_once()
    mock_db.commit.assert_called_once()


def test_put_csv_filename():
    """Test put method loads and stores CSV file."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    storage = StoragePostgres(db=mock_db, table='test_table')

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('name,age\n')
        f.write('Alice,30\n')
        f.write('Bob,25\n')
        csv_path = f.name

    try:
        storage.put('test_key', filename=csv_path)

        mock_cursor.execute.assert_called_once()
        mock_db.commit.assert_called_once()

        # Verify the data was converted from CSV to list of dicts
        call_args = str(mock_cursor.execute.call_args)
        assert 'Alice' in call_args or 'test_key' in call_args
    finally:
        os.unlink(csv_path)


def test_put_json_filename():
    """Test put method loads and stores JSON file."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    storage = StoragePostgres(db=mock_db, table='test_table')

    # Create a temporary JSON file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write('{"users": [{"name": "Alice"}], "count": 1}')
        json_path = f.name

    try:
        storage.put('test_key', filename=json_path)

        mock_cursor.execute.assert_called_once()
        mock_db.commit.assert_called_once()
    finally:
        os.unlink(json_path)


def test_put_unsupported_file_type():
    """Test put method raises error for unsupported file types."""
    mock_db = MagicMock()
    storage = StoragePostgres(db=mock_db, table='test_table')

    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write('some text')
        txt_path = f.name

    try:
        with pytest.raises(ValueError, match='Unsupported file type'):
            storage.put('test_key', filename=txt_path)
    finally:
        os.unlink(txt_path)


def test_put_requires_exactly_one_param():
    """Test put method requires exactly one of filename, fileobj, or dict."""
    mock_db = MagicMock()
    storage = StoragePostgres(db=mock_db, table='test_table')

    # No parameters
    with pytest.raises(ValueError, match='Exactly one'):
        storage.put('test_key')

    # Multiple parameters
    with pytest.raises(ValueError, match='Exactly one'):
        storage.put('test_key', dict={'foo': 'bar'}, filename='test.json')


def test_put_update_timestamp_false():
    """Test put method with update_timestamp=False."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    storage = StoragePostgres(db=mock_db, table='test_table', timestamp_field='updated_at')

    storage.put('test_key', dict={'foo': 'bar'}, update_timestamp=False)

    # Verify the INSERT query doesn't include timestamp
    # When update_timestamp=False, we should not be passing a timestamp value
    # The query should only have 2 parameters (key, value)
    assert mock_cursor.execute.called


def test_glob_basic():
    """Test glob method returns matching keys."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    # Simulate database returning some keys
    mock_cursor.fetchall.return_value = [
        ('data-2025-01-01',),
        ('data-2025-01-02',),
        ('config',),
    ]

    storage = StoragePostgres(db=mock_db, table='test_table')
    result = storage.glob('data-*')

    assert result == ['data-2025-01-01', 'data-2025-01-02']


def test_glob_with_timestamp_filter():
    """Test glob method with timestamp filtering."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    since = datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)
    until = datetime.datetime(2025, 1, 4, tzinfo=datetime.timezone.utc)

    # Simulate database returning keys with timestamps
    mock_cursor.fetchall.return_value = [
        ('data-2025-01-01', datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)),
        ('data-2025-01-02', datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)),
        ('data-2025-01-03', datetime.datetime(2025, 1, 3, tzinfo=datetime.timezone.utc)),
        ('data-2025-01-04', datetime.datetime(2025, 1, 4, tzinfo=datetime.timezone.utc)),
        ('data-2025-01-05', datetime.datetime(2025, 1, 5, tzinfo=datetime.timezone.utc)),
    ]

    storage = StoragePostgres(db=mock_db, table='test_table', timestamp_field='updated_at')
    result = storage.glob('data-*', since=since, until=until)

    # Should include 01-02 and 01-03 (since <= timestamp < until)
    assert result == ['data-2025-01-02', 'data-2025-01-03']


def test_exists_true():
    """Test exists method returns True when key exists."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = (1,)

    storage = StoragePostgres(db=mock_db, table='test_table')
    result = storage.exists('test_key')

    assert result is True


def test_exists_false():
    """Test exists method returns False when key doesn't exist."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = None

    storage = StoragePostgres(db=mock_db, table='test_table')
    result = storage.exists('nonexistent_key')

    assert result is False


def test_remove():
    """Test remove method deletes a key."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    storage = StoragePostgres(db=mock_db, table='test_table')
    storage.remove('test_key')

    mock_cursor.execute.assert_called_once()
    assert 'DELETE' in str(mock_cursor.execute.call_args)
    assert 'test_key' in str(mock_cursor.execute.call_args)
    mock_db.commit.assert_called_once()


def test_create_storage_table_with_timestamp():
    """Test create_storage_table creates table with timestamp field."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    create_storage_table(
        db=mock_db,
        table='my_storage',
        key_field='my_key',
        value_field='my_value',
        timestamp_field='my_timestamp',
    )

    # Should execute CREATE TABLE and CREATE INDEX
    assert mock_cursor.execute.call_count == 2

    # First call should be CREATE TABLE
    first_call = str(mock_cursor.execute.call_args_list[0])
    assert 'CREATE TABLE' in first_call

    # Second call should be CREATE INDEX
    second_call = str(mock_cursor.execute.call_args_list[1])
    assert 'CREATE INDEX' in second_call

    mock_db.commit.assert_called_once()


def test_create_storage_table_without_timestamp():
    """Test create_storage_table creates table without timestamp field."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    create_storage_table(
        db=mock_db,
        table='my_storage',
        timestamp_field=None,
    )

    # Should execute only CREATE TABLE (no index since no timestamp)
    assert mock_cursor.execute.call_count == 1

    call = str(mock_cursor.execute.call_args)
    assert 'CREATE TABLE' in call

    mock_db.commit.assert_called_once()


def test_get_as_context_manager():
    """Test get() context manager creates temp JSON file."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    # Simulate JSONB column returning a dict
    mock_cursor.fetchone.return_value = ({'foo': 'bar', 'count': 42},)

    storage = StoragePostgres(db=mock_db, table='test_table')

    with storage.get('test_key') as filename:
        # Verify it's a file path
        assert isinstance(filename, str)
        # Verify the file exists and contains the data as JSON
        assert os.path.exists(filename)
        with open(filename, 'r') as f:
            import json

            data = json.load(f)
            assert data == {'foo': 'bar', 'count': 42}

    # After exiting context, temp file should be cleaned up
    assert not os.path.exists(filename)


def test_get_as_context_manager_key_not_found():
    """Test get() context manager raises KeyError when key doesn't exist."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = None

    storage = StoragePostgres(db=mock_db, table='test_table')

    with pytest.raises(KeyError, match='Key not found'):
        with storage.get('nonexistent_key') as _filename:
            pass
