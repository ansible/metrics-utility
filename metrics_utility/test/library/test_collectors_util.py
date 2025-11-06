import os
import pathlib
import tempfile

from unittest.mock import MagicMock

import pytest

from metrics_utility.library.collectors.util import collector, init_tmp_dir


def test_collector_decorator_basic():
    """Test that collector decorator creates a proper collector class."""

    @collector
    def sample_collector(*, param1, param2):
        return {'result': param1 + param2}

    # Create instance
    instance = sample_collector(param1=5, param2=10)

    # Should have gather method
    assert hasattr(instance, 'gather')

    # gather() should return the result
    result = instance.gather()
    assert result == {'result': 15}


def test_collector_decorator_preserves_function_name():
    """Test that collector decorator preserves function name as key."""

    @collector
    def my_test_collector():
        return {'data': 'test'}

    instance = my_test_collector()

    # Should preserve function name as key
    assert hasattr(instance, 'key')
    assert instance.key == 'my_test_collector'


def test_collector_decorator_kwargs_storage():
    """Test that collector decorator stores kwargs properly."""

    @collector
    def sample_collector(*, db, since, until):
        return {'db': db, 'since': since, 'until': until}

    mock_db = MagicMock()
    instance = sample_collector(db=mock_db, since='2024-01-01', until='2024-12-31')

    # Should store kwargs
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db
    assert instance.kwargs['since'] == '2024-01-01'
    assert instance.kwargs['until'] == '2024-12-31'

    # gather() should use stored kwargs
    result = instance.gather()
    assert result['db'] == mock_db


def test_collector_decorator_no_params():
    """Test collector with no parameters."""

    @collector
    def simple_collector():
        return {'status': 'ok'}

    instance = simple_collector()
    result = instance.gather()

    assert result == {'status': 'ok'}


def test_collector_decorator_optional_params():
    """Test collector with optional parameters."""

    @collector
    def optional_collector(*, required, optional=None):
        return {'required': required, 'optional': optional}

    # Without optional param
    instance1 = optional_collector(required='test')
    result1 = instance1.gather()
    assert result1 == {'required': 'test', 'optional': None}

    # With optional param
    instance2 = optional_collector(required='test', optional='value')
    result2 = instance2.gather()
    assert result2 == {'required': 'test', 'optional': 'value'}


def test_collector_decorator_returns_list():
    """Test collector that returns a list of filenames."""

    @collector
    def file_collector(*, output_dir):
        return [f'{output_dir}/file1.csv', f'{output_dir}/file2.csv']

    instance = file_collector(output_dir='/tmp/test')
    result = instance.gather()

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == '/tmp/test/file1.csv'


def test_collector_decorator_staticmethod():
    """Test that the original function is stored as staticmethod."""

    @collector
    def sample_collector(*, value):
        return value * 2

    instance = sample_collector(value=5)

    # Should have fn as staticmethod
    assert hasattr(instance, 'fn')
    # Can call the original function directly
    assert instance.fn(value=10) == 20


def test_init_tmp_dir():
    """Test that init_tmp_dir creates proper directory structure."""
    result = init_tmp_dir()

    # Should return a Path object
    assert isinstance(result, pathlib.Path)

    # Directory should exist
    assert result.exists()
    assert result.is_dir()

    # Should be named 'stage'
    assert result.name == 'stage'

    # Parent should have 'awx_analytics-' prefix
    assert 'awx_analytics-' in result.parent.name

    # Parent should be in temp directory
    assert str(result.parent).startswith(tempfile.gettempdir())

    # Cleanup
    import shutil

    shutil.rmtree(result.parent)


def test_init_tmp_dir_permissions():
    """Test that stage directory has correct permissions."""
    result = init_tmp_dir()

    # Check permissions (0o700)
    stat_info = os.stat(result)
    permissions = oct(stat_info.st_mode)[-3:]

    assert permissions == '700'

    # Cleanup
    import shutil

    shutil.rmtree(result.parent)


def test_init_tmp_dir_unique():
    """Test that init_tmp_dir creates unique directories."""
    dir1 = init_tmp_dir()
    dir2 = init_tmp_dir()

    # Should be different directories
    assert dir1 != dir2
    assert dir1.parent != dir2.parent

    # Both should exist
    assert dir1.exists()
    assert dir2.exists()

    # Cleanup
    import shutil

    shutil.rmtree(dir1.parent)
    shutil.rmtree(dir2.parent)


def test_collector_decorator_exception_handling():
    """Test that exceptions in collector functions are properly raised."""

    @collector
    def failing_collector():
        raise ValueError('Test error')

    instance = failing_collector()

    with pytest.raises(ValueError, match='Test error'):
        instance.gather()


def test_collector_decorator_with_db_connection():
    """Test collector with mock database connection."""

    @collector
    def db_collector(*, db):
        cursor = db.cursor()
        cursor.execute('SELECT * FROM test')
        return cursor.fetchall()

    # Create mock database
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [('row1',), ('row2',)]

    instance = db_collector(db=mock_db)
    result = instance.gather()

    assert result == [('row1',), ('row2',)]
    mock_db.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with('SELECT * FROM test')
