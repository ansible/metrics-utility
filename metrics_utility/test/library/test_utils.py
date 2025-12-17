import os
import re

from metrics_utility.library.utils import tempdir


def test_tempdir_creates_directory():
    """Test that tempdir creates a temporary directory."""
    created_dir = None

    with tempdir(prefix='test') as temp_dir:
        created_dir = temp_dir
        # Directory should exist while in context
        assert os.path.exists(temp_dir)
        assert os.path.isdir(temp_dir)

        # Directory name should include prefix and timestamp
        basename = os.path.basename(temp_dir)
        assert 'test' in basename
        # Verify timestamp is present (format: YYYY-MM-DD-HHMMSS+ZZZZ)
        timestamp_pattern = r'\d{4}-\d{2}-\d{2}-\d{6}\+\d{4}'
        assert re.search(timestamp_pattern, basename), f'Expected timestamp pattern in {basename}'

    # Directory should be cleaned up after context exits (cleanup=True by default)
    assert not os.path.exists(created_dir)


def test_tempdir_changes_directory():
    """Test that tempdir changes to the temporary directory."""
    original_dir = os.getcwd()

    with tempdir(prefix='test') as temp_dir:
        # Should have changed to the temp directory (resolve symlinks for comparison)
        assert os.path.realpath(os.getcwd()) == os.path.realpath(temp_dir)
        assert os.path.realpath(os.getcwd()) != os.path.realpath(original_dir)

    # Should restore original directory after context exits
    assert os.path.realpath(os.getcwd()) == os.path.realpath(original_dir)


def test_tempdir_cleanup_true():
    """Test that tempdir cleans up when cleanup=True."""
    created_dir = None

    with tempdir(prefix='test', cleanup=True) as temp_dir:
        created_dir = temp_dir
        # Create a file in the tempdir
        test_file = os.path.join(temp_dir, 'test.txt')
        with open(test_file, 'w') as f:
            f.write('test content')
        assert os.path.exists(test_file)

    # Directory and contents should be cleaned up
    assert not os.path.exists(created_dir)


def test_tempdir_cleanup_false():
    """Test that tempdir preserves directory when cleanup=False."""
    created_dir = None

    try:
        with tempdir(prefix='test', cleanup=False) as temp_dir:
            created_dir = temp_dir
            # Create a file in the tempdir
            test_file = os.path.join(temp_dir, 'test.txt')
            with open(test_file, 'w') as f:
                f.write('test content')

        # Directory and contents should still exist
        assert os.path.exists(created_dir)
        assert os.path.exists(os.path.join(created_dir, 'test.txt'))
    finally:
        # Clean up manually for this test
        if created_dir and os.path.exists(created_dir):
            import shutil

            shutil.rmtree(created_dir)


def test_tempdir_restores_cwd_on_exception():
    """Test that tempdir restores original directory even when exception occurs."""
    original_dir = os.getcwd()

    try:
        with tempdir(prefix='test') as temp_dir:
            # Verify we're in the temp directory (resolve symlinks for comparison)
            assert os.path.realpath(os.getcwd()) == os.path.realpath(temp_dir)
            # Raise an exception
            raise ValueError('Test exception')
    except ValueError:
        pass

    # Should have restored original directory despite exception
    assert os.path.realpath(os.getcwd()) == os.path.realpath(original_dir)


def test_tempdir_with_prefix_none():
    """Test that tempdir works with no prefix and includes timestamp."""
    with tempdir(prefix=None) as temp_dir:
        assert os.path.exists(temp_dir)
        assert os.path.isdir(temp_dir)
        assert os.path.realpath(os.getcwd()) == os.path.realpath(temp_dir)

        # Verify timestamp is present in directory name (format: YYYY-MM-DD-HHMMSS+ZZZZ)
        basename = os.path.basename(temp_dir)
        timestamp_pattern = r'\d{4}-\d{2}-\d{2}-\d{6}\+\d{4}'
        assert re.search(timestamp_pattern, basename), f'Expected timestamp pattern in {basename}'


def test_tempdir_nested_file_creation():
    """Test creating nested files within the tempdir."""
    with tempdir(prefix='test') as temp_dir:
        # Create nested directory structure
        nested_dir = os.path.join(temp_dir, 'subdir')
        os.makedirs(nested_dir)

        # Create file in nested directory
        nested_file = os.path.join(nested_dir, 'data.csv')
        with open(nested_file, 'w') as f:
            f.write('col1,col2\nval1,val2\n')

        assert os.path.exists(nested_file)

        # Read it back
        with open(nested_file, 'r') as f:
            content = f.read()
            assert content == 'col1,col2\nval1,val2\n'
