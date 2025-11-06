import os
import tempfile

from metrics_utility.library.csv_file_splitter import CsvFileSplitter


def test_basic_write():
    """Test basic CSV writing to a single file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=1000000)

        # Write header and data
        splitter.write('col1,col2,col3\n')
        splitter.write('a,b,c\n')
        splitter.write('d,e,f\n')

        files = splitter.file_list()

        # Should create one file without _split suffix
        assert len(files) == 1
        assert files[0] == filespec
        assert os.path.exists(files[0])

        # Verify content
        with open(files[0], 'r') as f:
            content = f.read()
            assert content == 'col1,col2,col3\na,b,c\nd,e,f\n'


def test_file_splitting():
    """Test that files are split when exceeding max_file_size."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        # Small max size to force splitting
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=30)

        # Write header
        splitter.write('col1,col2\n')
        # Write rows that will exceed the limit
        splitter.write('aaaa,bbbb\n')
        splitter.write('cccc,dddd\n')
        splitter.write('eeee,ffff\n')

        files = splitter.file_list()

        # Should create multiple files
        assert len(files) > 1

        # All files should exist
        for fname in files:
            assert os.path.exists(fname)
            assert '_split' in fname

        # Each file should have the header
        for fname in files:
            with open(fname, 'r') as f:
                first_line = f.readline()
                assert first_line == 'col1,col2\n'


def test_header_preservation():
    """Test that header is written to each split file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=40)

        header = 'name,value,timestamp\n'
        splitter.write(header)

        # Write enough data to trigger splits
        for i in range(10):
            splitter.write(f'item{i},val{i},time{i}\n')

        files = splitter.file_list()

        # Verify all files have the header
        for fname in files:
            with open(fname, 'r') as f:
                assert f.readline() == header


def test_empty_file_removal():
    """Test that empty files (only header) are removed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=1000)

        # Write only header, no data
        splitter.write('col1,col2,col3\n')

        files = splitter.file_list()

        # Should remove empty file by default
        assert len(files) == 0


def test_keep_empty_file():
    """Test that empty files can be kept with keep_empty=True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=1000)

        # Write only header, no data
        splitter.write('col1,col2,col3\n')

        files = splitter.file_list(keep_empty=True)

        # Should keep empty file
        assert len(files) == 1
        assert os.path.exists(files[0])


def test_single_file_suffix_removal():
    """Test that _split0 suffix is removed when only one file exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=1000000)

        splitter.write('header\n')
        splitter.write('data\n')

        files = splitter.file_list()

        assert len(files) == 1
        assert '_split' not in files[0]
        assert files[0] == filespec


def test_multiple_files_keep_suffix():
    """Test that _split suffix is kept when multiple files exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=30)

        splitter.write('header\n')
        for i in range(10):
            splitter.write(f'data{i}\n')

        files = splitter.file_list()

        assert len(files) > 1
        for fname in files:
            assert '_split' in fname


def test_cycle_file():
    """Test manual file cycling."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=1000000)

        splitter.write('col1,col2\n')
        splitter.write('a,b\n')

        # Manually cycle to next file
        splitter.cycle_file()

        splitter.write('c,d\n')

        files = splitter.file_list()

        # Should have created 2 files
        assert len(files) == 2

        # Both should have the header
        for fname in files:
            with open(fname, 'r') as f:
                assert f.readline() == 'col1,col2\n'


def test_counter_tracking():
    """Test that counter properly tracks bytes written."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=1000)

        assert splitter.counter == 0

        header = 'col1,col2\n'
        splitter.write(header)

        # Counter should reflect header size
        assert splitter.counter == len(header)

        data = 'a,b\n'
        splitter.write(data)

        # Counter should include both header and data
        assert splitter.counter == len(header) + len(data)


def test_files_list_tracking():
    """Test that files list is properly maintained."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filespec = os.path.join(tmpdir, 'test.csv')
        splitter = CsvFileSplitter(filespec=filespec, max_file_size=30)

        splitter.write('header\n')

        # Initially 1 file
        assert len(splitter.files) == 1

        # Write data to force splits
        for i in range(5):
            splitter.write(f'data{i}\n')

        # Should have multiple files tracked
        assert len(splitter.files) > 1
