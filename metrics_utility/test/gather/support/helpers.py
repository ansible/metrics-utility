"""Helper functions for functional tests: CSV generators and slicing functions."""

import os
import tarfile

from django.utils.timezone import timedelta

from metrics_utility.library.csv_file_splitter import CsvFileSplitter


TIMESTAMP_CSV_LINE_LENGTH = 40


def trivial_slicing(key, last_gather, since, until, **kwargs):
    """Return a single slice covering the entire [since, until) window.

    Args:
        key: Unused collector key.
        last_gather: Unused last-gather datetime.
        since: Start of the collection window.
        until: End of the collection window.
        **kwargs: Ignored extra keyword arguments.

    Returns:
        List with one ``(since, until)`` tuple.
    """
    return [(since, until)]


def one_day_slicing(key, last_gather, since, until, **kwargs):
    """Yield one-day time slices between *since* and *until*.

    Args:
        key: Unused collector key.
        last_gather: Unused last-gather datetime.
        since: Start of the collection window (truncated to midnight).
        until: End of the collection window (truncated to midnight).
        **kwargs: Ignored extra keyword arguments.

    Yields:
        ``(start, end)`` tuples spanning one calendar day each.
    """
    since = since.replace(hour=0, minute=0, second=0, microsecond=0)
    until = until.replace(hour=0, minute=0, second=0, microsecond=0)
    start, end = since, None
    while start < until:
        end = min(start + timedelta(days=1), until)
        yield (start, end)
        start = end


def csv_generator(full_path, file_name, files_cnt, max_data_size, header, line):
    """Generate one or more split CSV files using :class:`~metrics_utility.library.CsvFileSplitter`.

    Args:
        full_path: Directory to write files into.
        file_name: Base file name (without extension).
        files_cnt: Number of split files to produce.
        max_data_size: Maximum file size in bytes before splitting.
        header: CSV header line string (including newline).
        line: CSV data line string to repeat (including newline).

    Returns:
        List of generated file paths.
    """
    file_path = get_file_path(full_path, file_name)
    with CsvFileSplitter(filespec=file_path, max_file_size=max_data_size) as file:
        # create required number of files (decrease by headers - it's CSV)
        file.write(header)
        for _ in range(files_cnt * int(max_data_size / len(line)) - files_cnt):
            file.write(line)

        return file.file_list()


def simple_csv(full_path, file_name, files_cnt, max_data_size):
    """CSVs with line length 10 bytes"""
    header = 'Col1,Col2\n'  # 10 chars
    line = '1234,6789\n'  # 10 chars
    return csv_generator(full_path, file_name, files_cnt, max_data_size, header, line)


def timestamp_csv(full_path, file_name, files_cnt, max_data_size, since, until):
    """CSVs with line length 40 bytes"""
    header = 'since______________,until______________\n'  # 40 chars
    line = [
        since.strftime('%Y,%m,%d,%H,00,00'),  # 19 chars
        until.strftime('%Y,%m,%d,%H,00,00'),
    ]  # 19 chars
    line = f'{",".join(line)}\n'  # +2 = 40 chars

    return csv_generator(full_path, file_name, files_cnt, max_data_size, header, line)


def get_file_path(path, table):
    """Return the expected CSV file path for a given table name.

    Args:
        path: Directory path.
        table: Table name used as the base filename.

    Returns:
        Full path string.
    """
    return os.path.join(path, table + '_table.csv')


def decode_csv_line(line):
    """Decode a raw CSV bytes line into a list of stripped field strings.

    Args:
        line: Bytes object representing one CSV row.

    Returns:
        List of field strings.
    """
    return line.decode('utf-8').replace('\r', '').replace('\n', '').split(',')


def read_tarball(path):
    with tarfile.open(path, 'r:gz') as archive:
        return {m.name: archive.extractfile(m).read() for m in archive.getmembers()}


def validate_csv_in_tarballs(tarball_glob, csv_filename, expected_lines, skip_columns_names):
    import csv
    import glob

    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    expected_header = expected_rows[0]
    expected_data = expected_rows[1:]

    actual_rows = []
    for file_path in sorted(glob.glob(tarball_glob)):
        files = read_tarball(file_path)
        match = next((name for name in files if name.endswith(csv_filename)), None)
        if match is None:
            continue

        text = files[match].decode('utf-8').splitlines()
        reader = csv.reader(text)
        rows = list(reader)
        header = rows[0]
        assert header == expected_header, f'\nHeader mismatch for {csv_filename}:\nExpected: {expected_header}\nActual:   {header}'
        actual_rows.extend(rows[1:])

    assert len(actual_rows) > 0, f'{csv_filename} not found in any tarballs under {tarball_glob}'

    assert len(actual_rows) == len(expected_data), f'\nRow count mismatch in {csv_filename}: expected {len(expected_data)}, got {len(actual_rows)}'

    skip_columns = set(skip_columns_names)
    actual_sorted = sorted(actual_rows, key=lambda r: r[0])
    expected_sorted = sorted(expected_data, key=lambda r: r[0])

    for i, (expected_row, actual_row) in enumerate(zip(expected_sorted, actual_sorted), start=1):
        for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
            col_name = expected_header[idx]
            if col_name in skip_columns:
                continue
            assert exp_cell == act_cell, (
                f'\nData mismatch in {csv_filename} on row {i + 1}, column {col_name!r} '
                f'(index {idx}):\n'
                f'Expected: {exp_cell!r}\n'
                f'Actual:   {act_cell!r}'
            )
