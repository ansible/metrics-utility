import os

from django.utils.timezone import timedelta

from metrics_utility.library import CsvFileSplitter


TIMESTAMP_CSV_LINE_LENGTH = 40


def trivial_slicing(key, last_gather, since, until, **kwargs):
    return [(since, until)]


def one_day_slicing(key, last_gather, since, until, **kwargs):
    since = since.replace(hour=0, minute=0, second=0, microsecond=0)
    until = until.replace(hour=0, minute=0, second=0, microsecond=0)
    start, end = since, None
    while start < until:
        end = min(start + timedelta(days=1), until)
        yield (start, end)
        start = end


def csv_generator(full_path, file_name, files_cnt, max_data_size, header, line):
    file_path = get_file_path(full_path, file_name)
    file = CsvFileSplitter(filespec=file_path, max_file_size=max_data_size)

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
    return os.path.join(path, table + '_table.csv')


def decode_csv_line(line):
    return line.decode('utf-8').replace('\r', '').replace('\n', '').split(',')


def assert_common_files(files):
    assert './config.json' in files.keys()
    assert './manifest.json' in files.keys()
    assert './data_collection_status.csv' in files.keys()
