import json
import logging
import shutil

from unittest.mock import patch

import pytest

import metrics_utility.test.gather.support.collector_module1
import metrics_utility.test.gather.support.collector_module2
import metrics_utility.test.gather.support.collector_module3

from metrics_utility.test.gather.support.analytics_collector import AnalyticsCollector
from metrics_utility.test.gather.support.helpers import decode_csv_line, read_tarball


COMMON_FILES = ['./config.json', './data_collection_status.csv', './manifest.json']


@pytest.fixture
def collector(mocker):
    collector = AnalyticsCollector(
        collector_module=metrics_utility.test.gather.support.collector_module1,
        collection_type=AnalyticsCollector.DRY_RUN,
    )

    yield collector

    if collector.tmp_dir:
        shutil.rmtree(collector.tmp_dir, ignore_errors=True)


def test_missing_config(mocker, collector):
    with patch('metrics_utility.gather.collector.logger') as mock_logger:
        tgz_files = collector.gather(subset=['json_collection_1', 'json_collection_2'])

        assert tgz_files is None
        mock_logger.log.assert_called_with(logging.ERROR, "'config' collector data is missing")


def test_json_collections(collector):
    tgz_files = collector.gather(subset=['config', 'json_collection_1', 'json_collection_2'])

    assert len(tgz_files) == 1

    files = read_tarball(tgz_files[0])

    assert sorted(files) == sorted(COMMON_FILES + ['./json_collection_1.json', './json_collection_2.json'])

    assert json.loads(files['./config.json']) == {'version': '1.0'}
    assert json.loads(files['./json_collection_1.json']) == {'json1': 'True'}
    assert json.loads(files['./json_collection_2.json']) == {'json2': 'True'}


def test_small_csvs(collector):
    tgz_files = collector.gather(subset=['config', 'csv_collection_1', 'csv_collection_2', 'csv_collection_3'])

    assert len(tgz_files) == 1

    files = read_tarball(tgz_files[0])

    assert sorted(files) == sorted(
        COMMON_FILES
        + [
            './csv_collection_1.csv',
            './csv_collection_2.csv',
            './csv_collection_3.csv',
        ]
    )

    # length defined by @registered function
    assert len(files['./csv_collection_1.csv']) == 100
    assert len(files['./csv_collection_2.csv']) == 200
    assert len(files['./csv_collection_3.csv']) == 300


def test_jsons_with_csvs_with_slicing(collector):
    tgz_files = collector.gather(
        subset=[
            'config',
            'json_collection_1',
            'json_collection_2',
            'csv_slicing_1',
            'csv_slicing_2',
        ]
    )

    assert len(tgz_files) == 3

    expected = [
        COMMON_FILES + ['./json_collection_1.json', './json_collection_2.json', './csv_slicing_1.csv'],
        COMMON_FILES + ['./csv_slicing_2.csv'],
        COMMON_FILES + ['./csv_slicing_2.csv'],
    ]
    for i in range(len(tgz_files)):
        files = read_tarball(tgz_files[i])
        assert sorted(files) == sorted(expected[i])


def test_one_csv_collection_splitted_by_size(collector):
    tgz_files = collector.gather(subset=['config', 'big_table'])

    assert len(tgz_files) == 10

    for i in range(len(tgz_files)):
        files = read_tarball(tgz_files[i])

        assert sorted(files) == sorted(COMMON_FILES + ['./big_table.csv'])
        assert len(files['./big_table.csv']) == 1000


def test_multiple_collections_multiple_tarballs(mocker, collector):
    mocker.patch('metrics_utility.test.gather.support.package.Package.MAX_DATA_SIZE', 1000)

    tgz_files = collector.gather(subset=['config', 'big_table_2', 'csv_collection_1', 'csv_collection_2'])

    assert len(tgz_files) == 3

    expected = [
        COMMON_FILES + ['./big_table_2.csv', './csv_collection_1.csv'],
        COMMON_FILES + ['./big_table_2.csv', './csv_collection_2.csv'],
        COMMON_FILES + ['./big_table_2.csv'],
    ]
    for i in range(len(tgz_files)):
        files = read_tarball(tgz_files[i])
        assert sorted(files) == sorted(expected[i])


def test_multiple_collections_and_distributions(collector):
    """
    - JSONs start at index 0
    - CSVs with no slicing start at index 0
    - CSVs with slicing start after index next to previous slice
    """
    collector.collector_module = metrics_utility.test.gather.support.collector_module3
    tgz_files = collector.gather()

    assert len(tgz_files) == 13

    # Each file appears in a range of tarball indices:
    #   file name                      indices
    #   ./simple_json1.json            0
    #   ./csv_no_slicing_1-2x.csv      0-1
    #   ./csv_no_slicing_2-1x.csv      0
    #   ./csv_no_slicing_3-10x.csv     0-9
    #   ./csv_no_slicing_4-12x.csv     0-11
    #   ./csv_with_slicing_1-5x.csv    0-4
    #   ./csv_with_slicing_2-3x.csv    5-7
    #   ./csv_with_slicing_3-2x.csv    8-9
    #   ./csv_with_slicing_4-3x.csv    10-12
    file_ranges = [
        ('./simple_json1.json', 0, 0),
        ('./csv_no_slicing_1-2x.csv', 0, 1),
        ('./csv_no_slicing_2-1x.csv', 0, 0),
        ('./csv_no_slicing_3-10x.csv', 0, 9),
        ('./csv_no_slicing_4-12x.csv', 0, 11),
        ('./csv_with_slicing_1-5x.csv', 0, 4),
        ('./csv_with_slicing_2-3x.csv', 5, 7),
        ('./csv_with_slicing_3-2x.csv', 8, 9),
        ('./csv_with_slicing_4-3x.csv', 10, 12),
    ]

    for i in range(len(tgz_files)):
        expected = COMMON_FILES + [name for name, lo, hi in file_ranges if lo <= i <= hi]
        files = read_tarball(tgz_files[i])
        assert sorted(files) == sorted(expected), f'tarball {i}'


def test_manifest_and_status(collector):
    collector.collector_module = metrics_utility.test.gather.support.collector_module2
    tgz_files = collector.gather()

    assert len(tgz_files) == 1

    files = read_tarball(tgz_files[0])

    assert sorted(files) == sorted(COMMON_FILES + ['./json1.json', './json2.json', './json3.json'])

    assert json.loads(files['./manifest.json']) == {
        'config.json': '1.0',
        'data_collection_status.csv': '1.0',
        'json1.json': '1.1',
        'json2.json': '1.2',
        'json3.json': '1.3',
    }

    _assert_data_collection_status(files['./data_collection_status.csv'])


def _assert_data_collection_status(status_bytes):
    lines = status_bytes.splitlines(True)
    files = ['json1.json', 'json2.json', 'json3.json']
    assert len(lines) == len(files) + 1  # +1 == header

    fieldnames = [
        'collection_start_timestamp',
        'since',
        'until',
        'file_name',
        'status',
        'elapsed',
    ]
    header = lines.pop(0)
    assert decode_csv_line(header) == fieldnames
    for line in lines:
        row = decode_csv_line(line)
        assert len(row) == len(fieldnames)
        assert row[4] == 'ok'  # status
        assert row[5] == '0'  # elapsed
        files.pop(files.index(row[3]))

    assert len(files) == 0
