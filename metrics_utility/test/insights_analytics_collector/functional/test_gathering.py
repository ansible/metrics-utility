import json
import logging
import tarfile

import pytest
import tests.functional.collector_module
import tests.functional.collector_module2
import tests.functional.collector_module3
from tests.classes.analytics_collector import AnalyticsCollector
from tests.functional.helpers import assert_common_files, decode_csv_line


@pytest.fixture
def collector(mocker):
    collector = AnalyticsCollector(
        collector_module=tests.functional.collector_module,
        collection_type=AnalyticsCollector.DRY_RUN,
    )
    mocker.patch.object(collector, "_is_valid_license", return_value=True)

    return collector


def test_missing_config(mocker, collector):
    mock_logger = mocker.patch.object(collector, "logger")

    tgz_files = collector.gather(subset=["json_collection_1", "json_collection_2"])

    assert tgz_files is None
    mock_logger.log.assert_called_with(
        logging.ERROR, "'config' collector data is missing"
    )


def test_json_collections(collector):
    tgz_files = collector.gather(
        subset=["config", "json_collection_1", "json_collection_2"]
    )

    assert len(tgz_files) == 1

    files = {}
    with tarfile.open(tgz_files[0], "r:gz") as archive:
        for member in archive.getmembers():
            files[member.name] = archive.extractfile(member)

        assert_common_files(files)
        assert "./json_collection_1.json" in files.keys()
        assert "./json_collection_2.json" in files.keys()

        assert json.loads(files["./config.json"].read()) == {"version": "1.0"}
        assert json.loads(files["./json_collection_1.json"].read()) == {"json1": "True"}
        assert json.loads(files["./json_collection_2.json"].read()) == {"json2": "True"}

    collector._gather_cleanup()


def test_small_csvs(collector):
    tgz_files = collector.gather(
        subset=["config", "csv_collection_1", "csv_collection_2", "csv_collection_3"]
    )

    assert len(tgz_files) == 1

    files = {}
    with tarfile.open(tgz_files[0], "r:gz") as archive:
        for member in archive.getmembers():
            files[member.name] = archive.extractfile(member)

        assert_common_files(files)
        assert "./csv_collection_1.csv" in files.keys()
        assert "./csv_collection_2.csv" in files.keys()
        assert "./csv_collection_3.csv" in files.keys()

        # length defined by @registered function
        assert len(files["./csv_collection_1.csv"].read()) == 100
        assert len(files["./csv_collection_2.csv"].read()) == 200
        assert len(files["./csv_collection_3.csv"].read()) == 300

    collector._gather_cleanup()


def test_jsons_with_csvs_with_slicing(collector):
    tgz_files = collector.gather(
        subset=[
            "config",
            "json_collection_1",
            "json_collection_2",
            "csv_slicing_1",
            "csv_slicing_2",
        ]
    )

    assert len(tgz_files) == 3

    for i in range(len(tgz_files)):
        files = {}
        with tarfile.open(tgz_files[i], "r:gz") as archive:
            for member in archive.getmembers():
                files[member.name] = archive.extractfile(member)

            assert_common_files(files)
            if i == 0:
                assert "./json_collection_1.json" in files.keys()
                assert "./json_collection_2.json" in files.keys()
                assert "./csv_slicing_1.csv" in files.keys()
            if i == 1:
                assert "./csv_slicing_2.csv" in files.keys()
            if i == 2:
                assert "./csv_slicing_2.csv" in files.keys()


def test_one_csv_collection_splitted_by_size(collector):
    tgz_files = collector.gather(subset=["config", "big_table"])

    assert len(tgz_files) == 10

    for i in range(len(tgz_files)):
        files = {}
        with tarfile.open(tgz_files[i], "r:gz") as archive:
            for member in archive.getmembers():
                files[member.name] = archive.extractfile(member)

            assert_common_files(files)
            assert len(files.keys()) == 1 + _common_files_count()
            assert "./big_table.csv" in files.keys()
            assert len(files["./big_table.csv"].read()) == 1000

    collector._gather_cleanup()


def test_multiple_collections_multiple_tarballs(mocker, collector):
    mocker.patch("tests.classes.package.Package.MAX_DATA_SIZE", 1000)

    tgz_files = collector.gather(
        subset=["config", "big_table_2", "csv_collection_1", "csv_collection_2"]
    )

    assert len(tgz_files) == 3

    for i in range(len(tgz_files)):
        files = {}
        with tarfile.open(tgz_files[i], "r:gz") as archive:
            for member in archive.getmembers():
                files[member.name] = archive.extractfile(member)

            assert_common_files(files)
            if i == 0:
                assert len(files.keys()) == 2 + _common_files_count()
                assert "./big_table_2.csv" in files.keys()
                assert "./csv_collection_1.csv" in files.keys()
            elif i == 1:
                assert len(files.keys()) == 2 + _common_files_count()
                assert "./big_table_2.csv" in files.keys()
                assert "./csv_collection_2.csv" in files.keys()
            elif i == 2:
                assert len(files.keys()) == 1 + _common_files_count()
                assert "./big_table_2.csv" in files.keys()

    collector._gather_cleanup()


def test_multiple_collections_and_distributions(collector):
    """
    - JSONs start at index 0
    - CSVs with no slicing start at index 0
    - CSVs with slicing start after index next to previous slice
    """
    collector.collector_module = tests.functional.collector_module3
    tgz_files = collector.gather()

    assert len(tgz_files) == 13

    for i in range(len(tgz_files)):
        files = {}
        with tarfile.open(tgz_files[i], "r:gz") as archive:
            for member in archive.getmembers():
                files[member.name] = archive.extractfile(member)

            assert_common_files(files)
            if i == 0:
                assert "./simple_json1.json" in files.keys()
            else:
                assert "./simple_json1.json" not in files.keys()

            # CSVs with no slicing start at index 0
            if 0 <= i <= 1:
                assert "./csv_no_slicing_1-2x.csv" in files.keys()
            else:
                assert "./csv_no_slicing_1-2x.csv" not in files.keys()

            if i == 0:
                assert "./csv_no_slicing_2-1x.csv" in files.keys()
            else:
                assert "./csv_no_slicing_2-1x.csv" not in files.keys()

            if 0 <= i <= 9:
                assert "./csv_no_slicing_3-10x.csv" in files.keys()
            else:
                assert "./csv_no_slicing_3-10x.csv" not in files.keys()

            if 0 <= i <= 11:
                assert "./csv_no_slicing_4-12x.csv" in files.keys()
            else:
                assert "./csv_no_slicing_4-12x.csv" not in files.keys()

            # CSVs with slicing start after index next to previous slice
            if 0 <= i <= 4:
                assert "./csv_with_slicing_1-5x.csv" in files.keys()
            else:
                assert "./csv_with_slicing_1-5x.csv" not in files.keys()

            if 5 <= i <= 7:
                assert "./csv_with_slicing_2-3x.csv" in files.keys()
            else:
                assert "./csv_with_slicing_2-3x.csv" not in files.keys()

            if 8 <= i <= 9:
                assert "./csv_with_slicing_3-2x.csv" in files.keys()
            else:
                assert "./csv_with_slicing_3-2x.csv" not in files.keys()

            if 10 <= i <= 12:
                assert "./csv_with_slicing_4-3x.csv" in files.keys()
            else:
                assert "./csv_with_slicing_4-3x.csv" not in files.keys()


def test_manifest_and_status(collector):
    collector.collector_module = tests.functional.collector_module2
    tgz_files = collector.gather()

    assert len(tgz_files) == 1

    files = {}
    with tarfile.open(tgz_files[0], "r:gz") as archive:
        for member in archive.getmembers():
            files[member.name] = archive.extractfile(member)

        assert_common_files(files)
        assert len(files.keys()) == 3 + _common_files_count()

        assert json.loads(files["./manifest.json"].read()) == {
            "config.json": "1.0",
            "data_collection_status.csv": "1.0",
            "json1.json": "1.1",
            "json2.json": "1.2",
            "json3.json": "1.3",
        }

        _assert_data_collection_status(files["./data_collection_status.csv"])

    collector._gather_cleanup()


def _assert_data_collection_status(status_file):
    lines = status_file.readlines()
    files = ["json1.json", "json2.json", "json3.json"]
    assert len(lines) == len(files) + 1  # +1 == header

    fieldnames = [
        "collection_start_timestamp",
        "since",
        "until",
        "file_name",
        "status",
        "elapsed",
    ]
    header = lines.pop(0)
    assert decode_csv_line(header) == fieldnames
    for line in lines:
        row = decode_csv_line(line)
        assert len(row) == len(fieldnames)
        assert row[4] == "ok"  # status
        assert row[5] == "0"  # elapsed
        files.pop(files.index(row[3]))

    assert len(files) == 0


def _common_files_count():
    return 3
