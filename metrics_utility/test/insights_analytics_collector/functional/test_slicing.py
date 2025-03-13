import datetime
import tarfile

import pytest
import pytz
import tests.functional.collector_module4_slicing
from django.utils.timezone import now, timedelta
from tests.classes.analytics_collector import AnalyticsCollector
from tests.functional.helpers import assert_common_files, decode_csv_line


@pytest.fixture
def collector(mocker):
    """
    Collector with registered module `collector_module4_slicing`.
    It's designed to use non-trivial slicing functions
    """
    collector = AnalyticsCollector(
        collector_module=tests.functional.collector_module4_slicing,
        collection_type=AnalyticsCollector.DRY_RUN,
    )
    mocker.patch.object(collector, "_is_valid_license", return_value=True)

    return collector


def test_slices_by_date(collector):
    """
    Slicing by fnc_slicing function `one_day_slicing`.
    Splits data to 10 days/files.
    """
    days_to_collect = 10

    until = now().replace(hour=0, minute=0, second=0, microsecond=0)
    since = until - timedelta(days=days_to_collect)

    tgz_files = collector.gather(
        subset=["config", "csv_one_day_slicing_1"], since=since, until=until
    )

    assert len(tgz_files) == days_to_collect

    idx = 0
    while since < until:
        files = {}
        with tarfile.open(tgz_files[idx], "r:gz") as archive:
            for member in archive.getmembers():
                files[member.name] = archive.extractfile(member)

            assert_common_files(files)
            assert "./csv_one_day_slicing_1.csv" in files.keys()

            lines = files["./csv_one_day_slicing_1.csv"].readlines()
            _header = lines.pop(0)
            row = decode_csv_line(lines[0])

            csv_since = datetime.datetime(
                int(row[0]),
                int(row[1]),
                int(row[2]),
                int(row[3]),
                int(row[4]),
                int(row[5]),
                tzinfo=pytz.utc,
            )
            csv_until = datetime.datetime(
                int(row[6]),
                int(row[7]),
                int(row[8]),
                int(row[9]),
                int(row[10]),
                int(row[11]),
                tzinfo=pytz.utc,
            )

            assert csv_since == since
            assert csv_until == since + timedelta(days=1)

        idx += 1
        since += timedelta(days=1)


def test_slices_by_date_and_size(collector):
    """
    Slicing by fnc_slicing function `one_day_slicing()`.
    Splits data to 10 days/files.
    Also splits each day to 2 files by size (CsvFileSplitter)
    => total 20 files
    """
    days_to_collect = 10

    until = now().replace(hour=0, minute=0, second=0, microsecond=0)
    since = until - timedelta(days=days_to_collect)

    tgz_files = collector.gather(
        subset=["config", "csv_one_day_slicing_2"], since=since, until=until
    )

    assert len(tgz_files) == days_to_collect * 2


@pytest.mark.parametrize("last_sync_days_ago", [4, 6])
def test_slices_by_full_sync(mocker, collector, last_sync_days_ago):
    """
    In the collector method `csv_full_sync_slicing_1()` there is 5 days interval for full sync
    if `last_sync_days_ago` is:
    - 4 days ago, it uses `since` (7 days ago)
    - 6 days ago, it uses slicing interval (10 days ago) (in `full_sync_slicing()`)

    """
    last_gathered_entries = {
        "csv_full_sync_slicing_1": (now() - timedelta(days=7)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ),
        "csv_full_sync_slicing_1_full": now() - timedelta(days=last_sync_days_ago),
    }
    mocker.patch.object(
        collector, "_load_last_gathered_entries", return_value=last_gathered_entries
    )

    tgz_files = collector.gather(
        subset=["config", "csv_full_sync_slicing_1"],
        since=last_gathered_entries["csv_full_sync_slicing_1"],
    )

    if last_sync_days_ago == 4:
        assert len(tgz_files) == 7
    else:
        assert len(tgz_files) == 10
