import datetime

import pytest

from django.utils.timezone import now, timedelta

import metrics_utility.test.gather.support.collector_module4_slicing

from metrics_utility.test.gather.support.analytics_collector import AnalyticsCollector
from metrics_utility.test.gather.support.helpers import decode_csv_line, read_tarball


COMMON_FILES = ['./config.json', './data_collection_status.csv', './manifest.json']


@pytest.fixture
def collector(mocker):
    """
    Collector with registered module `collector_module4_slicing`.
    It's designed to use non-trivial slicing functions
    """
    collector = AnalyticsCollector(
        collector_module=metrics_utility.test.gather.support.collector_module4_slicing,
        collection_type=AnalyticsCollector.DRY_RUN,
    )

    yield collector

    collector._gather_cleanup()


def test_slices_by_date(collector):
    """
    Slicing by slicing function `one_day_slicing`.
    Splits data to 10 days/files.
    """
    days_to_collect = 10

    until = now().replace(hour=0, minute=0, second=0, microsecond=0)
    since = until - timedelta(days=days_to_collect)

    tgz_files = collector.gather(subset=['config', 'csv_one_day_slicing_1'], since=since, until=until)

    assert len(tgz_files) == days_to_collect

    idx = 0
    while since < until:
        files = read_tarball(tgz_files[idx])

        assert sorted(files) == sorted(COMMON_FILES + ['./csv_one_day_slicing_1.csv'])

        lines = files['./csv_one_day_slicing_1.csv'].splitlines(True)
        _header = lines.pop(0)
        row = decode_csv_line(lines[0])

        csv_since = datetime.datetime(
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]),
            int(row[4]),
            int(row[5]),
            tzinfo=datetime.timezone.utc,
        )
        csv_until = datetime.datetime(
            int(row[6]),
            int(row[7]),
            int(row[8]),
            int(row[9]),
            int(row[10]),
            int(row[11]),
            tzinfo=datetime.timezone.utc,
        )

        assert csv_since == since
        assert csv_until == since + timedelta(days=1)

        idx += 1
        since += timedelta(days=1)


def test_slices_by_date_and_size(collector):
    """
    Slicing by slicing function `one_day_slicing`.
    Splits data to 10 days/files.
    Also splits each day to 2 files by size (CsvFileSplitter)
    => total 20 files
    """
    days_to_collect = 10

    until = now().replace(hour=0, minute=0, second=0, microsecond=0)
    since = until - timedelta(days=days_to_collect)

    tgz_files = collector.gather(subset=['config', 'csv_one_day_slicing_2'], since=since, until=until)

    assert len(tgz_files) == days_to_collect * 2
