from insights_analytics_collector import register
from tests.functional.helpers import (
    TIMESTAMP_CSV_LINE_LENGTH,
    full_sync_slicing,
    one_day_slicing,
    timestamp_csv,
)


@register("config", "1.0", description="CONFIG", config=True)
def config(since, **kwargs):
    return {"version": "1.0"}


@register(
    "csv_one_day_slicing_1",
    "1.0",
    format="csv",
    description="CSVs splitted by date",
    fnc_slicing=one_day_slicing,
)
def csv_one_day_slicing_1(since, full_path, until, **kwargs):
    return timestamp_csv(
        full_path,
        "csv_one_day_slicing_1",
        1,
        2 * TIMESTAMP_CSV_LINE_LENGTH,
        since=since,
        until=until,
    )


@register(
    "csv_one_day_slicing_2",
    "1.0",
    format="csv",
    description="CSVs splitted by size and date",
    fnc_slicing=one_day_slicing,
)
def csv_one_day_slicing_2(since, full_path, until, **kwargs):
    return timestamp_csv(
        full_path,
        "csv_one_day_slicing_2",
        2,
        2 * TIMESTAMP_CSV_LINE_LENGTH,
        since=since,
        until=until,
    )


@register(
    "csv_full_sync_slicing_1",
    "1.0",
    format="csv",
    description="CSVs splitted by date",
    fnc_slicing=full_sync_slicing,
    full_sync_interval_days=5,
)
def csv_full_sync_slicing_1(since, full_path, until, **kwargs):
    return timestamp_csv(
        full_path,
        "csv_full_sync_slicing_1",
        1,
        2 * TIMESTAMP_CSV_LINE_LENGTH,
        since=since,
        until=until,
    )
