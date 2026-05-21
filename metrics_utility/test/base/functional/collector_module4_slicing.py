from base.functional.helpers import (
    TIMESTAMP_CSV_LINE_LENGTH,
    one_day_slicing,
    timestamp_csv,
)
from metrics_utility.gather.decorators import register


@register('config', '1.0', config=True)
def config(since, **kwargs):
    return {'version': '1.0'}


@register(
    'csv_one_day_slicing_1',
    '1.0',
    format='csv',
    fnc_slicing=one_day_slicing,
)
def csv_one_day_slicing_1(since, *, output, until, **kwargs):
    return timestamp_csv(
        output.full_path,
        'csv_one_day_slicing_1',
        1,
        2 * TIMESTAMP_CSV_LINE_LENGTH,
        since=since,
        until=until,
    )


@register(
    'csv_one_day_slicing_2',
    '1.0',
    format='csv',
    fnc_slicing=one_day_slicing,
)
def csv_one_day_slicing_2(since, *, output, until, **kwargs):
    return timestamp_csv(
        output.full_path,
        'csv_one_day_slicing_2',
        2,
        2 * TIMESTAMP_CSV_LINE_LENGTH,
        since=since,
        until=until,
    )
