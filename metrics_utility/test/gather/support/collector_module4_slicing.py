from metrics_utility.gather.decorators import register
from metrics_utility.test.gather.support.helpers import (
    TIMESTAMP_CSV_LINE_LENGTH,
    one_day_slicing,
    timestamp_csv,
)


@register('config', '1.0')
def config(since, **kwargs):
    return {'version': '1.0'}


@register(
    'csv_one_day_slicing_1',
    '1.0',
    output_format='csv',
    slicing=one_day_slicing,
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
    output_format='csv',
    slicing=one_day_slicing,
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
