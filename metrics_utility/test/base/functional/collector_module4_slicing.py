from base.functional.helpers import (
    TIMESTAMP_CSV_LINE_LENGTH,
    one_day_slicing,
    timestamp_csv,
)
from metrics_utility.base import register


@register('config', '1.0', description='CONFIG', config=True)
def config(since, **kwargs):
    return {'version': '1.0'}


@register(
    'csv_one_day_slicing_1',
    '1.0',
    format='csv',
    description='CSVs splitted by date',
    fnc_slicing=one_day_slicing,
)
def csv_one_day_slicing_1(**kwargs):
    since, until = kwargs.get('since', None), kwargs.get('until', None)
    return timestamp_csv(
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
    description='CSVs splitted by size and date',
    fnc_slicing=one_day_slicing,
)
def csv_one_day_slicing_2(**kwargs):
    since, until = kwargs.get('since', None), kwargs.get('until', None)
    return timestamp_csv(
        'csv_one_day_slicing_2',
        2,
        2 * TIMESTAMP_CSV_LINE_LENGTH,
        since=since,
        until=until,
    )
