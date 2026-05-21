from base.functional.helpers import simple_csv, trivial_slicing
from metrics_utility.gather.decorators import register


@register('config', '1.0', config=True)
def config(since, **kwargs):
    return {'version': '1.0'}


@register('big_table', '1.0', format='csv')
def big_table(*, output, **kwargs):
    return simple_csv(output.full_path, 'big_table', 10, 1000)


@register(
    'big_table_2',
    '1.0',
    format='csv',
)
def big_table_2(*, output, **kwargs):
    return simple_csv(output.full_path, 'big_table', 3, 800)


@register('csv_collection_1', '1.0', format='csv')
def csv_collection_1(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_collection_1', 1, 100)


@register('csv_collection_2', '1.0', format='csv')
def csv_collection_2(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_collection_2', 1, 200)


@register('csv_collection_3', '1.0', format='csv')
def csv_collection_3(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_collection_3', 1, 300)


@register(
    'csv_slicing_1',
    '1.0',
    format='csv',
    fnc_slicing=trivial_slicing,
)
def csv_slicing_1(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_slicing_1', 1, 100)


@register(
    'csv_slicing_2',
    '1.0',
    format='csv',
    fnc_slicing=trivial_slicing,
)
def csv_slicing_2(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_slicing_2', 2, 100)


@register('json_collection_1', '1.0', format='json')
def json_collection_1(**kwargs):
    return {'json1': 'True'}


@register('json_collection_2', '2.0', format='json')
def json_collection_2(**kwargs):
    return {'json2': 'True'}
