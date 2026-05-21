from base.functional.helpers import simple_csv, trivial_slicing
from metrics_utility.gather.decorators import register


@register('config', '1.0')
def config(since, **kwargs):
    return {'version': '1.0'}


@register('simple_json1', '1.0')
def simple_json1(**kwargs):
    return {'simple_json': 'True'}


@register('csv_no_slicing_1-2x', '1.0', output_format='csv')
def csv_no_slicing1(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_no_slicing_1-2x', 2, 100)


@register(
    'csv_with_slicing_1-5x',
    '1.0',
    output_format='csv',
    slicing=trivial_slicing,
)
def csv_with_slicing1a(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_with_slicing_1-5x', 2, 100)


@register(
    'csv_with_slicing_1-5x',
    '1.0',
    output_format='csv',
    slicing=trivial_slicing,
)
def csv_with_slicing1b(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_with_slicing_1-5x', 3, 100)


@register('csv_no_slicing_2-1x', '1.0', output_format='csv')
def csv_no_slicing2(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_no_slicing_2-1x', 1, 100)


@register(
    'csv_with_slicing_2-3x',
    '1.0',
    output_format='csv',
    slicing=trivial_slicing,
)
def csv_with_slicing2a(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_with_slicing_2-3x', 2, 100)


@register('csv_no_slicing_3-10x', '1.0', output_format='csv')
def csv_no_slicing3(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_no_slicing_3-10x', 10, 100)


@register(
    'csv_with_slicing_3-2x',
    '2.0',
    output_format='csv',
    slicing=trivial_slicing,
)
def csv_with_slicing3(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_with_slicing_3-2x', 2, 100)


@register('csv_no_slicing_4-12x', '1.0', output_format='csv')
def csv_no_slicing4(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_no_slicing_4-12x', 12, 100)


@register(
    'csv_with_slicing_2-3x',
    '2.0',
    output_format='csv',
    slicing=trivial_slicing,
)
def csv_with_slicing2b(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_with_slicing_2-3x', 1, 100)


@register(
    'csv_with_slicing_4-3x',
    '2.0',
    output_format='csv',
    slicing=trivial_slicing,
)
def csv_with_slicing4(*, output, **kwargs):
    return simple_csv(output.full_path, 'csv_with_slicing_4-3x', 3, 100)
