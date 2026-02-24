from metrics_utility.base import register


@register('config', '1.0', config=True)
def config(since, **kwargs):
    return {'version': '1.0'}


@register('json1', '1.1')
def json1(**kwargs):
    return {'json1': 'True'}


@register('json2', '1.2')
def json2(**kwargs):
    return {'json2': 'True'}


@register('json3', '1.3')
def json3(**kwargs):
    return {'json3': 'True'}
