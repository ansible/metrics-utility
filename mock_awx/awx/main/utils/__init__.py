from django.utils.dateparse import parse_datetime


def get_awx_version():
    "24.6.123"


def get_awx_http_client_headers():
    return {
        'Content-Type': 'application/json',
        'User-Agent': '{} {} ({})'.format('AWX', get_awx_version(), 'UNLICENSED'),
    }


def datetime_hook(d):
    new_d = {}
    for key, value in d.items():
        try:
            new_d[key] = parse_datetime(value)
        except TypeError:
            new_d[key] = value
    return new_d
