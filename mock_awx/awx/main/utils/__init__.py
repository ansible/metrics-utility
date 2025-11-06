def get_awx_version():
    "24.6.123"


def get_awx_http_client_headers():
    return {
        'Content-Type': 'application/json',
        'User-Agent': '{} {} ({})'.format('AWX', get_awx_version(), 'UNLICENSED'),
    }
