import os


def get_optional_collectors():
    return os.environ.get('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').split(',')


DIRECT = 0
INDIRECT = 1
# later also EDGE = 2

MANAGED_NODE_TYPES = {DIRECT: 'DIRECT', INDIRECT: 'INDIRECT'}
