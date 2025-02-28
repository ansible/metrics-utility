import os

def get_optional_collectors():
    return os.environ.get('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').split(",")

environs = get_optional_collectors()

INCLUDE_INDIRECT = ('indirect_nodes' in environs)
DIRECT = 0
INDIRECT = 1
# later also EDGE = 2
