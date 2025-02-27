import os

def get_optional_collectors():
    return os.environ.get('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').split(",")