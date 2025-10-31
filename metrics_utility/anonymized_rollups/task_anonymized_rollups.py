from datetime import datetime, timedelta

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
from metrics_utility.test.util import run_gather_int


def task_anonymized_rollups(salt, year, month, day, ship_path, save_rollups: bool = True):
    env_vars = {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_jobevent_service,execution_environments,unified_jobs,job_host_summary_service',
        'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true',
    }

    datetime_since = datetime(year, month, day)
    datetime_until = datetime_since + timedelta(days=1)

    since_param = datetime_since.strftime('%Y-%m-%d')
    until_param = datetime_until.strftime('%Y-%m-%d')

    run_gather_int(env_vars, {'ship': True, 'force': True, 'since': since_param, 'until': until_param})

    # load data for each collector
    json_data = compute_anonymized_rollup_from_raw_data(salt, year, month, day, ship_path, save_rollups)

    return json_data
