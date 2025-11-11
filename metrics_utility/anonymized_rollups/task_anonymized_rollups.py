from datetime import datetime, timedelta

from metrics_utility.anonymized_rollups.anonymized_rollups import compute_anonymized_rollup_from_raw_data
#from metrics_utility.test.util import run_gather_int

from metrics_utility.library.collectors import execution_environments, unified_jobs, job_host_summary_service, main_jobevent_service

def task_anonymized_rollups(db, salt, year, month, day, ship_path, save_rollups: bool = True):
    '''
    env_vars = {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_jobevent_service,execution_environments,unified_jobs,job_host_summary_service',
        'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true',
    }
    '''

    datetime_since = datetime(year, month, day)
    datetime_until = datetime_since + timedelta(days=1)

    execution_environments_data = execution_environments(db, since=datetime_since, until=datetime_until)
    unified_jobs_data = unified_jobs(db, since=datetime_since, until=datetime_until)
    job_host_summary_data = job_host_summary_service(db, since=datetime_since, until=datetime_until)
    main_jobevent_data = main_jobevent_service(db, since=datetime_since, until=datetime_until)

    print(execution_environments_data)
    print(unified_jobs_data)
    print(job_host_summary_data)
    print(main_jobevent_data)

    #since_param = datetime_since.strftime('%Y-%m-%d')
    #until_param = datetime_until.strftime('%Y-%m-%d')

    #run_gather_int(env_vars, {'ship': True, 'force': True, 'since': since_param, 'until': until_param})

    # run gather from collectors in library

    # load data for each collector
    #json_data = compute_anonymized_rollup_from_raw_data(salt, year, month, day, ship_path, save_rollups)

    #return json_data
    return {}