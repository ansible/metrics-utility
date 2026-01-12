from metrics_utility.anonymized_rollups.anonymized_rollups import (
    compute_anonymized_rollup_from_raw_data,
)

# from metrics_utility.test.util import run_gather_int
from metrics_utility.library.collectors.controller import (
    execution_environments,
    job_host_summary_service,
    main_jobevent_service,
    unified_jobs,
)
from metrics_utility.logger import logger


def compute_anonymized_rollup(db, salt, since, until, ship_path, save_rollups: bool = True, save_rollups_packed: bool = True):
    # This will contain list of files that belongs to particular collector
    execution_environments_data = []
    try:
        execution_environments_data = execution_environments(db=db).gather()
    except Exception as e:
        logger.error(f'Failed to gather execution_environments data: {e}')

    unified_jobs_data = []
    try:
        unified_jobs_data = unified_jobs(db=db, since=since, until=until).gather()
    except Exception as e:
        logger.error(f'Failed to gather unified_jobs data: {e}')

    job_host_summary_data = []
    try:
        job_host_summary_data = job_host_summary_service(db=db, since=since, until=until).gather()
    except Exception as e:
        logger.error(f'Failed to gather job_host_summary data: {e}')

    main_jobevent_data = []
    try:
        main_jobevent_data = main_jobevent_service(db=db, since=since, until=until).gather()
    except Exception as e:
        logger.error(f'Failed to gather main_jobevent data: {e}')

    input_data = {
        'execution_environments': execution_environments_data,
        'unified_jobs': unified_jobs_data,
        'job_host_summary': job_host_summary_data,
        'main_jobevent': main_jobevent_data,
    }

    # load data for each collector
    json_data = compute_anonymized_rollup_from_raw_data(input_data, salt, since, until, ship_path, save_rollups, save_rollups_packed)

    return json_data
