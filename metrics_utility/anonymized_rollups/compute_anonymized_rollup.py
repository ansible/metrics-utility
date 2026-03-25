from metrics_utility.anonymized_rollups.anonymized_rollups import (
    compute_anonymized_rollup_from_raw_data,
)

# from metrics_utility.test.util import run_gather_int
from metrics_utility.library.collectors.controller import (
    controller_version_service,
    credentials_service,
    execution_environments,
    feature_flags_service,
    job_host_summary_service,
    main_jobevent_service,
    table_metadata,
    unified_jobs,
)
from metrics_utility.logger import logger


def compute_anonymized_rollup(db, salt, since, until):
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

    credentials_data = []
    try:
        credentials_data = credentials_service(db=db, since=since, until=until).gather()
    except Exception as e:
        logger.error(f'Failed to gather credentials data: {e}')

    table_metadata_data = []
    try:
        table_metadata_data = table_metadata(db=db).gather()
    except Exception as e:
        logger.error(f'Failed to gather table_metadata data: {e}')

    controller_version_data = []
    try:
        controller_version_data = controller_version_service(db=db).gather()
    except Exception as e:
        logger.error(f'Failed to gather controller_version data: {e}')

    feature_flags_data = []
    try:
        feature_flags_data = feature_flags_service(db=db).gather()
    except Exception as e:
        logger.error(f'Failed to gather feature_flags data: {e}')

    input_data = {
        'execution_environments': execution_environments_data,
        'unified_jobs': unified_jobs_data,
        'job_host_summary': job_host_summary_data,
        'main_jobevent': main_jobevent_data,
        'credentials': credentials_data,
        'table_metadata': table_metadata_data,
        'controller_version': controller_version_data,
        'feature_flags': feature_flags_data,
    }

    # load data for each collector
    json_data = compute_anonymized_rollup_from_raw_data(input_data, salt)

    return json_data
