"""
Test helpers for anonymized rollup tests.

These functions support test scenarios that load raw data (DataFrames or CSV file paths),
run the full rollup+anonymize pipeline, and write intermediate debug files to ./out/batches/.
They are intentionally NOT part of the production flow — production code calls the individual
rollup classes and anonymize_rollups() directly.
"""

import json
import os
import shutil

import pandas as pd

from metrics_utility.anonymized_rollups.anonymized_rollups import anonymize_rollups
from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup
from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json
from metrics_utility.anonymized_rollups.indirect_managed_nodes_anonymized_rollup import IndirectManagedNodesAnonymizedRollup
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup
from metrics_utility.anonymized_rollups.task_executions_anonymized_rollup import TaskExecutionsAnonymizedRollup
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
from metrics_utility.library.collectors.service import task_executions_service
from metrics_utility.logger import logger


OUT_BATCHES_DIR = './out/batches'


# loads data from a list of dataframes
# then filter_function is applied to the dataframe
# all result dataframes are concatenated into one dataframe
def load_anonymized_rollup_data(rollup_object: BaseAnonymizedRollup, dataframe_list):
    # compat for one dataframe
    if isinstance(dataframe_list, pd.DataFrame):
        prepared_data = rollup_object.prepare(dataframe_list)
        return rollup_object.merge(None, prepared_data)

    concat_data = None

    rollup_object_name = rollup_object.rollup_name

    counter = -1
    for dataframe in dataframe_list:
        counter += 1
        # compat for CSVs
        if isinstance(dataframe, str):
            dataframe = pd.read_csv(dataframe, encoding='utf-8')

        prepared_data = rollup_object.prepare(dataframe)
        # serialize prepared data to json
        prepared_data = json.dumps(prepared_data, indent=2)
        # back to dict/list
        prepared_data = json.loads(prepared_data)
        concat_data = rollup_object.merge(concat_data, prepared_data)
        # concat data to json and then back
        concat_data = json.dumps(concat_data, indent=2)
        concat_data = json.loads(concat_data)

        # Create a subdirectory for this rollup
        rollup_batch_dir = os.path.join(OUT_BATCHES_DIR, rollup_object_name)
        os.makedirs(rollup_batch_dir, exist_ok=True)
        # save prepare data and concat data to separate files (as json pretty printed)
        # Each rollup has its own folder
        file1_name = os.path.join(rollup_batch_dir, f'{counter}_prepare.json')
        file2_name = os.path.join(rollup_batch_dir, f'{counter}_xconcat.json')
        with open(file1_name, 'w') as f:
            f.write(json.dumps(prepared_data, indent=2))
        with open(file2_name, 'w') as f:
            f.write(json.dumps(concat_data, indent=2))

    return concat_data


def compute_anonymized_rollup_from_raw_data(input_data):

    # delete everything in the directory ./out/batches (including subdirectories)
    if os.path.exists(OUT_BATCHES_DIR):
        shutil.rmtree(OUT_BATCHES_DIR, ignore_errors=True)

    jobs = load_anonymized_rollup_data(JobsAnonymizedRollup(), input_data['unified_jobs'])
    jobs_result = JobsAnonymizedRollup().base(jobs)

    job_host_summary = load_anonymized_rollup_data(JobHostSummaryAnonymizedRollup(), input_data['job_host_summary'])
    job_host_summary_result = JobHostSummaryAnonymizedRollup().base(job_host_summary)

    events_modules = load_anonymized_rollup_data(EventModulesAnonymizedRollup(), input_data['main_jobevent'])
    events_modules_result = EventModulesAnonymizedRollup().base(events_modules)

    execution_environments = load_anonymized_rollup_data(ExecutionEnvironmentsAnonymizedRollup(), input_data['execution_environments'])
    execution_environments_result = ExecutionEnvironmentsAnonymizedRollup().base(execution_environments)

    credentials = load_anonymized_rollup_data(CredentialsAnonymizedRollup(), input_data['credentials'])
    credentials_result = CredentialsAnonymizedRollup().base(credentials)

    table_metadata = load_anonymized_rollup_data(TableMetadataAnonymizedRollup(), input_data.get('table_metadata', []))
    table_metadata_result = TableMetadataAnonymizedRollup().base(table_metadata)

    controller_version = load_anonymized_rollup_data(ControllerVersionAnonymizedRollup(), input_data.get('controller_version', []))
    controller_version_result = ControllerVersionAnonymizedRollup().base(controller_version)

    feature_flags = load_anonymized_rollup_data(FeatureFlagsAnonymizedRollup(), input_data.get('feature_flags', []))
    feature_flags_result = FeatureFlagsAnonymizedRollup().base(feature_flags)

    task_executions = load_anonymized_rollup_data(TaskExecutionsAnonymizedRollup(), input_data.get('task_executions', []))
    task_executions_result = TaskExecutionsAnonymizedRollup().base(task_executions)

    indirect_managed_nodes = load_anonymized_rollup_data(IndirectManagedNodesAnonymizedRollup(), input_data.get('indirect_managed_nodes', []))
    indirect_managed_nodes_result = IndirectManagedNodesAnonymizedRollup().base(indirect_managed_nodes)

    anonymized_rollup = anonymize_rollups(
        events_modules_rollup=events_modules_result['json'],
        execution_environments_rollup=execution_environments_result['json'],
        jobs_rollup=jobs_result['json'],
        job_host_summary_rollup=job_host_summary_result['json'],
        credentials_rollup=credentials_result['json'],
        table_metadata_rollup=table_metadata_result['json'],
        controller_version_rollup=controller_version_result['json'],
        feature_flags_rollup=feature_flags_result['json'],
        task_executions_rollup=task_executions_result['json'],
        indirect_managed_nodes_rollup=indirect_managed_nodes_result['json'],
    )
    # Sanitize the result to replace NaN and infinity values with None (valid JSON)
    anonymized_rollup = sanitize_json(anonymized_rollup)

    # save anonymized rollup to file
    os.makedirs(OUT_BATCHES_DIR, exist_ok=True)
    file_name = os.path.join(OUT_BATCHES_DIR, 'anonymized_rollup.json')
    with open(file_name, 'w') as f:
        f.write(json.dumps(anonymized_rollup, indent=2))
    return anonymized_rollup


def compute_anonymized_rollup(db, since, until, service_db=None):
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

    task_executions_data = []
    if service_db is not None:
        try:
            task_executions_data = task_executions_service(db=service_db, since=since, until=until).gather()
        except Exception as e:
            logger.error(f'Failed to gather task_executions data: {e}')

    input_data = {
        'execution_environments': execution_environments_data,
        'unified_jobs': unified_jobs_data,
        'job_host_summary': job_host_summary_data,
        'main_jobevent': main_jobevent_data,
        'credentials': credentials_data,
        'table_metadata': table_metadata_data,
        'controller_version': controller_version_data,
        'feature_flags': feature_flags_data,
        'task_executions': task_executions_data,
    }

    # load data for each collector
    json_data = compute_anonymized_rollup_from_raw_data(input_data)

    return json_data
