import glob
import hashlib
import tarfile

import pandas as pd

from pandas import DataFrame

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup


def hash(value, salt):
    # has the value and salt, hash should be string
    combined = (salt + ':' + value).encode('utf-8')
    hashed = hashlib.sha512(combined).hexdigest()
    return hashed


def create_anonymized_object(rollup_name: str):
    if rollup_name == 'jobs':
        return JobsAnonymizedRollup()
    elif rollup_name == 'job_host_summary':
        return JobHostSummaryAnonymizedRollup()
    elif rollup_name == 'events_modules':
        return EventModulesAnonymizedRollup()
    elif rollup_name == 'execution_environments':
        return ExecutionEnvironmentsAnonymizedRollup()
    else:
        raise ValueError(f'Invalid rollup name: {rollup_name}')


def anonymize_data(data, salt):
    if not data or not isinstance(data, dict):
        return

    # anonymize jobs job template name
    if 'jobs' in data and data['jobs']:
        for job in data['jobs']:
            if job and 'job_template_name' in job and job['job_template_name']:
                job['job_template_name'] = hash(job['job_template_name'], salt)

    # anonymize jobhostsummary job template name
    if 'job_host_summary' in data and data['job_host_summary']:
        for jobhostsummary in data['job_host_summary']:
            if jobhostsummary and 'job_template_name' in jobhostsummary and jobhostsummary['job_template_name']:
                jobhostsummary['job_template_name'] = hash(jobhostsummary['job_template_name'], salt)

    # anonymize events modules module name
    if 'events_modules' in data and isinstance(data['events_modules'], dict):
        events_modules = data['events_modules']

        # list of modules to automate
        if 'list_of_modules_used_to_automate' in events_modules and events_modules['list_of_modules_used_to_automate']:
            for module in events_modules['list_of_modules_used_to_automate']:
                if module and module.get('collection_source') == 'Unknown':
                    if 'module_name' in module and module['module_name']:
                        module['module_name'] = hash(module['module_name'], salt)
                    if 'collection_name' in module and module['collection_name']:
                        module['collection_name'] = hash(module['collection_name'], salt)

        # module_stats
        if 'module_stats' in events_modules and events_modules['module_stats']:
            for module in events_modules['module_stats']:
                if module and module.get('collection_source') == 'Unknown':
                    if 'module_name' in module and module['module_name']:
                        module['module_name'] = hash(module['module_name'], salt)
                    if 'collection_name' in module and module['collection_name']:
                        module['collection_name'] = hash(module['collection_name'], salt)

        # collection_name_stats
        if 'collection_name_stats' in events_modules and events_modules['collection_name_stats']:
            for collection in events_modules['collection_name_stats']:
                if collection and collection.get('collection_source') == 'Unknown':
                    if 'module_name' in collection and collection['module_name']:
                        collection['module_name'] = hash(collection['module_name'], salt)
                    if 'collection_name' in collection and collection['collection_name']:
                        collection['collection_name'] = hash(collection['collection_name'], salt)

        # anonymize modules_used_per_playbook_total, playbook names
        if 'modules_used_per_playbook_total' in events_modules and events_modules['modules_used_per_playbook_total']:
            old_dict = events_modules['modules_used_per_playbook_total']
            new_dict = {}

            for playbook, modules in old_dict.items():
                if playbook:
                    hashed_playbook = hash(playbook, salt)
                    new_dict[hashed_playbook] = modules

            events_modules['modules_used_per_playbook_total'] = new_dict


def anonymize_rollups(events_modules_rollup, execution_environments_rollup, jobs_rollup, job_host_summary_rollup, salt):
    data = {
        'events_modules': events_modules_rollup,
        'execution_environments': execution_environments_rollup,
        'jobs': jobs_rollup,
        'job_host_summary': job_host_summary_rollup,
    }
    anonymize_data(data, salt)
    return data


def compute_anonymized_rollup_from_raw_data(salt, year, month, day):
    jobs = load_anonymized_rollup_data(JobsAnonymizedRollup(), './out', year, month, day)
    jobs_result = JobsAnonymizedRollup().base(jobs)
    JobsAnonymizedRollup().save_rollup(jobs_result['rollup'], './out', year, month, day)

    job_host_summary = load_anonymized_rollup_data(JobHostSummaryAnonymizedRollup(), './out', year, month, day)
    job_host_summary_result = JobHostSummaryAnonymizedRollup().base(job_host_summary)
    JobHostSummaryAnonymizedRollup().save_rollup(job_host_summary_result['rollup'], './out', year, month, day)

    events_modules = load_anonymized_rollup_data(EventModulesAnonymizedRollup(), './out', year, month, day)
    events_modules_result = EventModulesAnonymizedRollup().base(events_modules)
    EventModulesAnonymizedRollup().save_rollup(events_modules_result['rollup'], './out', year, month, day)

    execution_environments = load_anonymized_rollup_data(ExecutionEnvironmentsAnonymizedRollup(), './out', year, month, day)
    execution_environments_result = ExecutionEnvironmentsAnonymizedRollup().base(execution_environments)
    ExecutionEnvironmentsAnonymizedRollup().save_rollup(execution_environments_result['rollup'], './out', year, month, day)

    anonymized_rollup = anonymize_rollups(
        events_modules_result['json'], execution_environments_result['json'], jobs_result['json'], job_host_summary_result['json'], 'salt'
    )
    return anonymized_rollup


# loads data from tarballs located in base_path/data/year/month/day/*{collector_name}*.tar.gz
# inside tarball is file named {collector_name}.csv
# this goes to dataframe, then filter_function is applied to the dataframe
# all result dataframes are concatenated into one dataframe
def load_anonymized_rollup_data(rollup_object: BaseAnonymizedRollup, base_path: str, year: int, month: int, day: int) -> DataFrame:
    # list all tarballs in base_path/data/year/month/day/*{collector_name}*.tar.gz

    collection_names = rollup_object.collector_names

    tarballs = []
    for collection_name in collection_names:
        tarballs2 = glob.glob(f'{base_path}/data/{year}/{month:02d}/{day:02d}/*{collection_name}*.tar.gz')
        tarballs.extend(tarballs2)

    # load each tarball into a dataframe
    concat_dataframe = pd.DataFrame()

    for tarball in tarballs:
        with tarfile.open(tarball, 'r') as tar:
            for member in tar.getmembers():
                if member.name.endswith(f'{collection_name}.csv'):
                    df = pd.read_csv(tar.extractfile(member))

                    df = rollup_object.prepare(df)
                    concat_dataframe = rollup_object.merge(concat_dataframe, df)

    return concat_dataframe
