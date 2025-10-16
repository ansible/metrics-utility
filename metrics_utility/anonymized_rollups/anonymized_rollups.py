from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup

import glob
import tarfile
import pandas as pd
from pandas import DataFrame

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
        raise ValueError(f"Invalid rollup name: {rollup_name}")

def anonymize_data(data, salt):
    # anonymize jobs job template name
    if 'jobs' in data:
        for job in data['jobs']:
            job['job_template_name'] = hash(job['job_template_name'], salt)

    # anonymize jobhostsummary job template name
    if 'jobhostsummary' in data:
        for jobhostsummary in data['jobhostsummary']:
            jobhostsummary['job_template_name'] = hash(jobhostsummary['job_template_name'], salt)

    # anonymize events modules module name
    if 'events_modules' in data:
        # list of modules to automate
        for module in data['events_modules']['list_of_modules_used_to_automate']:
            if module['collection_source'] == 'Unknown':
                module['module_name'] = hash(module['module_name'], salt)
                module['collection_name'] = hash(module['collection_name'], salt)

        # module_stats
        for module in data['events_modules']['module_stats']:
            if module['collection_source'] == 'Unknown':
                module['module_name'] = hash(module['module_name'], salt)
                module['collection_name'] = hash(module['collection_name'], salt)

        # collection_name_stats
        for collection in data['events_modules']['collection_name_stats']:
            if collection['collection_source'] == 'Unknown':
                collection['module_name'] = hash(module['module_name'], salt)
                collection['collection_name'] = hash(collection['collection_name'], salt)

    # anonymize modules_used_per_playbook_total, playbook names
    if 'modules_used_per_playbook_total' in data['events_modules']:
        old_dict = data['events_modules']['modules_used_per_playbook_total']
        new_dict = {}

        for playbook, modules in old_dict.items():
            hashed_playbook = hash(playbook, salt)  # replace with your actual hash function
            new_dict[hashed_playbook] = modules

        data['events_modules']['modules_used_per_playbook_total'] = new_dict


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
    jobs_rollup = JobsAnonymizedRollup().base(jobs)
    JobsAnonymizedRollup().save_rollup(jobs_rollup, './out', year, month, day)

    job_host_summary = load_anonymized_rollup_data(JobHostSummaryAnonymizedRollup(), './out', year, month, day)
    job_host_summary_rollup = JobHostSummaryAnonymizedRollup.base(job_host_summary)
    JobHostSummaryAnonymizedRollup().save_rollup(job_host_summary_rollup, './out', year, month, day)

    job_events = load_anonymized_rollup_data(EventModulesAnonymizedRollup(), './out', year, month, day)
    job_events_rollup = EventModulesAnonymizedRollup.base(job_events)
    EventModulesAnonymizedRollup().save_rollup(job_events_rollup, './out', year, month, day)

    execution_environments = load_anonymized_rollup_data(ExecutionEnvironmentsAnonymizedRollup(), './out', year, month, day)
    execution_environments_rollup = ExecutionEnvironmentsAnonymizedRollup.base(execution_environments) 
    ExecutionEnvironmentsAnonymizedRollup().save_rollup(execution_environments_rollup, './out', year, month, day)

    anonymized_rollup = anonymize_rollups(job_events_rollup, execution_environments_rollup, jobs_rollup, job_host_summary_rollup, 'salt')
    return anonymized_rollup

# loads data from tarballs located in base_path/data/year/month/day/*{collector_name}*.tar.gz
# inside tarball is file named {collector_name}.csv
# this goes to dataframe, then filter_function is applied to the dataframe
# all result dataframes are concatenated into one dataframe
def load_anonymized_rollup_data(rollup_object: BaseAnonymizedRollup, base_path: str, year: int, month: int, day: int) -> DataFrame:
    # list all tarballs in base_path/data/year/month/day/*{collector_name}*.tar.gz

    collection_name = rollup_object.rollup_name
    tarballs = glob.glob(f'{base_path}/data/{year}/{month:02d}/{day:02d}/*{collection_name}*.tar.gz')

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




