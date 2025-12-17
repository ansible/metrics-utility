import hashlib

from typing import Any, Dict, List

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup


def hash(value, salt):
    # has the value and salt, hash should be string
    combined = (salt + ':' + value).encode('utf-8')
    hashed = hashlib.sha256(combined).hexdigest()
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
    """
    Anonymizes sensitive data in the flattened report structure.
    This function expects data to be already flattened by flatten_json_report().

    Args:
        data: Flattened data structure with keys:
            - jobs_by_template: array of job template stats
            - job_host_summary: array of host summary stats
            - module_stats: array of module statistics
            - collection_name_stats: array of collection statistics
            - modules_used_per_playbook: array of {playbook_id, modules_used}
        salt: Salt string for hashing
    """
    if not data or not isinstance(data, dict):
        return

    # anonymize jobs_by_template job template name
    if 'jobs_by_template' in data and data['jobs_by_template']:
        for job in data['jobs_by_template']:
            if job and 'job_template_name' in job and job['job_template_name']:
                job['job_template_name'] = hash(job['job_template_name'], salt)

    # anonymize job_host_summary job template name
    if 'job_host_summary' in data and data['job_host_summary']:
        for jobhostsummary in data['job_host_summary']:
            if jobhostsummary and 'job_template_name' in jobhostsummary and jobhostsummary['job_template_name']:
                jobhostsummary['job_template_name'] = hash(jobhostsummary['job_template_name'], salt)

    # anonymize module_stats - anonymize module name and collection name for 'Unknown' sources
    if 'module_stats' in data and data['module_stats']:
        for module in data['module_stats']:
            if module and module.get('collection_source') == 'Unknown':
                if 'module_name' in module and module['module_name']:
                    module['module_name'] = hash(module['module_name'], salt)
                if 'collection_name' in module and module['collection_name']:
                    module['collection_name'] = hash(module['collection_name'], salt)

    # anonymize collection_name_stats - anonymize collection name for 'Unknown' sources
    if 'collection_name_stats' in data and data['collection_name_stats']:
        for collection in data['collection_name_stats']:
            if collection and collection.get('collection_source') == 'Unknown':
                if 'collection_name' in collection and collection['collection_name']:
                    collection['collection_name'] = hash(collection['collection_name'], salt)

    # anonymize modules_used_per_playbook - anonymize playbook_id (which is the playbook name)
    if 'modules_used_per_playbook' in data and data['modules_used_per_playbook']:
        for playbook_entry in data['modules_used_per_playbook']:
            if playbook_entry and 'playbook_id' in playbook_entry and playbook_entry['playbook_id']:
                playbook_entry['playbook_id'] = hash(playbook_entry['playbook_id'], salt)


def flatten_json_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Manually flattens the given nested report into:
      - statistics: object of primitive totals
      - modules_used_per_playbook: array of {playbook_id, modules_used}
      - module_stats: array (copied as-is)
      - collection_name_stats: array (copied as-is)
      - jobs_by_template: array (copied as-is)
      - job_host_summary: array (copied as-is)
    """
    events_modules = data.get('events_modules', {})
    execution_environments = data.get('execution_environments', {})
    jobs = data.get('jobs', {})
    job_host_summary_root = data.get('job_host_summary', {})

    # Handle edge case: job_host_summary might be list instead of dict for empty data
    if isinstance(job_host_summary_root, list):
        job_host_summary_root = {}

    # 1) statistics (collect only primitive totals)
    statistics = {
        # from events_modules
        'modules_used_to_automate_total': events_modules.get('modules_used_to_automate_total'),
        'avg_number_of_modules_used_in_a_playbooks': events_modules.get('avg_number_of_modules_used_in_a_playbooks'),
        'hosts_automated_total': events_modules.get('hosts_automated_total'),
        'event_total': events_modules.get('event_total'),
        # from execution_environments
        'EE_total': execution_environments.get('EE_total'),
        'EE_default_total': execution_environments.get('EE_default_total'),
        'EE_custom_total': execution_environments.get('EE_custom_total'),
        # from jobs
        'jobs_total': jobs.get('jobs_total'),
        # from job_host_summary
        'unique_hosts_total': job_host_summary_root.get('unique_hosts_total'),
        'jobhostsummary_total': job_host_summary_root.get('jobhostsummary_total'),
    }

    # 2) modules_used_per_playbook (convert map -> array)
    mup_map: Dict[str, int] = events_modules.get('modules_used_per_playbook_total', {}) or {}
    modules_used_per_playbook: List[Dict[str, Any]] = [
        {'playbook_id': playbook_id, 'modules_used': modules_used} for playbook_id, modules_used in mup_map.items()
    ]

    # 3) arrays copied as-is from their respective parents
    module_stats: List[Dict[str, Any]] = events_modules.get('module_stats', []) or []
    collection_name_stats: List[Dict[str, Any]] = events_modules.get('collection_name_stats', []) or []
    jobs_by_template: List[Dict[str, Any]] = jobs.get('by_template', []) or []
    job_host_summary: List[Dict[str, Any]] = job_host_summary_root.get('aggregated', []) or []

    # 4) assemble the flattened object
    flattened: Dict[str, Any] = {
        'statistics': statistics,
        'modules_used_per_playbook': modules_used_per_playbook,
        'module_stats': module_stats,
        'collection_name_stats': collection_name_stats,
        'jobs_by_template': jobs_by_template,
        'job_host_summary': job_host_summary,
    }

    return flattened


def anonymize_rollups(events_modules_rollup, execution_environments_rollup, jobs_rollup, job_host_summary_rollup, salt):
    """
    Combines rollup data, flattens it, and anonymizes sensitive fields.

    Args:
        events_modules_rollup: Event modules statistics
        execution_environments_rollup: Execution environment statistics
        jobs_rollup: Jobs statistics
        job_host_summary_rollup: Job host summary statistics
        salt: Salt string for hashing sensitive data

    Returns:
        Flattened and anonymized rollup data
    """
    data = {
        'events_modules': events_modules_rollup,
        'execution_environments': execution_environments_rollup,
        'jobs': jobs_rollup,
        'job_host_summary': job_host_summary_rollup,
    }

    # First flatten the nested structure
    data = flatten_json_report(data)

    # Then anonymize the flattened structure
    anonymize_data(data, salt)

    return data


def compute_anonymized_rollup_from_raw_data(input_data, salt, since, until, base_path, save_rollups: bool = True, save_rollups_packed: bool = True):
    jobs = load_anonymized_rollup_data(JobsAnonymizedRollup(), input_data['unified_jobs'])
    jobs_result = JobsAnonymizedRollup().base(jobs)
    if save_rollups:
        JobsAnonymizedRollup().save_rollup(jobs_result['rollup'], base_path, since, until, packed=save_rollups_packed)

    job_host_summary = load_anonymized_rollup_data(JobHostSummaryAnonymizedRollup(), input_data['job_host_summary'])
    job_host_summary_result = JobHostSummaryAnonymizedRollup().base(job_host_summary)
    if save_rollups:
        JobHostSummaryAnonymizedRollup().save_rollup(job_host_summary_result['rollup'], base_path, since, until, packed=save_rollups_packed)

    events_modules = load_anonymized_rollup_data(EventModulesAnonymizedRollup(), input_data['main_jobevent'])
    events_modules_result = EventModulesAnonymizedRollup().base(events_modules)
    if save_rollups:
        EventModulesAnonymizedRollup().save_rollup(events_modules_result['rollup'], base_path, since, until, packed=save_rollups_packed)

    execution_environments = load_anonymized_rollup_data(ExecutionEnvironmentsAnonymizedRollup(), input_data['execution_environments'])
    execution_environments_result = ExecutionEnvironmentsAnonymizedRollup().base(execution_environments)
    if save_rollups:
        ExecutionEnvironmentsAnonymizedRollup().save_rollup(
            execution_environments_result['rollup'], base_path, since, until, packed=save_rollups_packed
        )

    anonymized_rollup = anonymize_rollups(
        events_modules_result['json'], execution_environments_result['json'], jobs_result['json'], job_host_summary_result['json'], salt
    )
    # Sanitize the result to replace NaN and infinity values with None (valid JSON)
    anonymized_rollup = sanitize_json(anonymized_rollup)
    return anonymized_rollup


# loads data from tarballs located in base_path/data/year/month/day/*{collector_name}*.tar.gz
# inside tarball is file named {collector_name}.csv
# this goes to dataframe, then filter_function is applied to the dataframe
# all result dataframes are concatenated into one dataframe
def load_anonymized_rollup_data(rollup_object: BaseAnonymizedRollup, file_list: []):
    # file_list - list of csv files that needs to be read

    concat_data = None

    for file in file_list:
        df = pd.read_csv(file, encoding='utf-8')
        prepared_data = rollup_object.prepare(df)
        concat_data = rollup_object.merge(concat_data, prepared_data)

    return concat_data
