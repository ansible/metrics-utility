import hashlib

from typing import Any, Dict, List

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup
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
    elif rollup_name == 'credentials':
        return CredentialsAnonymizedRollup()
    else:
        raise ValueError(f'Invalid rollup name: {rollup_name}')


def anonymize_data(data, salt):
    """
    Anonymizes sensitive data in the flattened report structure.
    This function expects data to be already flattened by flatten_json_report().

    Args:
        data: Flattened data structure with keys:
            - jobs_by_job_type: array of job stats (grouped by job_type, merged with job_host_summary and credentials data)
            - jobs_by_launch_type: array of job stats (grouped by launch_type, with default host summary fields)
            - jobs_by_ansible_version: array of job stats (grouped by ansible_version, with default host summary fields)
            - module_stats: array of module statistics
            - collection_name_stats: array of collection statistics
            - modules_used_per_playbook: array of {playbook_id, modules_used}
            - collections_versions: array of {name, version, job_count} from installed collections
        salt: Salt string for hashing
    """
    if not data or not isinstance(data, dict):
        return

    # anonymize jobs_by_job_type job template name (if present)
    # Note: jobs_by_job_type is now grouped by job_type, but may still have job_template_name for templates_total
    if 'jobs_by_job_type' in data and data['jobs_by_job_type']:
        for job in data['jobs_by_job_type']:
            if job and 'job_template_name' in job and job['job_template_name']:
                job['job_template_name'] = hash(job['job_template_name'], salt)

    # anonymize jobs_by_launch_type job template name (if present)
    if 'jobs_by_launch_type' in data and data['jobs_by_launch_type']:
        for job in data['jobs_by_launch_type']:
            if job and 'job_template_name' in job and job['job_template_name']:
                job['job_template_name'] = hash(job['job_template_name'], salt)

    # anonymize jobs_by_ansible_version job template name (if present)
    if 'jobs_by_ansible_version' in data and data['jobs_by_ansible_version']:
        for job in data['jobs_by_ansible_version']:
            if job and 'job_template_name' in job and job['job_template_name']:
                job['job_template_name'] = hash(job['job_template_name'], salt)

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
      - statistics: object of primitive totals (includes credentials)
      - modules_used_per_playbook: array of {playbook_id, modules_used}
      - module_stats: array (copied as-is)
      - collection_name_stats: array (copied as-is)
      - jobs_by_job_type: array (grouped by job_type, merged with job_host_summary data)
      - jobs_by_launch_type: array (grouped by launch_type, merged with job_host_summary data)
      - jobs_by_ansible_version: array (grouped by ansible_version, merged with job_host_summary data)
      - collections_versions: array of {name, version, job_count} from installed collections
    """
    events_modules = data.get('events_modules', {})
    execution_environments = data.get('execution_environments', {})
    jobs = data.get('jobs', {})
    job_host_summary_root = data.get('job_host_summary', {})
    credentials_root = data.get('credentials', {})

    # credentials_root is now a list of unique credential types (from the 'json' field)
    credentials_list: List[str] = credentials_root if isinstance(credentials_root, list) else []

    # 1) statistics (collect only primitive totals)
    # Get jobs_total directly from jobs data, or calculate by summing jobs_total from all job_type groups as fallback
    jobs_by_job_type: List[Dict[str, Any]] = jobs.get('by_job_type', []) or []
    jobs_total = jobs.get('jobs_total')  # Use direct value from jobs data

    # Calculate unique_hosts_total by summing unique_hosts_total from all job_type groups
    job_host_summary_by_job_type: List[Dict[str, Any]] = job_host_summary_root.get('by_job_type', []) or []
    job_host_summary_by_launch_type: List[Dict[str, Any]] = job_host_summary_root.get('by_launch_type', []) or []
    job_host_summary_by_ansible_version: List[Dict[str, Any]] = job_host_summary_root.get('by_ansible_version', []) or []
    unique_hosts_total = sum(jhs.get('unique_hosts_total', 0) for jhs in job_host_summary_by_job_type) if job_host_summary_by_job_type else None
    job_host_pairs_total = job_host_summary_root.get('job_host_pairs_total')

    # Calculate playbooks_total from modules_used_per_playbook_total dict
    modules_used_per_playbook_total: Dict[str, int] = events_modules.get('modules_used_per_playbook_total', {}) or {}
    playbooks_total = len(modules_used_per_playbook_total)

    # Calculate job_templates_total by summing templates_total from all job_type groups
    job_templates_total = sum(job.get('templates_total', 0) for job in jobs_by_job_type) if jobs_by_job_type else None

    # Merge ansible_versions from all job_type groups (unique values, sorted)
    ansible_versions_set = set()
    for job in jobs_by_job_type:
        ansible_versions = job.get('ansible_versions', [])
        if isinstance(ansible_versions, list):
            ansible_versions_set.update(ansible_versions)
    ansible_versions_merged = sorted(list(ansible_versions_set)) if ansible_versions_set else []

    # Extract SCM types from jobs_by_job_type
    # Check jobs_using_scm_type_*_total fields across all job_type groups
    scm_types_set = set()
    scm_type_field_prefix = 'jobs_using_scm_type_'
    scm_type_field_suffix = '_total'
    # Known SCM types from jobs_anonymized_rollup.py
    known_scm_types = ['git', 'hg', 'svn', 'insights', 'archive', 'manual', 'unknown']
    
    for job in jobs_by_job_type:
        for scm_type in known_scm_types:
            field_name = f'{scm_type_field_prefix}{scm_type}{scm_type_field_suffix}'
            count = job.get(field_name, 0)
            if count and count > 0:
                scm_types_set.add(scm_type)
    scm_types_merged = sorted(list(scm_types_set)) if scm_types_set else []

    # Extract credential types from credentials_list (already sorted list from credentials rollup)
    credential_types_merged = credentials_list if isinstance(credentials_list, list) else []

    # Build statistics with rollup_period_ prefix for all fields
    statistics = {
        # from events_modules
        'rollup_period_modules_used_to_automate_total': events_modules.get('modules_used_to_automate_total'),
        'rollup_period_hosts_automated_total': events_modules.get('hosts_automated_total'),
        'rollup_period_event_total': events_modules.get('event_total'),
        'rollup_period_warnings_total': events_modules.get('warnings_total'),
        'rollup_period_deprecations_total': events_modules.get('deprecations_total'),
        'rollup_period_playbooks_total': playbooks_total,
        # from execution_environments
        'rollup_period_execution_environments_total': execution_environments.get('execution_environments_total'),
        'rollup_period_execution_environments_default_total': execution_environments.get('execution_environments_default_total'),
        'rollup_period_execution_environments_custom_total': execution_environments.get('execution_environments_custom_total'),
        # from jobs (top-level fields)
        'rollup_period_jobs_total': jobs_total,
        'rollup_period_organizations_total': jobs.get('organizations_total'),
        'rollup_period_ansible_version': jobs.get('ansible_version'),
        'rollup_period_ansible_versions': ansible_versions_merged,
        'rollup_period_forks_total': jobs.get('forks_total'),
        'rollup_period_job_templates_total': job_templates_total,
        # from job_host_summary (sum of all job_type groups)
        'rollup_period_unique_hosts_total': unique_hosts_total,
        'rollup_period_job_host_pairs_total': job_host_pairs_total,
        # computed arrays
        'rollup_period_scm_types': scm_types_merged,
        'rollup_period_credential_types': credential_types_merged,
    }

    # 2) modules_used_per_playbook (convert map -> array)
    modules_used_per_playbook: List[Dict[str, Any]] = [
        {'playbook_id': playbook_id, 'modules_used': modules_used} for playbook_id, modules_used in modules_used_per_playbook_total.items()
    ]

    # 3) arrays copied as-is from their respective parents
    module_stats: List[Dict[str, Any]] = events_modules.get('module_stats', []) or []
    collection_name_stats: List[Dict[str, Any]] = events_modules.get('collection_name_stats', []) or []

    # 4) Extract and transform installed collections from jobs data
    installed_collections: List[Dict[str, Any]] = jobs.get('installed_collections', []) or []
    collections_versions: List[Dict[str, Any]] = [
        {
            'name': item.get('collection_name', ''),
            'version': item.get('collection_version', ''),
            'job_count': item.get('job_count', 0),
        }
        for item in installed_collections
        if item and 'collection_name' in item and 'collection_version' in item
    ]

    # 5) Merge job_host_summary into jobs groupings (by_job_type, by_launch_type, by_ansible_version)
    # Create a lookup dict for job_host_summary by job_type
    jhs_lookup: Dict[str, Dict[str, Any]] = {jhs.get('job_type'): jhs for jhs in job_host_summary_by_job_type}

    # Default values for host summary fields when no match is found
    default_host_summary_fields = {
        'dark_total': 0,
        'failures_total': 0,
        'ok_total': 0,
        'skipped_total': 0,
        'ignored_total': 0,
        'rescued_total': 0,
        'unique_hosts_total': 0,
        'hosts_successful_total': 0,
        'hosts_failed_total': 0,
        'hosts_unreachable_total': 0,
    }

    # Merge job_host_summary data into jobs_by_job_type
    jobs_by_job_type_merged: List[Dict[str, Any]] = []
    for job in jobs_by_job_type:
        job_type = job.get('job_type')
        merged_job = job.copy()

        # Add host summary fields from matching job_host_summary entry, or use defaults
        if job_type in jhs_lookup:
            jhs_data = jhs_lookup[job_type]
            merged_job.update(
                {
                    'dark_total': jhs_data.get('dark_total', 0),
                    'failures_total': jhs_data.get('failures_total', 0),
                    'ok_total': jhs_data.get('ok_total', 0),
                    'skipped_total': jhs_data.get('skipped_total', 0),
                    'ignored_total': jhs_data.get('ignored_total', 0),
                    'rescued_total': jhs_data.get('rescued_total', 0),
                    'unique_hosts_total': jhs_data.get('unique_hosts_total', 0),
                    'hosts_successful_total': jhs_data.get('hosts_successful_total', 0),
                    'hosts_failed_total': jhs_data.get('hosts_failed_total', 0),
                    'hosts_unreachable_total': jhs_data.get('hosts_unreachable_total', 0),
                }
            )
        else:
            # No match found, use default values
            merged_job.update(default_host_summary_fields)

        jobs_by_job_type_merged.append(merged_job)

    # 5b) Merge job_host_summary into jobs_by_launch_type (grouped by launch_type)
    # Create a lookup dict for job_host_summary by launch_type
    jhs_launch_type_lookup: Dict[str, Dict[str, Any]] = {jhs.get('launch_type'): jhs for jhs in job_host_summary_by_launch_type}

    jobs_by_launch_type: List[Dict[str, Any]] = jobs.get('by_launch_type', []) or []
    jobs_by_launch_type_merged: List[Dict[str, Any]] = []
    for job in jobs_by_launch_type:
        launch_type = job.get('launch_type')
        merged_job = job.copy()

        # Add host summary fields from matching job_host_summary entry, or use defaults
        if launch_type in jhs_launch_type_lookup:
            jhs_data = jhs_launch_type_lookup[launch_type]
            merged_job.update(
                {
                    'dark_total': jhs_data.get('dark_total', 0),
                    'failures_total': jhs_data.get('failures_total', 0),
                    'ok_total': jhs_data.get('ok_total', 0),
                    'skipped_total': jhs_data.get('skipped_total', 0),
                    'ignored_total': jhs_data.get('ignored_total', 0),
                    'rescued_total': jhs_data.get('rescued_total', 0),
                    'unique_hosts_total': jhs_data.get('unique_hosts_total', 0),
                    'hosts_successful_total': jhs_data.get('hosts_successful_total', 0),
                    'hosts_failed_total': jhs_data.get('hosts_failed_total', 0),
                    'hosts_unreachable_total': jhs_data.get('hosts_unreachable_total', 0),
                }
            )
        else:
            # No match found, use default values
            merged_job.update(default_host_summary_fields)

        jobs_by_launch_type_merged.append(merged_job)

    # 5c) Merge job_host_summary into jobs_by_ansible_version (grouped by ansible_version)
    # Create a lookup dict for job_host_summary by ansible_version
    # Handle None/NaN values by converting to string for consistent lookup
    jhs_ansible_version_lookup: Dict[str, Dict[str, Any]] = {}
    for jhs in job_host_summary_by_ansible_version:
        ansible_version_key = jhs.get('ansible_version')
        # Convert None/NaN to string for consistent lookup
        if ansible_version_key is None or (isinstance(ansible_version_key, float) and pd.isna(ansible_version_key)):
            ansible_version_key = 'None'
        else:
            ansible_version_key = str(ansible_version_key)
        jhs_ansible_version_lookup[ansible_version_key] = jhs

    jobs_by_ansible_version: List[Dict[str, Any]] = jobs.get('by_ansible_version', []) or []
    jobs_by_ansible_version_merged: List[Dict[str, Any]] = []
    for job in jobs_by_ansible_version:
        ansible_version = job.get('ansible_version')
        merged_job = job.copy()

        # Convert None/NaN to string for consistent lookup
        if ansible_version is None or (isinstance(ansible_version, float) and pd.isna(ansible_version)):
            ansible_version_key = 'None'
        else:
            ansible_version_key = str(ansible_version)

        # Add host summary fields from matching job_host_summary entry, or use defaults
        if ansible_version_key in jhs_ansible_version_lookup:
            jhs_data = jhs_ansible_version_lookup[ansible_version_key]
            merged_job.update(
                {
                    'dark_total': jhs_data.get('dark_total', 0),
                    'failures_total': jhs_data.get('failures_total', 0),
                    'ok_total': jhs_data.get('ok_total', 0),
                    'skipped_total': jhs_data.get('skipped_total', 0),
                    'ignored_total': jhs_data.get('ignored_total', 0),
                    'rescued_total': jhs_data.get('rescued_total', 0),
                    'unique_hosts_total': jhs_data.get('unique_hosts_total', 0),
                    'hosts_successful_total': jhs_data.get('hosts_successful_total', 0),
                    'hosts_failed_total': jhs_data.get('hosts_failed_total', 0),
                    'hosts_unreachable_total': jhs_data.get('hosts_unreachable_total', 0),
                }
            )
        else:
            # No match found, use default values
            merged_job.update(default_host_summary_fields)

        jobs_by_ansible_version_merged.append(merged_job)

    # 6) assemble the flattened object
    flattened: Dict[str, Any] = {
        'statistics': statistics,
        'modules_used_per_playbook': modules_used_per_playbook,
        'module_stats': module_stats,
        'collection_name_stats': collection_name_stats,
        'jobs_by_job_type': jobs_by_job_type_merged,
        'jobs_by_launch_type': jobs_by_launch_type_merged,
        'jobs_by_ansible_version': jobs_by_ansible_version_merged,
        'collections_versions': collections_versions,
    }

    return flattened


def anonymize_rollups(events_modules_rollup, execution_environments_rollup, jobs_rollup, job_host_summary_rollup, credentials_rollup, salt):
    """
    Combines rollup data, flattens it, and anonymizes sensitive fields.

    Args:
        events_modules_rollup: Event modules statistics
        execution_environments_rollup: Execution environment statistics
        jobs_rollup: Jobs statistics
        job_host_summary_rollup: Job host summary statistics
        credentials_rollup: Credentials statistics
        salt: Salt string for hashing sensitive data

    Returns:
        Flattened and anonymized rollup data
    """
    data = {
        'events_modules': events_modules_rollup,
        'execution_environments': execution_environments_rollup,
        'jobs': jobs_rollup,
        'job_host_summary': job_host_summary_rollup,
        'credentials': credentials_rollup,
    }

    # First flatten the nested structure
    data = flatten_json_report(data)

    # Then anonymize the flattened structure
    anonymize_data(data, salt)

    return data


def compute_anonymized_rollup_from_raw_data(input_data, salt, since, until, base_path, save_rollups: bool = True, save_rollups_packed: bool = True):
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

    anonymized_rollup = anonymize_rollups(
        events_modules_result['json'],
        execution_environments_result['json'],
        jobs_result['json'],
        job_host_summary_result['json'],
        credentials_result['json'],
        salt,
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
