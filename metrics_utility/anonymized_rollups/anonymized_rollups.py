import hashlib
import json
import os
import shutil

from typing import Any, Callable, Dict, List

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup
from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup


OUT_BATCHES_DIR = './out/batches'


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
    elif rollup_name == 'table_metadata':
        return TableMetadataAnonymizedRollup()
    elif rollup_name == 'controller_version':
        return ControllerVersionAnonymizedRollup()
    else:
        raise ValueError(f'Invalid rollup name: {rollup_name}')


def anonymize_data(data, salt):
    """
    Anonymizes sensitive data in the flattened report structure.
    This function expects data to be already flattened by flatten_json_report().

    For items with collection_source == 'Custom', replaces module names, collection names,
    and role names with the string "Custom" instead of hashing them.

    Args:
        data: Flattened data structure with keys:
            - jobs_by_job_type: array of job stats (grouped by job_type, merged with job_host_summary and credentials data)
            - jobs_by_launch_type: array of job stats (grouped by launch_type, with default host summary fields)
            - jobs_by_ansible_version: array of job stats (grouped by ansible_version, with default host summary fields)
            - module_stats: array of module statistics
            - collection_stats: array of collection statistics
            - role_stats: array of role statistics
            - collections_versions: array of {name, version, jobs_total, jobs_failed_total, jobs_successful_total} from installed collections
        salt: Salt string for hashing (used for job_template_name hashing)
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

    # anonymize module_stats - replace module name and collection name with 'Custom' for 'Custom' sources
    if 'module_stats' in data and data['module_stats']:
        for module in data['module_stats']:
            if module and module.get('collection_source') == 'Custom':
                if 'module_name' in module and module['module_name']:
                    module['module_name'] = 'Custom'
                if 'collection_name' in module and module['collection_name']:
                    module['collection_name'] = 'Custom'

    # anonymize collection_stats - replace collection name with 'Custom' for 'Custom' sources
    if 'collection_stats' in data and data['collection_stats']:
        for collection in data['collection_stats']:
            if collection and collection.get('collection_source') == 'Custom':
                if 'collection_name' in collection and collection['collection_name']:
                    collection['collection_name'] = 'Custom'

    # anonymize role_stats - replace role name and collection name with 'Custom' for 'Custom' sources
    if 'role_stats' in data and data['role_stats']:
        for role in data['role_stats']:
            if role and role.get('collection_source') == 'Custom':
                if 'role' in role and role['role']:
                    role['role'] = 'Custom'
                if 'collection_name' in role and role['collection_name']:
                    role['collection_name'] = 'Custom'

    # anonymize collections_versions - replace collection name and version with 'Unknown' for unknown collections
    # Load collections.json to check if collection is known
    collections_path = os.path.join(os.path.dirname(__file__), 'collections.json')
    try:
        with open(collections_path, 'r') as f:
            collections = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        collections = {}

    if 'collections_versions' in data and data['collections_versions']:
        for collection_version in data['collections_versions']:
            if collection_version and 'name' in collection_version:
                collection_name = collection_version.get('name', '')
                # If collection is not in the known collections mapping, replace name and version with 'Custom'
                if collection_name and collection_name not in collections:
                    collection_version['name'] = 'Custom'
                    collection_version['version'] = 'Custom'

    # Note: modules_used_per_playbook anonymization removed since it's not in final output
    # If needed in future, can be re-enabled when modules_used_per_playbook is added back to output


def _normalize_ansible_version_key(ansible_version: Any) -> str:
    """Normalize ansible version key for consistent lookup, handling None/NaN values."""
    if ansible_version is None or (isinstance(ansible_version, float) and pd.isna(ansible_version)):
        return 'None'
    return str(ansible_version)


def _get_default_host_summary_fields() -> Dict[str, int]:
    """Get default values for host summary fields when no match is found."""
    return {
        'unreachable_total': 0,
        'failed_total': 0,
        'ok_total': 0,
        'skipped_total': 0,
        'ignored_total': 0,
        'rescued_total': 0,
        'successful_hosts_total': 0,
        'failed_hosts_total': 0,
        'unreachable_hosts_total': 0,
    }


def _extract_host_summary_fields(jhs_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract host summary fields from job_host_summary data.

    Note: unique_hosts_total is not included here as it's only computed at the top level,
    not per grouping.
    """
    return {
        'unreachable_total': jhs_data.get('unreachable_total', 0),
        'failed_total': jhs_data.get('failed_total', 0),
        'ok_total': jhs_data.get('ok_total', 0),
        'skipped_total': jhs_data.get('skipped_total', 0),
        'ignored_total': jhs_data.get('ignored_total', 0),
        'rescued_total': jhs_data.get('rescued_total', 0),
        'successful_hosts_total': jhs_data.get('successful_hosts_total', 0),
        'failed_hosts_total': jhs_data.get('failed_hosts_total', 0),
        'unreachable_hosts_total': jhs_data.get('unreachable_hosts_total', 0),
    }


def _merge_jobs_with_host_summary(
    jobs_list: List[Dict[str, Any]],
    jhs_lookup: Dict[str, Dict[str, Any]],
    key_extractor: Callable[[Dict[str, Any]], Any],
    normalize_key: Callable[[Any], str] = None,
) -> List[Dict[str, Any]]:
    """Merge job_host_summary data into jobs list using a lookup dictionary."""
    default_fields = _get_default_host_summary_fields()
    merged_jobs = []

    for job in jobs_list:
        merged_job = job.copy()
        lookup_key = key_extractor(job)

        if normalize_key:
            lookup_key = normalize_key(lookup_key)

        if lookup_key in jhs_lookup:
            merged_job.update(_extract_host_summary_fields(jhs_lookup[lookup_key]))
        else:
            merged_job.update(default_fields)

        merged_jobs.append(merged_job)

    return merged_jobs


def _calculate_sum_from_list(items: List[Dict[str, Any]], field: str) -> Any:
    """Calculate sum of a field from a list of dictionaries, returning 0 if list is empty."""
    if not items:
        return 0
    return sum(item.get(field, 0) for item in items)


def _calculate_host_summary_totals(job_host_summary_by_job_type: List[Dict[str, Any]], host_ids: List[Any] = None) -> Dict[str, Any]:
    """Calculate host summary totals from job_type groups.

    Args:
        job_host_summary_by_job_type: List of job_type group dictionaries
        host_ids: Top-level list of host IDs to compute unique_hosts_total from
    """
    # Compute unique_hosts_total from top-level host_ids list, not from groupings
    if host_ids is not None and isinstance(host_ids, list) and len(host_ids) > 0:
        unique_hosts_total = len(set(host_ids))
    else:
        unique_hosts_total = 0

    return {
        'unique_hosts_total': unique_hosts_total,
        'successful_hosts_total': _calculate_sum_from_list(job_host_summary_by_job_type, 'successful_hosts_total'),
        'failed_hosts_total': _calculate_sum_from_list(job_host_summary_by_job_type, 'failed_hosts_total'),
        'unreachable_hosts_total': _calculate_sum_from_list(job_host_summary_by_job_type, 'unreachable_hosts_total'),
    }


def _calculate_job_statistics(jobs_by_job_type: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate job statistics by summing from all job_type groups."""
    return {
        'rollup_period_jobs_total': _calculate_sum_from_list(jobs_by_job_type, 'jobs_total'),
        'job_templates_total': _calculate_sum_from_list(jobs_by_job_type, 'templates_total'),
        'inventories_total': _calculate_sum_from_list(jobs_by_job_type, 'inventories_total'),
        'rollup_period_jobs_successful': _calculate_sum_from_list(jobs_by_job_type, 'jobs_successful_total'),
        'rollup_period_jobs_failed': _calculate_sum_from_list(jobs_by_job_type, 'jobs_failed_total'),
        'rollup_period_jobs_duration_all_statuses_seconds': _calculate_sum_from_list(jobs_by_job_type, 'jobs_duration_total_seconds'),
        'rollup_period_jobs_successful_duration_total_seconds': _calculate_sum_from_list(jobs_by_job_type, 'jobs_successful_duration_total_seconds'),
        'rollup_period_jobs_failed_duration_total_seconds': _calculate_sum_from_list(jobs_by_job_type, 'jobs_failed_duration_total_seconds'),
    }


def _merge_ansible_versions(jobs_by_job_type: List[Dict[str, Any]]) -> List[str]:
    """Merge ansible_versions from all job_type groups (unique values, sorted)."""
    ansible_versions_set = set()
    for job in jobs_by_job_type:
        ansible_versions = job.get('ansible_versions', [])
        if isinstance(ansible_versions, list):
            ansible_versions_set.update(ansible_versions)
    return sorted(list(ansible_versions_set)) if ansible_versions_set else []


def _calculate_execution_environments_total(execution_environments: Dict[str, Any]) -> Any:
    """Calculate execution_environments_total as sum of default and custom."""
    default_total = execution_environments.get('execution_environments_default_total')
    custom_total = execution_environments.get('execution_environments_custom_total')
    if default_total is None and custom_total is None:
        return None
    return (default_total or 0) + (custom_total or 0)


def _calculate_task_statistics(jobs_by_job_type_merged: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate task statistics from merged jobs_by_job_type."""
    task_ok = sum(job.get('ok_total', 0) for job in jobs_by_job_type_merged)
    task_failed = sum(job.get('failed_total', 0) for job in jobs_by_job_type_merged)
    task_skipped = sum(job.get('skipped_total', 0) for job in jobs_by_job_type_merged)
    task_unreachable = sum(job.get('unreachable_total', 0) for job in jobs_by_job_type_merged)
    task_ignored = sum(job.get('ignored_total', 0) for job in jobs_by_job_type_merged)

    return {
        'rollup_period_tasks_total': task_ok + task_failed + task_skipped + task_unreachable + task_ignored,
        'rollup_period_task_ok_total': task_ok,
        'rollup_period_task_failed_total': task_failed,
        'rollup_period_task_skipped_total': task_skipped,
        'rollup_period_task_unreachable_total': task_unreachable,
        'rollup_period_task_ignored_total': task_ignored,
    }


def _build_statistics(
    events_modules: Dict[str, Any],
    execution_environments: Dict[str, Any],
    jobs: Dict[str, Any],
    job_statistics: Dict[str, Any],
    host_summary_totals: Dict[str, Any],
    job_host_pairs_total: Any,
    playbooks_total: int,
    execution_environments_total: Any,
    has_events: bool = True,
) -> Dict[str, Any]:
    """Build statistics dictionary with rollup_period_ prefix for all fields."""
    statistics = {
        # from execution_environments
        'rollup_period_execution_environments_total': execution_environments_total,
        'rollup_period_EE_default_total': execution_environments.get('execution_environments_default_total'),
        'rollup_period_EE_custom_total': execution_environments.get('execution_environments_custom_total'),
        # from jobs (computed from jobs_by_job_type aggregation)
        'rollup_period_jobs_total': job_statistics['rollup_period_jobs_total'],
        'rollup_period_jobs_successful': job_statistics['rollup_period_jobs_successful'],
        'rollup_period_jobs_failed': job_statistics['rollup_period_jobs_failed'],
        'rollup_period_jobs_duration_all_statuses_seconds': job_statistics['rollup_period_jobs_duration_all_statuses_seconds'],
        'rollup_period_jobs_successful_duration_total_seconds': job_statistics['rollup_period_jobs_successful_duration_total_seconds'],
        'rollup_period_jobs_failed_duration_total_seconds': job_statistics['rollup_period_jobs_failed_duration_total_seconds'],
        'rollup_period_organizations_total': jobs.get('organizations_total') or 0,
        'rollup_period_forks_total': jobs.get('forks_total') or 0,
        'rollup_period_templates_total': job_statistics['job_templates_total'],
        'rollup_period_inventories_total': job_statistics['inventories_total'],
        # from job_host_summary (sum of all job_type groups)
        'rollup_period_unique_hosts_total': host_summary_totals['unique_hosts_total'],
        'rollup_period_job_host_pairs_total': job_host_pairs_total,
        'rollup_period_successful_hosts_total': host_summary_totals['successful_hosts_total'],
        'rollup_period_failed_hosts_total': host_summary_totals['failed_hosts_total'],
        'rollup_period_unreachable_hosts_total': host_summary_totals['unreachable_hosts_total'],
    }

    # Only include event-related fields if there are events
    if has_events:
        statistics.update(
            {
                # from events_modules
                'rollup_period_modules_total': events_modules.get('modules_used_to_automate_total'),
                'rollup_period_unique_hosts_automated_total': events_modules.get('hosts_automated_total'),
                'rollup_period_collected_events_total': events_modules.get('collected_events_total'),
                'rollup_period_warnings_total': events_modules.get('warnings_total'),
                'rollup_period_deprecations_total': events_modules.get('deprecations_total'),
                'rollup_period_playbooks_total': playbooks_total,
            }
        )

    return statistics


def _inject_controller_version(jobs_by_controller_version: List[Dict[str, Any]], controller_versions: List[str]) -> List[Dict[str, Any]]:
    """Inject the first controller_version from the controller_versions list into the
    single-item jobs_by_controller_version summary."""
    if not jobs_by_controller_version:
        return jobs_by_controller_version

    first_version = controller_versions[0] if controller_versions else None
    jobs_by_controller_version[0]['controller_version'] = first_version
    return jobs_by_controller_version


def _extract_collections_versions(jobs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract and transform installed collections from jobs data."""
    installed_collections: List[Dict[str, Any]] = jobs.get('installed_collections', []) or []
    return [
        {
            'name': item.get('collection_name', ''),
            'version': item.get('collection_version', ''),
            'jobs_total': item.get('job_count', 0),
            'jobs_failed_total': item.get('jobs_failed_total', 0),
            'jobs_successful_total': item.get('jobs_successful_total', 0),
        }
        for item in installed_collections
        if item and 'collection_name' in item and 'collection_version' in item
    ]


def _merge_all_jobs_groupings(
    jobs: Dict[str, Any],
    job_host_summary_by_job_type: List[Dict[str, Any]],
    job_host_summary_by_launch_type: List[Dict[str, Any]],
    job_host_summary_by_ansible_version: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Merge job_host_summary data into all jobs groupings."""
    # Merge by job_type
    jhs_lookup_by_job_type: Dict[str, Dict[str, Any]] = {jhs.get('job_type'): jhs for jhs in job_host_summary_by_job_type}
    jobs_by_job_type: List[Dict[str, Any]] = jobs.get('by_job_type', []) or []
    jobs_by_job_type_merged = _merge_jobs_with_host_summary(
        jobs_by_job_type,
        jhs_lookup_by_job_type,
        lambda job: job.get('job_type'),
    )

    # Merge by launch_type
    jhs_lookup_by_launch_type: Dict[str, Dict[str, Any]] = {jhs.get('launch_type'): jhs for jhs in job_host_summary_by_launch_type}
    jobs_by_launch_type: List[Dict[str, Any]] = jobs.get('by_launch_type', []) or []
    jobs_by_launch_type_merged = _merge_jobs_with_host_summary(
        jobs_by_launch_type,
        jhs_lookup_by_launch_type,
        lambda job: job.get('launch_type'),
    )

    # Merge by ansible_version
    jhs_lookup_by_ansible_version: Dict[str, Dict[str, Any]] = {}
    for jhs in job_host_summary_by_ansible_version:
        key = _normalize_ansible_version_key(jhs.get('ansible_version'))
        jhs_lookup_by_ansible_version[key] = jhs

    jobs_by_ansible_version: List[Dict[str, Any]] = jobs.get('by_ansible_version', []) or []
    jobs_by_ansible_version_merged = _merge_jobs_with_host_summary(
        jobs_by_ansible_version,
        jhs_lookup_by_ansible_version,
        lambda job: job.get('ansible_version'),
        normalize_key=_normalize_ansible_version_key,
    )

    return jobs_by_job_type_merged, jobs_by_launch_type_merged, jobs_by_ansible_version_merged


def flatten_json_report(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Manually flattens the given nested report into:
      - statistics: object of primitive totals (includes credentials)
      - module_stats: array (copied as-is)
      - collection_stats: array (copied as-is)
      - role_stats: array (copied as-is)
      - jobs_by_job_type: array (grouped by job_type, merged with job_host_summary data)
      - jobs_by_launch_type: array (grouped by launch_type, merged with job_host_summary data)
      - jobs_by_ansible_version: array (grouped by ansible_version, merged with job_host_summary data)
      - collections_versions: array of {name, version, jobs_total, jobs_failed_total, jobs_successful_total} from installed collections
      - table_metadata: object with table metadata statistics
      - controller_versions: array of controller versions

    Note: modules_used_per_playbook is computed but not included in final output.
    """
    events_modules = data.get('events_modules', {})
    execution_environments = data.get('execution_environments', {})
    jobs = data.get('jobs', {})
    job_host_summary_root = data.get('job_host_summary', {})
    credentials_root = data.get('credentials', {})
    table_metadata_root = data.get('table_metadata', {})
    controller_version_root = data.get('controller_version', [])

    # Extract data structures
    credentials_list: List[str] = credentials_root if isinstance(credentials_root, list) else []
    jobs_by_job_type: List[Dict[str, Any]] = jobs.get('by_job_type', []) or []
    job_host_summary_by_job_type: List[Dict[str, Any]] = job_host_summary_root.get('by_job_type', []) or []
    job_host_summary_by_launch_type: List[Dict[str, Any]] = job_host_summary_root.get('by_launch_type', []) or []
    job_host_summary_by_ansible_version: List[Dict[str, Any]] = job_host_summary_root.get('by_ansible_version', []) or []

    # Extract top-level host_ids list to compute unique_hosts_total
    host_ids: List[Any] = job_host_summary_root.get('host_ids', []) or []

    # Calculate statistics using helper functions
    host_summary_totals = _calculate_host_summary_totals(job_host_summary_by_job_type, host_ids)
    job_statistics = _calculate_job_statistics(jobs_by_job_type)
    playbooks_total = len(events_modules.get('modules_used_per_playbook_total', {}) or {})
    execution_environments_total = _calculate_execution_environments_total(execution_environments)
    ansible_versions_merged = _merge_ansible_versions(jobs_by_job_type)

    # Check if there are any events
    collected_events_total = events_modules.get('collected_events_total', 0) or 0
    has_events = collected_events_total > 0

    # Build statistics dictionary
    statistics = _build_statistics(
        events_modules,
        execution_environments,
        jobs,
        job_statistics,
        host_summary_totals,
        job_host_summary_root.get('job_host_pairs_total'),
        playbooks_total,
        execution_environments_total,
        has_events,
    )

    # Extract arrays and collections
    # Only include event-related arrays if there are events
    module_stats: List[Dict[str, Any]] = events_modules.get('module_stats', []) or []
    collection_stats: List[Dict[str, Any]] = events_modules.get('collection_stats', []) or []
    role_stats: List[Dict[str, Any]] = events_modules.get('role_stats', []) or []
    collections_versions = _extract_collections_versions(jobs)

    # Merge job_host_summary into jobs groupings
    jobs_by_job_type_merged, jobs_by_launch_type_merged, jobs_by_ansible_version_merged = _merge_all_jobs_groupings(
        jobs,
        job_host_summary_by_job_type,
        job_host_summary_by_launch_type,
        job_host_summary_by_ansible_version,
    )

    # Build jobs_by_controller_version: inject first controller_version from the controller_version collector
    jobs_by_controller_version: List[Dict[str, Any]] = jobs.get('jobs_by_controller_version', []) or []
    controller_versions: List[str] = controller_version_root if isinstance(controller_version_root, list) else []
    jobs_by_controller_version = _inject_controller_version(jobs_by_controller_version, controller_versions)

    # Calculate task statistics and update statistics dictionary
    task_statistics = _calculate_task_statistics(jobs_by_job_type_merged)
    statistics.update(task_statistics)

    # Assemble the flattened object
    flattened: Dict[str, Any] = {
        'statistics': statistics,
        'rollup_period_ansible_versions': ansible_versions_merged,
        'rollup_period_scm_types': jobs.get('scm_types', []) if isinstance(jobs.get('scm_types'), list) else [],
        'rollup_period_credential_types': credentials_list,
        'jobs_by_job_type': jobs_by_job_type_merged,
        'jobs_by_launch_type': jobs_by_launch_type_merged,
        'jobs_by_ansible_version': jobs_by_ansible_version_merged,
        'jobs_by_controller_version': jobs_by_controller_version,
        'collections_versions': collections_versions,
        'table_metadata': table_metadata_root,
        'controller_versions': controller_versions,
    }

    # Only include event-related arrays if there are events
    if has_events:
        flattened['module_stats'] = module_stats
        flattened['collection_stats'] = collection_stats
        flattened['role_stats'] = role_stats

    return flattened


def anonymize_rollups(
    events_modules_rollup,
    execution_environments_rollup,
    jobs_rollup,
    job_host_summary_rollup,
    credentials_rollup,
    table_metadata_rollup,
    controller_version_rollup,
    salt,
):
    """
    Combines rollup data, flattens it, and anonymizes sensitive fields.

    Args:
        events_modules_rollup: Event modules statistics
        execution_environments_rollup: Execution environment statistics
        jobs_rollup: Jobs statistics
        job_host_summary_rollup: Job host summary statistics
        credentials_rollup: Credentials statistics
        table_metadata_rollup: Table metadata statistics
        controller_version_rollup: Controller version statistics
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
        'table_metadata': table_metadata_rollup,
        'controller_version': controller_version_rollup,
    }

    # First flatten the nested structure
    data = flatten_json_report(data)

    # Then anonymize the flattened structure
    anonymize_data(data, salt)

    return data


def compute_anonymized_rollup_from_raw_data(input_data, salt):

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

    anonymized_rollup = anonymize_rollups(
        events_modules_result['json'],
        execution_environments_result['json'],
        jobs_result['json'],
        job_host_summary_result['json'],
        credentials_result['json'],
        table_metadata_result['json'],
        controller_version_result['json'],
        salt,
    )
    # Sanitize the result to replace NaN and infinity values with None (valid JSON)
    anonymized_rollup = sanitize_json(anonymized_rollup)

    # save anonymized rollup to file
    os.makedirs(OUT_BATCHES_DIR, exist_ok=True)
    file_name = os.path.join(OUT_BATCHES_DIR, 'anonymized_rollup.json')
    with open(file_name, 'w') as f:
        f.write(json.dumps(anonymized_rollup, indent=2))
    return anonymized_rollup


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
