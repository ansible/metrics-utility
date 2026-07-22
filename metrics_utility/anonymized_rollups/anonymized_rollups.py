from typing import Any, Callable, Dict, List

import pandas as pd

from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup
from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import load_known_collections
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup
from metrics_utility.anonymized_rollups.task_executions_anonymized_rollup import TaskExecutionsAnonymizedRollup
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage


def _installed_collection_name_is_unknown(collection_name: Any, known: Dict[str, Any]) -> bool:
    """
    Return True if the name should be anonymized to 'Custom': missing, blank, NA,
    or not present in the known collections map.

    Using ``if collection_name:`` is unsafe for pandas ``pd.NA`` (TypeError in boolean
    context); we normalize with ``pd.isna`` first.
    """
    if collection_name is None:
        return True
    if isinstance(collection_name, float) and pd.isna(collection_name):
        return True
    try:
        if pd.isna(collection_name):
            return True
    except (TypeError, ValueError):
        pass
    s = str(collection_name).strip()
    if not s:
        return True
    return s not in known


def create_anonymized_object(rollup_name: str):
    """Instantiate and return the rollup object for the given collector name.

    Args:
        rollup_name: One of ``'jobs'``, ``'job_host_summary'``, ``'events_modules'``,
            ``'execution_environments'``, ``'credentials'``, ``'table_metadata'``,
            ``'controller_version'``, ``'feature_flags'``, or ``'task_executions'``.

    Returns:
        An instance of the corresponding ``*AnonymizedRollup`` class.

    Raises:
        ValueError: If *rollup_name* is not recognised.
    """
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
    elif rollup_name == 'feature_flags':
        return FeatureFlagsAnonymizedRollup()
    elif rollup_name == 'task_executions':
        return TaskExecutionsAnonymizedRollup()
    else:
        raise ValueError(f'Invalid rollup name: {rollup_name}')


def _remove_custom_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return a new list with all items whose collection_source is 'Custom' removed."""
    return [item for item in items if item and item.get('collection_source') != 'Custom']


def _remove_unknown_installed_collections(items: List[Dict[str, Any]], known_collections: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a new list with installed-collection entries not in the known whitelist removed."""
    return [item for item in items if item and not _installed_collection_name_is_unknown(item.get('collection', ''), known_collections)]


def anonymize_data(data):
    """
    Anonymizes sensitive data in the flattened report structure.
    This function expects data to be already flattened by flatten_json_report().

    For items with collection_source == 'Custom', removes their entries from
    module_stats, collection_stats, and role_stats entirely so private names
    never appear in the outbound payload.

    Args:
        data: Flattened data structure with keys:
            - jobs_by_job_type: array of job stats (grouped by job_type, merged with job_host_summary and credentials data)
            - jobs_by_launch_type: array of job stats (grouped by launch_type, with default host summary fields)
            - jobs_by_ansible_version: array of job stats (grouped by ansible_version, with default host summary fields)
            - module_stats: array of module statistics
            - collection_stats: array of collection statistics
            - role_stats: array of role statistics
            - jobs_by_installed_collections_versions: array of {collection, version, jobs_total, jobs_failed_total,
              jobs_successful_total} from installed collections
    """
    if not data or not isinstance(data, dict):
        return

    for key in ('module_stats', 'collection_stats', 'role_stats'):
        if key in data:
            data[key] = _remove_custom_items(data[key] or [])

    known_collections = load_known_collections()

    if 'jobs_by_installed_collections_versions' in data:
        data['jobs_by_installed_collections_versions'] = _remove_unknown_installed_collections(
            data['jobs_by_installed_collections_versions'] or [],
            known_collections,
        )
    if 'indirect_nodes_by_collection' in data:
        data['indirect_nodes_by_collection'] = [
            item
            for item in (data['indirect_nodes_by_collection'] or [])
            if item and not _installed_collection_name_is_unknown(item.get('collection', ''), known_collections)
        ]
    if 'indirect_nodes_by_module' in data:
        data['indirect_nodes_by_module'] = [
            item
            for item in (data['indirect_nodes_by_module'] or [])
            if item
            and not _installed_collection_name_is_unknown(
                DataframeContentUsage.extract_collection_name(item.get('module', '')),
                known_collections,
            )
        ]


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
    indirect_managed_nodes: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Build statistics dictionary with rollup_period_ prefix for all fields."""
    # Calculate indirect node count
    indirect_nodes_total = 0
    if indirect_managed_nodes:
        indirect_nodes_total = indirect_managed_nodes.get('indirect_nodes_total', 0)

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
        # from indirect_managed_nodes
        'rollup_period_indirect_managed_nodes_all_total': indirect_nodes_total,
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


def _inject_controller_version_to_all_items(jobs_list: List[Dict[str, Any]], controller_versions: List[str]) -> List[Dict[str, Any]]:
    """Inject the first controller_version from the controller_versions list into every
    item of the given jobs grouping list."""
    first_version = controller_versions[0] if controller_versions else None
    for item in jobs_list:
        item['controller_version'] = first_version
    return jobs_list


def _extract_jobs_by_installed_collections_versions(jobs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract and transform installed collections from jobs data."""
    installed_collections: List[Dict[str, Any]] = jobs.get('installed_collections', []) or []
    return [
        {
            'collection': item.get('collection_name', ''),
            'version': item.get('collection_version', ''),
            'jobs_total': item.get('job_count', 0),
            'jobs_failed_total': item.get('jobs_failed_total', 0),
            'jobs_successful_total': item.get('jobs_successful_total', 0),
            'jobs_never_started_total': item.get('jobs_never_started_total', 0),
            'jobs_duration_total_seconds': item.get('jobs_duration_total_seconds', 0),
            'jobs_successful_duration_total_seconds': item.get('jobs_successful_duration_total_seconds', 0),
            'jobs_failed_duration_total_seconds': item.get('jobs_failed_duration_total_seconds', 0),
            'job_duration_maximum_seconds': item.get('job_duration_maximum_seconds'),
            'job_duration_minimum_seconds': item.get('job_duration_minimum_seconds'),
            'job_waiting_time_total_seconds': item.get('job_waiting_time_total_seconds', 0),
            'job_waiting_time_maximum_seconds': item.get('job_waiting_time_maximum_seconds'),
            'job_waiting_time_minimum_seconds': item.get('job_waiting_time_minimum_seconds'),
            'templates_total': item.get('templates_total', 0),
            'inventories_total': item.get('inventories_total', 0),
            'ansible_versions': item.get('ansible_versions', []),
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


def _as_list(value: Any) -> List[Any]:
    """Return *value* unchanged when it is already a list; otherwise return an empty list."""
    return value if isinstance(value, list) else []


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
      - jobs_by_installed_collections_versions: array of {collection, version, jobs_total, jobs_failed_total,
        jobs_successful_total} from installed collections
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
    feature_flags_root = data.get('feature_flags', [])
    task_executions_root = data.get('task_executions', [])
    indirect_managed_nodes_root = data.get('indirect_managed_nodes', {})

    # Extract data structures
    credentials_list: List[str] = _as_list(credentials_root)
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
        indirect_managed_nodes_root,
    )

    # Extract arrays and collections
    # Only include event-related arrays if there are events
    module_stats: List[Dict[str, Any]] = events_modules.get('module_stats', []) or []
    collection_stats: List[Dict[str, Any]] = events_modules.get('collection_stats', []) or []
    role_stats: List[Dict[str, Any]] = events_modules.get('role_stats', []) or []
    jobs_by_installed_collections_versions = _extract_jobs_by_installed_collections_versions(jobs)

    # Merge job_host_summary into jobs groupings
    jobs_by_job_type_merged, jobs_by_launch_type_merged, jobs_by_ansible_version_merged = _merge_all_jobs_groupings(
        jobs,
        job_host_summary_by_job_type,
        job_host_summary_by_launch_type,
        job_host_summary_by_ansible_version,
    )

    # Build jobs_by_controller_version: inject first controller_version from the controller_version collector
    jobs_by_controller_version: List[Dict[str, Any]] = jobs.get('jobs_by_controller_version', []) or []
    controller_versions: List[str] = _as_list(controller_version_root)
    jobs_by_controller_version = _inject_controller_version(jobs_by_controller_version, controller_versions)

    # Inject controller_version into every item of the three job groupings
    jobs_by_job_type_merged = _inject_controller_version_to_all_items(jobs_by_job_type_merged, controller_versions)
    jobs_by_launch_type_merged = _inject_controller_version_to_all_items(jobs_by_launch_type_merged, controller_versions)
    jobs_by_ansible_version_merged = _inject_controller_version_to_all_items(jobs_by_ansible_version_merged, controller_versions)

    # Calculate task statistics and update statistics dictionary
    task_statistics = _calculate_task_statistics(jobs_by_job_type_merged)
    statistics.update(task_statistics)

    # Assemble the flattened object
    flattened: Dict[str, Any] = {
        'statistics': statistics,
        'rollup_period_ansible_versions': ansible_versions_merged,
        'rollup_period_scm_types': _as_list(jobs.get('scm_types')),
        'rollup_period_credential_types': credentials_list,
        'jobs_by_job_type': jobs_by_job_type_merged,
        'jobs_by_launch_type': jobs_by_launch_type_merged,
        'jobs_by_ansible_version': jobs_by_ansible_version_merged,
        'jobs_by_controller_version': jobs_by_controller_version,
        'jobs_by_installed_collections_versions': jobs_by_installed_collections_versions,
        'table_metadata': table_metadata_root,
        'controller_versions': controller_versions,
        'feature_flags': _as_list(feature_flags_root),
        'observability_by_tasks': _as_list(task_executions_root),
        'indirect_nodes_by_collection': indirect_managed_nodes_root.get('by_collection', []),
        'indirect_nodes_by_module': indirect_managed_nodes_root.get('by_module', []),
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
    *,
    feature_flags_rollup=None,
    task_executions_rollup=None,
    indirect_managed_nodes_rollup=None,
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
        feature_flags_rollup: Enabled feature flags list (optional, keyword-only)
        task_executions_rollup: Task execution observability statistics (optional, keyword-only)
        indirect_managed_nodes_rollup: Indirect managed nodes statistics (optional, keyword-only)

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
        'feature_flags': feature_flags_rollup or [],
        'task_executions': task_executions_rollup or [],
        'indirect_managed_nodes': indirect_managed_nodes_rollup or {},
    }

    # First flatten the nested structure
    data = flatten_json_report(data)

    # Then anonymize the flattened structure
    anonymize_data(data)

    return data
