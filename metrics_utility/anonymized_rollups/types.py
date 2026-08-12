"""TypedDict definitions for the anonymized daily rollup payload sent to Segment."""

from typing import NotRequired, TypedDict


class Statistics(TypedDict):
    rollup_period_execution_environments_total: int | None
    rollup_period_EE_default_total: int | None
    rollup_period_EE_custom_total: int | None
    rollup_period_jobs_total: int
    rollup_period_jobs_successful: int
    rollup_period_jobs_failed: int
    rollup_period_jobs_duration_all_statuses_seconds: int | float
    rollup_period_jobs_successful_duration_total_seconds: int | float
    rollup_period_jobs_failed_duration_total_seconds: int | float
    rollup_period_organizations_total: int
    rollup_period_forks_total: int
    rollup_period_templates_total: int
    rollup_period_inventories_total: int
    rollup_period_unique_hosts_total: int
    rollup_period_job_host_pairs_total: int | None
    rollup_period_successful_hosts_total: int
    rollup_period_failed_hosts_total: int
    rollup_period_unreachable_hosts_total: int
    rollup_period_indirect_managed_nodes_all_total: int
    rollup_period_tasks_total: int
    rollup_period_task_ok_total: int
    rollup_period_task_failed_total: int
    rollup_period_task_skipped_total: int
    rollup_period_task_unreachable_total: int
    rollup_period_task_ignored_total: int
    # Present only when collected_events_total > 0
    rollup_period_modules_total: NotRequired[int | None]
    rollup_period_collected_events_total: NotRequired[int]
    rollup_period_warnings_total: NotRequired[int]
    rollup_period_deprecations_total: NotRequired[int]
    rollup_period_playbooks_total: NotRequired[int]


class JobStatsFields(TypedDict):
    jobs_total: int
    jobs_failed_total: int
    jobs_successful_total: int
    jobs_never_started_total: int
    jobs_duration_total_seconds: int | float
    jobs_successful_duration_total_seconds: int | float
    jobs_failed_duration_total_seconds: int | float
    job_duration_maximum_seconds: int | float | None
    job_duration_minimum_seconds: int | float | None
    job_waiting_time_total_seconds: int | float
    job_waiting_time_maximum_seconds: int | float | None
    job_waiting_time_minimum_seconds: int | float | None
    templates_total: int
    inventories_total: int


class HostSummaryFields(TypedDict):
    unreachable_total: int
    failed_total: int
    ok_total: int
    skipped_total: int
    ignored_total: int
    rescued_total: int
    successful_hosts_total: int
    failed_hosts_total: int
    unreachable_hosts_total: int


class JobsByJobType(JobStatsFields, HostSummaryFields):
    job_type: str
    controller_version: str | None
    is_automation: bool
    ansible_versions: list[str]


class JobsByLaunchType(JobStatsFields, HostSummaryFields):
    launch_type: str
    controller_version: str | None
    ansible_versions: list[str]


class JobsByAnsibleVersion(JobStatsFields, HostSummaryFields):
    ansible_version: str | None
    controller_version: str | None


class JobsByControllerVersion(JobStatsFields):
    controller_version: str | None
    ansible_versions: list[str]


class JobsByInstalledCollection(JobStatsFields):
    collection: str
    version: str
    ansible_versions: list[str]


class EventStatsFields(TypedDict):
    collection_source: str
    jobs_total: int
    jobs_successful_total: int
    jobs_failed_total: int
    jobs_duration_total_seconds: int | float
    jobs_waiting_time_total_seconds: int | float
    jobs_never_started_total: int
    jobs_successful_duration_total_seconds: int | float
    jobs_failed_duration_total_seconds: int | float
    tasks_total: int
    runner_on_ok_total: int
    runner_on_failed_total: int
    runner_on_unreachable_total: int
    runner_on_async_ok_total: int
    runner_on_async_failed_total: int
    runner_item_on_ok_total: int
    runner_item_on_failed_total: int
    runner_retry_total: int
    ignore_errors_total: int
    warnings_total: int
    deprecations_total: int
    collected_events_total: int
    event_data_size_total: int | float
    ansible_versions: list[str]


class ModuleStats(EventStatsFields):
    module: str
    collection: str


class CollectionStats(EventStatsFields):
    collection: str
    unique_hosts_total: int


class RoleStats(EventStatsFields):
    role: str
    collection: str | None


class ObservabilityByTask(TypedDict):
    collector_type: str
    executions_total: int
    executions_missing_total: int
    execution_duration_total_seconds: int | float | None
    execution_duration_minimum_seconds: int | float | None
    execution_duration_maximum_seconds: int | float | None


class IndirectNodesByCollection(TypedDict):
    collection: str
    host_count: int


class IndirectNodesByModule(TypedDict):
    module: str
    host_count: int


class AnonymizedPayload(TypedDict):
    statistics: Statistics
    rollup_period_ansible_versions: list[str]
    rollup_period_scm_types: list[str]
    rollup_period_credential_types: list[str]
    jobs_by_job_type: list[JobsByJobType]
    jobs_by_launch_type: list[JobsByLaunchType]
    jobs_by_ansible_version: list[JobsByAnsibleVersion]
    jobs_by_controller_version: list[JobsByControllerVersion]
    jobs_by_installed_collections_versions: list[JobsByInstalledCollection]
    table_metadata: dict[str, int]
    controller_versions: list[str]
    feature_flags: list[str]
    observability_by_tasks: list[ObservabilityByTask]
    indirect_nodes_by_collection: list[IndirectNodesByCollection]
    indirect_nodes_by_module: list[IndirectNodesByModule]
    # Present only when collected_events_total > 0
    module_stats: NotRequired[list[ModuleStats]]
    collection_stats: NotRequired[list[CollectionStats]]
    role_stats: NotRequired[list[RoleStats]]
