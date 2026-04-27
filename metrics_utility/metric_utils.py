"""Constants for managed node type classification in billing reports."""

DIRECT = 0
"""Integer code for directly managed nodes (automated by AWX)."""

INDIRECT = 1
"""Integer code for indirectly managed nodes (discovered via audit)."""
# later also EDGE = 2

MANAGED_NODE_TYPES = {DIRECT: 'DIRECT', INDIRECT: 'INDIRECT'}
"""Mapping from integer node-type codes to human-readable string labels."""

DATETIME64_NS = 'datetime64[ns]'

# Shared schema constants for job host summary dataframes (used by both
# DataframeJobhostSummaryUsage and DataframeJobHostSummary)
JOB_HOST_SUMMARY_INDEX_COLUMNS = [
    'organization_name',
    'job_template_name',
    'host_name',
    'original_host_name',
    'install_uuid',
    'job_remote_id',
]

JOB_HOST_SUMMARY_DATA_COLUMNS = [
    'host_runs',
    'task_runs',
    'first_automation',
    'last_automation',
    'job_created',
    'managed_node_type',
    'managed_node_types_set',
    'canonical_facts',
    'facts',
    'events',
    'host_names_before_dedup',
]

JOB_HOST_SUMMARY_CAST_TYPES = {
    'task_runs': int,
    'host_runs': int,
    'managed_node_type': int,
    'first_automation': DATETIME64_NS,
    'last_automation': DATETIME64_NS,
    'job_created': DATETIME64_NS,
}

JOB_HOST_SUMMARY_OPERATIONS = {
    'first_automation': 'min',
    'last_automation': 'max',
    'job_created': 'max',
    'managed_node_type': 'min',
    'managed_node_types_set': 'combine_set',
    'events': 'combine_set',
    'canonical_facts': 'combine_json_values',
    'facts': 'combine_json_values',
    'host_names_before_dedup': 'combine_set',
}
