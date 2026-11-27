"""Synthetic canary privacy suite for Segment telemetry and billing archives.

Every test in this module is self-contained: no PostgreSQL, SeaweedFS, or Go
mock server is required.  Realistic DataFrames are pushed through the real
rollup processors, and canaries are then hunted in the resulting payloads,
HTTP bodies, log output, and saved configuration.
"""

import gzip
import json
import logging
import types
import uuid

from unittest.mock import MagicMock

import pandas as pd
import pytest

from pydantic import ConfigDict, TypeAdapter, with_config

from metrics_utility.anonymized_rollups.anonymized_rollups import anonymize_rollups
from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup
from metrics_utility.anonymized_rollups.credentials_anonymized_rollup import CredentialsAnonymizedRollup
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollup
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup
from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup
from metrics_utility.anonymized_rollups.indirect_managed_nodes_anonymized_rollup import IndirectManagedNodesAnonymizedRollup
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollup
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup
from metrics_utility.anonymized_rollups.task_executions_anonymized_rollup import TaskExecutionsAnonymizedRollup
from metrics_utility.anonymized_rollups.types import AnonymizedPayload
from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.base.collector import Collector as BaseCollector
from metrics_utility.library.collectors.controller.credentials_service import credentials_service
from metrics_utility.library.collectors.controller.table_metadata import table_metadata
from metrics_utility.library.storage import segment as segment_module
from metrics_utility.library.storage.segment import StorageSegment
from metrics_utility.test.privacy.canary_data import (
    CANARY_CANDLEPIN_CERT_PEM,
    CANARY_CANDLEPIN_KEY_PEM,
    CANARY_CUSTOM_COLLECTION,
    CANARY_CUSTOM_MODULE,
    CANARY_CUSTOM_ROLE,
    CANARY_EE_IMAGE,
    CANARY_EE_NAME,
    CANARY_HOST_NAME,
    CANARY_INSTALL_UUID,
    CANARY_INVENTORY_NAME,
    CANARY_JOB_TEMPLATE_NAME,
    CANARY_MACHINE_ID,
    CANARY_ORG_NAME,
    CANARY_PLAYBOOK_NAME,
    CANARY_PROJECT_NAME,
    CANARY_S3_ACCESS_KEY,
    CANARY_S3_SECRET_KEY,
    CANARY_SERIAL,
    SEGMENT_FORBIDDEN_CANARIES,
    canary_variants,
    find_forbidden_keys,
    find_leaked_canaries,
)


_CREATED = '2024-01-01 00:00:00+00'
_STARTED = '2024-01-01 00:01:00+00'
_FINISHED = '2024-01-01 00:10:00+00'

_FORBIDDEN_PAYLOAD_KEYS = {
    'host_ids',
    'host_names',
    'host_name',
    'host_remote_id',
    'organizations',
    'organization_name',
    'modules_used_per_playbook_total',
    'playbook_events',
    'indirect_managed_nodes',
    'canonical_facts',
    'job_template_name',
    'inventory_name',
    'project_name',
}

_STRICT_ADAPTER = TypeAdapter(with_config(ConfigDict(extra='forbid'))(AnonymizedPayload))


# ---------------------------------------------------------------------------
# Realistic fixtures – public allowlisted data mixed with every canary class
# ---------------------------------------------------------------------------


def _events_dataframe():
    """Build main_jobevent rows with public modules and private custom content."""
    rows = [
        {
            'job_id': 1001,
            'playbook': 'site.yml',
            'host_id': 1,
            'task_uuid': 't-001',
            'event': 'runner_on_ok',
            'task_action': 'ansible.builtin.copy',
            'job_created': _CREATED,
            'job_started': _STARTED,
            'job_finished': _FINISHED,
            'job_failed': False,
            'resolved_action': None,
            'resolved_role': 'ansible.builtin.base',
            'role': 'ansible.builtin.base',
            'ignore_errors': False,
            'ansible_version': '2.16.0',
            'event_data_length': 120,
        },
        {
            'job_id': 1001,
            'playbook': CANARY_PLAYBOOK_NAME,
            'host_id': 2,
            'task_uuid': 't-002',
            'event': 'runner_on_ok',
            'task_action': 'ansible.posix.firewalld',
            'job_created': _CREATED,
            'job_started': _STARTED,
            'job_finished': _FINISHED,
            'job_failed': False,
            'resolved_action': None,
            'resolved_role': 'ansible.posix.firewalld',
            'role': 'ansible.posix.firewalld',
            'ignore_errors': False,
            'ansible_version': '2.16.0',
            'event_data_length': 80,
        },
        {
            'job_id': 1002,
            'playbook': CANARY_PLAYBOOK_NAME,
            'host_id': CANARY_MACHINE_ID,
            'task_uuid': 't-003',
            'event': 'runner_on_failed',
            'task_action': CANARY_CUSTOM_MODULE,
            'job_created': _CREATED,
            'job_started': _STARTED,
            'job_finished': _FINISHED,
            'job_failed': True,
            'resolved_action': None,
            'resolved_role': CANARY_CUSTOM_ROLE,
            'role': CANARY_CUSTOM_ROLE,
            'ignore_errors': False,
            'ansible_version': '2.16.0',
            'event_data_length': 300,
        },
        {
            'job_id': 1002,
            'playbook': CANARY_PLAYBOOK_NAME,
            'host_id': 3,
            'task_uuid': 't-004',
            'event': 'runner_on_ok',
            'task_action': f'{CANARY_CUSTOM_COLLECTION}.gather',
            'job_created': _CREATED,
            'job_started': _STARTED,
            'job_finished': _FINISHED,
            'job_failed': True,
            'resolved_action': None,
            'resolved_role': None,
            'role': None,
            'ignore_errors': False,
            'ansible_version': '2.16.0',
            'event_data_length': 40,
        },
    ]
    return pd.DataFrame(rows)


def _jobs_dataframe():
    installed_collections = json.dumps(
        {
            'ansible.builtin': {'version': '2.16.0'},
            'ansible.posix': {'version': '1.5.4'},
            CANARY_CUSTOM_COLLECTION: {'version': '1.0.0'},
        }
    )
    return pd.DataFrame(
        [
            {
                'id': 1001,
                'started': _STARTED,
                'finished': _FINISHED,
                'failed': False,
                'job_template_name': CANARY_JOB_TEMPLATE_NAME,
                'unified_job_template_id': 11,
                'controller_node': CANARY_HOST_NAME,
                'ansible_version': '2.16.0',
                'organization_name': CANARY_ORG_NAME,
                'created': _CREATED,
                'model': 'job',
                'launch_type': 'manual',
                'forks': 5,
                'inventory_name': CANARY_INVENTORY_NAME,
                'inventory_id': 21,
                'project_name': CANARY_PROJECT_NAME,
                'scm_type': 'git',
                'installed_collections': installed_collections,
                'execution_environment_id': 1,
            },
            {
                'id': 1002,
                'started': _STARTED,
                'finished': _FINISHED,
                'failed': True,
                'job_template_name': CANARY_JOB_TEMPLATE_NAME,
                'unified_job_template_id': 11,
                'controller_node': CANARY_HOST_NAME,
                'ansible_version': '2.16.0',
                'organization_name': CANARY_ORG_NAME,
                'created': _CREATED,
                'model': 'job',
                'launch_type': 'manual',
                'forks': 5,
                'inventory_name': CANARY_INVENTORY_NAME,
                'inventory_id': 21,
                'project_name': CANARY_PROJECT_NAME,
                'scm_type': 'git',
                'installed_collections': installed_collections,
                'execution_environment_id': 1,
            },
        ]
    )


def _job_host_summary_dataframe():
    return pd.DataFrame(
        [
            {
                'dark': 0,
                'failures': 0,
                'ok': 3,
                'skipped': 0,
                'ignored': 0,
                'rescued': 0,
                'host_name': CANARY_HOST_NAME,
                'host_remote_id': CANARY_SERIAL,
                'job_remote_id': 1001,
                'job_template_name': CANARY_JOB_TEMPLATE_NAME,
                'model': 'job',
                'ansible_version': '2.16.0',
                'launch_type': 'manual',
            },
            {
                'dark': 1,
                'failures': 0,
                'ok': 1,
                'skipped': 0,
                'ignored': 0,
                'rescued': 0,
                'host_name': CANARY_HOST_NAME,
                'host_remote_id': CANARY_MACHINE_ID,
                'job_remote_id': 1002,
                'job_template_name': CANARY_JOB_TEMPLATE_NAME,
                'model': 'job',
                'ansible_version': '2.16.0',
                'launch_type': 'manual',
            },
        ]
    )


def _execution_environments_dataframe():
    return pd.DataFrame(
        [
            {'managed': 't', 'name': 'Default execution environment', 'image': 'quay.io/ansible/awx-ee:latest'},
            {'managed': 't', 'name': 'Default execution environment', 'image': 'quay.io/ansible/awx-ee:latest'},
            {'managed': 'f', 'name': CANARY_EE_NAME, 'image': CANARY_EE_IMAGE},
        ]
    )


def _credentials_dataframe():
    return pd.DataFrame([{'credential_type': 'Machine'}, {'credential_type': 'Machine'}])


def _table_metadata_dataframe():
    return pd.DataFrame(
        [
            {
                'schemaname': 'public',
                'tablename': 'main_jobevent',
                'estimated_row_count': 1000,
                'total_size_bytes': 2048,
                'table_size_bytes': 1024,
                'indexes_size_bytes': 1024,
            }
        ]
    )


def _controller_version_dataframe():
    return pd.DataFrame([{'controller_version': '4.5.0'}])


def _feature_flags_dataframe():
    return pd.DataFrame([{'name': 'FEATURE_INDIRECT_NODE_COUNTING'}])


def _task_executions_dataframe():
    return pd.DataFrame([{'collector_type': 'unified_jobs', 'started_at': _STARTED, 'completed_at': _FINISHED}])


def _indirect_managed_nodes_dataframe():
    return pd.DataFrame(
        [
            {
                'id': 1,
                'host_name': CANARY_HOST_NAME,
                'organization_name': CANARY_ORG_NAME,
                'events': json.dumps([CANARY_CUSTOM_MODULE, 'ansible.builtin.setup']),
            }
        ]
    )


def _build_segment_payload():
    """Run every rollup processor and return the final anonymized payload."""
    events = EventModulesAnonymizedRollup()
    events_json = events.base(events.prepare(_events_dataframe()))['json']

    jobs = JobsAnonymizedRollup()
    jobs_json = jobs.base(jobs.prepare(_jobs_dataframe()))['json']

    job_host_summary = JobHostSummaryAnonymizedRollup()
    job_host_summary_json = job_host_summary.base(job_host_summary.prepare(_job_host_summary_dataframe()))['json']

    execution_environments = ExecutionEnvironmentsAnonymizedRollup()
    execution_environments_json = execution_environments.base(execution_environments.prepare(_execution_environments_dataframe()))['json']

    credentials = CredentialsAnonymizedRollup()
    credentials_json = credentials.base(credentials.prepare(_credentials_dataframe()))['json']

    table = TableMetadataAnonymizedRollup()
    table_json = table.base(table.prepare(_table_metadata_dataframe()))['json']

    controller_version = ControllerVersionAnonymizedRollup()
    controller_version_json = controller_version.base(controller_version.prepare(_controller_version_dataframe()))['json']

    feature_flags = FeatureFlagsAnonymizedRollup()
    feature_flags_json = feature_flags.base(feature_flags.prepare(_feature_flags_dataframe()))['json']

    task_executions = TaskExecutionsAnonymizedRollup()
    task_executions_json = task_executions.base(task_executions.prepare(_task_executions_dataframe()))['json']

    indirect = IndirectManagedNodesAnonymizedRollup()
    indirect_json = indirect.base(indirect.prepare(_indirect_managed_nodes_dataframe()))['json']

    return anonymize_rollups(
        events_modules_rollup=events_json,
        execution_environments_rollup=execution_environments_json,
        jobs_rollup=jobs_json,
        job_host_summary_rollup=job_host_summary_json,
        credentials_rollup=credentials_json,
        table_metadata_rollup=table_json,
        controller_version_rollup=controller_version_json,
        feature_flags_rollup=feature_flags_json,
        task_executions_rollup=task_executions_json,
        indirect_managed_nodes_rollup=indirect_json,
    )


@pytest.fixture(scope='module')
def segment_payload():
    return _build_segment_payload()


# ---------------------------------------------------------------------------
# (a) Segment telemetry path – rollup + anonymization end to end
# ---------------------------------------------------------------------------


def test_segment_payload_matches_strict_schema(segment_payload):
    """The anonymized payload must satisfy the TypedDict schema with no extras."""
    _STRICT_ADAPTER.validate_python(segment_payload, strict=True)


def test_segment_payload_contains_no_canaries(segment_payload):
    """No forbidden canary representation may appear in the Segment payload."""
    assert find_leaked_canaries(json.dumps(segment_payload), SEGMENT_FORBIDDEN_CANARIES) == []


def test_segment_payload_contains_no_forbidden_keys(segment_payload):
    """Raw identifier keys must never be projected into the anonymized payload."""
    assert find_forbidden_keys(segment_payload, _FORBIDDEN_PAYLOAD_KEYS) == []


def test_segment_payload_preserves_public_allowlist(segment_payload):
    """Public allowlisted content survives, proving the canary fixture is real data."""
    module_names = {item['module'] for item in segment_payload['module_stats']}
    assert 'ansible.builtin.copy' in module_names
    assert 'ansible.posix.firewalld' in module_names

    collection_names = {item['collection'] for item in segment_payload['collection_stats']}
    assert {'ansible.builtin', 'ansible.posix'} <= collection_names

    installed_collections = {item['collection'] for item in segment_payload['jobs_by_installed_collections_versions']}
    assert {'ansible.builtin', 'ansible.posix'} <= installed_collections
    assert CANARY_CUSTOM_COLLECTION not in installed_collections

    assert segment_payload['rollup_period_credential_types'] == ['Machine']
    assert segment_payload['indirect_nodes_by_collection'] == [{'collection': 'ansible.builtin', 'host_count': 1}]
    assert segment_payload['indirect_nodes_by_module'] == [{'module': 'ansible.builtin.setup', 'host_count': 1}]
    assert segment_payload['observability_by_tasks'][0]['collector_type'] == 'unified_jobs'


def test_segment_payload_has_no_custom_content(segment_payload):
    """Every custom module/collection/role entry must be dropped entirely."""
    for stats_key in ('module_stats', 'collection_stats', 'role_stats'):
        assert all(item.get('collection_source') != 'Custom' for item in segment_payload[stats_key])

    custom_markers = (CANARY_CUSTOM_COLLECTION, CANARY_CUSTOM_MODULE, CANARY_CUSTOM_ROLE)
    blob = json.dumps(segment_payload)
    for marker in custom_markers:
        assert marker not in blob


# ---------------------------------------------------------------------------
# (b) Fail-closed allowlist behaviour
# ---------------------------------------------------------------------------


def test_segment_anonymization_fails_closed_without_allowlist(monkeypatch):
    """When the known-collection list is unavailable, unknown data is dropped."""
    monkeypatch.setattr('metrics_utility.anonymized_rollups.anonymized_rollups.load_known_collections', dict)

    payload = _build_segment_payload()

    assert payload['jobs_by_installed_collections_versions'] == []
    assert payload['indirect_nodes_by_collection'] == []
    assert payload['indirect_nodes_by_module'] == []
    for stats_key in ('module_stats', 'collection_stats', 'role_stats'):
        assert all(item.get('collection_source') != 'Custom' for item in payload[stats_key])

    assert find_leaked_canaries(json.dumps(payload), SEGMENT_FORBIDDEN_CANARIES) == []


# ---------------------------------------------------------------------------
# (c) Transport envelope and log capture
# ---------------------------------------------------------------------------


def _fake_segment_response():
    response = MagicMock()
    response.status_code = 200
    response.text = ''
    return response


def test_segment_transport_envelope_and_logs_are_canary_free(segment_payload, monkeypatch, caplog):
    """Capture the gzipped HTTP body and logs; assert unlinkable, canary-free sends."""
    captured = []

    def fake_post(endpoint, data=None, headers=None, auth=None, timeout=None):
        captured.append({'endpoint': endpoint, 'data': data, 'headers': headers, 'auth': auth, 'timeout': timeout})
        return _fake_segment_response()

    monkeypatch.setattr(segment_module.requests, 'post', fake_post)
    caplog.set_level(logging.DEBUG)

    storage = StorageSegment(debug=True, write_key='segment-test-write-key', host='https://api.segment.io', gzip=True)
    storage.put('Metrics Artifact', dict=segment_payload)
    storage.put('Metrics Artifact', dict=segment_payload)

    assert len(captured) == 2

    decoded_bodies = []
    for request in captured:
        assert request['data']
        decoded_bodies.append(gzip.decompress(request['data']).decode('utf-8'))

    payload_blob = '\n'.join(decoded_bodies)
    header_blob = json.dumps([request['headers'] for request in captured])
    assert find_leaked_canaries(payload_blob, SEGMENT_FORBIDDEN_CANARIES) == []
    assert find_leaked_canaries(header_blob, SEGMENT_FORBIDDEN_CANARIES) == []
    assert find_leaked_canaries(caplog.text, SEGMENT_FORBIDDEN_CANARIES) == []

    anonymous_ids = set()
    for body in decoded_bodies:
        envelope = json.loads(body)
        assert envelope['batch']
        assert envelope['sentAt']
        for event in envelope['batch']:
            assert 'userId' not in event
            assert event['context'] == {}
            anonymous_ids.add(event['anonymousId'])

    assert len(anonymous_ids) == 2
    for anonymous_id in anonymous_ids:
        parsed = uuid.UUID(anonymous_id)
        assert parsed.version == 4


# ---------------------------------------------------------------------------
# (d) Billing archive path – config redaction
# ---------------------------------------------------------------------------


class _InMemoryConfigCollection:
    """Minimal stand-in for CollectionJSON backed by ``config_collection.data``."""

    def __init__(self, data):
        self.data = json.dumps(data)

    def _save_gathering(self, data):
        self.data = json.dumps(data)


def _gather_billing_config(monkeypatch, billing_provider_params):
    monkeypatch.setattr(BaseCollector, '_gather_config', lambda self: True)

    collector = Collector(
        collector_module=types.ModuleType('privacy_test_collector_module'),
        billing_provider_params=billing_provider_params,
    )
    config_collection = _InMemoryConfigCollection({'controller_version': '4.5.0', 'install_uuid': CANARY_INSTALL_UUID})
    collector.collections = {'config': config_collection}

    assert collector._gather_config() is True
    return json.loads(config_collection.data)


def test_billing_config_redacts_candlepin_pem(monkeypatch):
    """mTLS key material must never reach the archive's config.json."""
    config_json = _gather_billing_config(
        monkeypatch,
        {
            'billing_account_id': '123456',
            'red_hat_org_id': '987654',
            'billing_provider': 'aws',
            'candlepin_cert_pem': CANARY_CANDLEPIN_CERT_PEM,
            'candlepin_key_pem': CANARY_CANDLEPIN_KEY_PEM,
        },
    )

    serialized = json.dumps(config_json)
    billing_params = config_json['billing_provider_params']

    assert billing_params['billing_account_id'] == '123456'
    assert billing_params['red_hat_org_id'] == '987654'
    assert billing_params['billing_provider'] == 'aws'
    assert 'candlepin_cert_pem' not in billing_params
    assert 'candlepin_key_pem' not in billing_params
    assert 'CANARY_CANDLEPIN_CERT_BODY' not in serialized
    assert 'CANARY_CANDLEPIN_KEY_BODY' not in serialized
    assert find_leaked_canaries(serialized, [CANARY_CANDLEPIN_CERT_PEM, CANARY_CANDLEPIN_KEY_PEM]) == []


@pytest.mark.xfail(strict=True, reason='S3 bucket_* redaction in _gather_config() is being fixed in a separate PR')
def test_billing_config_redacts_s3_credentials(monkeypatch):
    """S3 credentials must not be persisted into config.json."""
    config_json = _gather_billing_config(
        monkeypatch,
        {
            'billing_provider': 'aws',
            'bucket_access_key': CANARY_S3_ACCESS_KEY,
            'bucket_secret_key': CANARY_S3_SECRET_KEY,
        },
    )

    assert find_leaked_canaries(json.dumps(config_json), [CANARY_S3_ACCESS_KEY, CANARY_S3_SECRET_KEY]) == []


# ---------------------------------------------------------------------------
# (e) AWX / controller SQL privacy invariants
# ---------------------------------------------------------------------------


class _CapturingSQLOutput:
    """Capture the SQL a collector would execute instead of touching a database."""

    def __init__(self):
        self.queries = []

    def sql(self, db, query, params=None):
        self.queries.append(query)
        return pd.DataFrame()


def test_credentials_service_sql_requires_managed_credential_types():
    """CredentialsAnonymizedRollup has no internal allowlist; SQL managed=true is the boundary."""
    output = _CapturingSQLOutput()
    credentials_service(db=None, since=None, until=None, output=output).gather()

    assert len(output.queries) == 1
    query = ' '.join(output.queries[0].split())
    assert 'main_credentialtype.managed = true' in query
    assert 'main_unifiedjob_credentials' in query


def test_table_metadata_sql_is_scoped_to_approved_tables():
    """table_metadata must only describe the three contract-approved tables."""
    output = _CapturingSQLOutput()
    table_metadata(db=None, output=output).gather()

    assert len(output.queries) == 1
    query = ' '.join(output.queries[0].split())
    for table in ('main_jobevent', 'main_unifiedjob', 'main_jobhostsummary'):
        assert f"c.relname = '{table}'" in query
    assert query.count("c.relname = '") == 3


# ---------------------------------------------------------------------------
# (f) Canary helper self-tests
# ---------------------------------------------------------------------------


def test_find_leaked_canaries_detects_every_encoding():
    """Raw, lowercase, URL-encoded, base64, and SHA-256 representations are detected."""
    token = CANARY_EE_IMAGE
    variants = canary_variants(token)
    blob = '\n'.join(variants.values())

    leaks = find_leaked_canaries(blob, [token])

    assert {variant_name for _, variant_name in leaks} == set(variants)


def test_find_leaked_canaries_returns_empty_for_clean_text():
    assert find_leaked_canaries('public ansible.builtin content only', [CANARY_HOST_NAME]) == []


def test_find_forbidden_keys_walks_nested_structures():
    payload = {'stats': {'host_names': ['x']}, 'rows': [{'organization_name': 'acme'}]}

    found = find_forbidden_keys(payload, {'host_names', 'organization_name'})

    assert found == ['stats.host_names', 'rows[0].organization_name']
