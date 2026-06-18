"""Integration test: vCPU gather against mock Prometheus with schema validation."""

import glob
import io
import json
import os
import tarfile
import urllib.request

from importlib.resources import files
from unittest.mock import patch

import jsonschema
import pytest

from metrics_utility.test.util import run_gather_int


MOCK_PROMETHEUS_URL = os.getenv('MOCK_PROMETHEUS_URL', 'http://localhost:9090')

K8S_TOKEN_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token'
K8S_CERT_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'

_original_exists = os.path.exists
_original_open = open


def _fake_exists(path):
    if path in (K8S_TOKEN_PATH, K8S_CERT_PATH):
        return True
    return _original_exists(path)


def _fake_open(path, *args, **kwargs):
    if str(path) == K8S_TOKEN_PATH:
        return io.StringIO('fake-token')
    return _original_open(path, *args, **kwargs)


env_vars = {
    'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
    'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true',
    'METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES': 'true',
    'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'total_workers_vcpu',
    'METRICS_UTILITY_PROMETHEUS_URL': MOCK_PROMETHEUS_URL,
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_USAGE_BASED_METERING_ENABLED': 'true',
}

year = '2025'
uuid = '00000000-0000-0000-0000-000000000000'
file_glob = f'./metrics_utility/test/test_data/data/{year}/*/*/{uuid}-*.tar.gz'


def _load_schema(filename):
    return json.loads(files('metrics_utility.schemas').joinpath(filename).read_text())


def _configure_mock(**kwargs):
    data = json.dumps(kwargs).encode()
    req = urllib.request.Request(f'{MOCK_PROMETHEUS_URL}/config', data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    urllib.request.urlopen(req, timeout=5)


def _reset_mock():
    req = urllib.request.Request(f'{MOCK_PROMETHEUS_URL}/reset', method='POST')
    urllib.request.urlopen(req, timeout=5)


@pytest.fixture
def cleanup_glob():
    for f in glob.glob(file_glob):
        os.remove(f)
    yield
    for f in glob.glob(file_glob):
        os.remove(f)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@patch('metrics_utility.automation_controller_billing.collectors.open', create=True, side_effect=_fake_open)
@patch('metrics_utility.automation_controller_billing.collectors.os.path.exists', side_effect=_fake_exists)
def test_vcpu_gather_validates_against_schema(mock_exists, mock_open, cleanup_glob):
    _reset_mock()
    _configure_mock(cpu_value='16', empty_result=False)

    run_gather_int(
        env_vars,
        {
            'ship': True,
            'since': '2025-06-12',
            'until': '2025-06-14',
        },
    )

    tarballs = glob.glob(file_glob)
    assert len(tarballs) > 0, f'No tarballs found matching {file_glob}'

    vcpu_data = None
    manifest_data = None
    for tarball_path in tarballs:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            try:
                vcpu_member = tar.extractfile('./total_workers_vcpu.json')
            except KeyError:
                continue
            if vcpu_member is None:
                continue
            vcpu_data = json.load(vcpu_member)

            manifest_member = tar.extractfile('./manifest.json')
            if manifest_member is not None:
                manifest_data = json.load(manifest_member)
            break

    assert vcpu_data is not None, 'total_workers_vcpu.json not found in any tarball'

    # validate against JSON schema
    schema = _load_schema('total_workers_vcpu-1.0.jsonschema')
    jsonschema.validate(instance=vcpu_data, schema=schema)

    # verify data came from mock Prometheus (not the stub value of 1)
    assert vcpu_data['total_workers_vcpu'] == 16
    assert vcpu_data['cluster_name'] == 'test-cluster'
    assert 'timestamp' in vcpu_data

    # validate manifest references vcpu
    assert manifest_data is not None, 'manifest.json not found in tarball'
    manifest_schema = _load_schema('manifest.jsonschema')
    jsonschema.validate(instance=manifest_data, schema=manifest_schema)
    assert 'total_workers_vcpu.json' in manifest_data

    # verify Prometheus was actually called (instant + range query)
    with urllib.request.urlopen(f'{MOCK_PROMETHEUS_URL}/requests', timeout=5) as resp:
        captured = json.loads(resp.read())
    paths = [r['path'] for r in captured]
    assert '/api/v1/query' in paths
    assert '/api/v1/query_range' in paths
