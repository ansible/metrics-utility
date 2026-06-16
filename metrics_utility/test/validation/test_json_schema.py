import csv
import glob
import io
import json
import os
import pathlib
import tarfile

from datetime import datetime

import jsonschema
import pytest

from metrics_utility.test.util import run_gather_int


env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

year = '2025'
uuid = '00000000-0000-0000-0000-000000000000'

file_glob = f'./metrics_utility/test/test_data/data/{year}/*/*/{uuid}-*.tar.gz'


def _load_schema(request, filename):
    schemas_dir = pathlib.Path(request.config.rootpath).joinpath('schemas')
    with open(schemas_dir.joinpath(filename)) as f:
        return json.load(f)


def _generate_and_extract(member_path, extra_env=None):
    merged_env = {**env_vars, **(extra_env or {})}
    run_gather_int(
        merged_env,
        {'ship': True, 'since': '2025-06-12', 'until': '2025-06-14'},
    )

    tarballs = glob.glob(file_glob)
    assert len(tarballs) > 0, f'No tarballs found matching {file_glob}'

    with tarfile.open(tarballs[0], 'r:gz') as tar:
        member = tar.extractfile(member_path)
        assert member is not None, f'{member_path} not found in tarball'
        return json.load(member)


@pytest.fixture
def cleanup_glob():
    for file in glob.glob(file_glob):
        os.remove(file)


@pytest.fixture
def manifest_schema(request):
    return _load_schema(request, 'manifest.jsonschema')


@pytest.fixture
def generated_manifest(cleanup_glob):
    return _generate_and_extract('./manifest.json')


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_manifest_schema_validation(manifest_schema, generated_manifest):
    jsonschema.validate(instance=generated_manifest, schema=manifest_schema)


@pytest.fixture
def generated_config_with_schema(request, cleanup_glob):
    run_gather_int(
        env_vars,
        {'ship': True, 'since': '2025-06-12', 'until': '2025-06-14'},
    )

    tarballs = glob.glob(file_glob)
    assert len(tarballs) > 0, f'No tarballs found matching {file_glob}'

    with tarfile.open(tarballs[0], 'r:gz') as tar:
        manifest_member = tar.extractfile('./manifest.json')
        assert manifest_member is not None, 'manifest.json not found in tarball'
        manifest = json.load(manifest_member)

        config_version = manifest.get('config.json')
        assert config_version is not None, 'config.json not listed in manifest'

        schema = _load_schema(request, f'config-{config_version}.jsonschema')

        config_member = tar.extractfile('./config.json')
        assert config_member is not None, 'config.json not found in tarball'
        config = json.load(config_member)

    return config, schema


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_config_schema_validation(generated_config_with_schema):
    config, schema = generated_config_with_schema
    jsonschema.validate(instance=config, schema=schema)


@pytest.fixture
def generated_total_workers_vcpu_with_schema(request, cleanup_glob):
    vcpu_env = {
        'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'total_workers_vcpu',
        'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster',
    }
    merged_env = {**env_vars, **vcpu_env}
    run_gather_int(
        merged_env,
        {'ship': True, 'since': '2025-06-12', 'until': '2025-06-14'},
    )

    tarballs = glob.glob(file_glob)
    assert len(tarballs) > 0, f'No tarballs found matching {file_glob}'

    total_workers_vcpu_version = "1.0"

    for tarball_path in tarballs:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            try:
                vcpu_member = tar.extractfile('./total_workers_vcpu.json')
            except KeyError:
                continue
            assert vcpu_member is not None, 'total_workers_vcpu.json not found in tarball'
            vcpu = json.load(vcpu_member)
            schema = _load_schema(request, f'total_workers_vcpu-{total_workers_vcpu_version}.jsonschema')
            return vcpu, schema

    pytest.fail(f'total_workers_vcpu-{total_workers_vcpu_version}.json not found in any tarball')


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_total_workers_vcpu_schema_validation(generated_total_workers_vcpu_with_schema):
    vcpu, schema = generated_total_workers_vcpu_with_schema
    jsonschema.validate(instance=vcpu, schema=schema)


@pytest.fixture
def generated_data_collection_status(request, cleanup_glob):
    run_gather_int(
        env_vars,
        {'ship': True, 'since': '2025-06-12', 'until': '2025-06-14'},
    )

    tarballs = glob.glob(file_glob)
    assert len(tarballs) > 0, f'No tarballs found matching {file_glob}'

    with tarfile.open(tarballs[0], 'r:gz') as tar:
        member = tar.extractfile('./data_collection_status.csv')
        assert member is not None, 'data_collection_status.csv not found in tarball'
        reader = csv.DictReader(io.TextIOWrapper(member))
        data = list(reader)

    schema = _load_schema(request, 'data_collection_status.jsonschema')
    return data, schema


def validate_with_datetime(schema, instance):
    BaseVal = jsonschema.validators.validator_for(schema)

    def is_datetime(_, inst):
        try:
            datetime.fromisoformat(inst)
        except (TypeError, ValueError):
            return False
        return True

    date_check = BaseVal.TYPE_CHECKER.redefine('datetime', is_datetime)
    Validator = jsonschema.validators.extend(BaseVal, type_checker=date_check)
    Validator(schema=schema).validate(instance)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_data_collection_status_schema_validation(generated_data_collection_status):
    data, schema = generated_data_collection_status
    validate_with_datetime(schema, data)


@pytest.fixture
def generated_job_host_summary(request, cleanup_glob):
    run_gather_int(
        env_vars,
        {'ship': True, 'since': '2025-06-12', 'until': '2025-06-14'},
    )

    tarballs = glob.glob(file_glob)
    assert len(tarballs) > 0, f'No tarballs found matching {file_glob}'

    job_host_summary_version = "1.2"
    
    for tarball_path in tarballs:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            try:
                member = tar.extractfile('./job_host_summary.csv')
            except KeyError:
                continue
            assert member is not None, 'job_host_summary.csv not found in tarball'
            reader = csv.DictReader(io.TextIOWrapper(member))
            data = list(reader)
            if len(data) > 0:
                schema = _load_schema(request, f'job_host_summary-{job_host_summary_version}.jsonschema')
                return data, schema

    pytest.fail(f'job_host_summary-{job_host_summary_version}.csv with data not found in any tarball')


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_job_host_summary_schema_validation(generated_job_host_summary):
    data, schema = generated_job_host_summary
    validate_with_datetime(schema, data)
