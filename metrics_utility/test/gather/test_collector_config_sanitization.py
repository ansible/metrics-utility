"""Billing collector: only non-secret metadata is persisted in config.json."""

import io
import json
import tarfile

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.automation_controller_billing.package.package_s3 import PackageS3
from metrics_utility.base import CollectionJSON, register
from metrics_utility.base.collector import Collector as BaseCollector


S3_PARAMS = {
    'ship_path': 'metrics-utility/shipped_data',
    'bucket_name': 'test-bucket',
    'bucket_endpoint': 'https://s3.example.com',
    'bucket_region': 'us-east-1',
    'bucket_access_key': 'AKIA_TEST_ACCESS_KEY',
    'bucket_secret_key': 'test-secret-key',
}

CRC_PARAMS = {
    'billing_provider': 'aws',
    'billing_account_id': '123456789012',
    'red_hat_org_id': '99900001',
    'candlepin_cert_pem': '-----BEGIN CERTIFICATE-----\ntest-cert\n-----END CERTIFICATE-----',
    'candlepin_key_pem': '-----BEGIN PRIVATE KEY-----\ntest-key\n-----END PRIVATE KEY-----',
}


@register('config', '2.0', format='json', config=True)
def _fake_config(since, until, output):
    """Minimal replacement for the real config collector."""
    return {'install_uuid': 'test-install-uuid', 'billing_provider_params': {'leaked': 'value'}}


def _make_collector(billing_provider_params, ship_target='s3'):
    collector = Collector(ship_target=ship_target, billing_provider_params=billing_provider_params)
    collector.last_gathered_entries = {}
    collector.gather_dir = None
    collector.gather_until = datetime.now(UTC)
    collector.gather_since = collector.gather_until - timedelta(days=1)
    collector.collections['config'] = CollectionJSON(collector, _fake_config)
    return collector


def _persisted_params(collector):
    return json.loads(collector.collections['config'].data)['billing_provider_params']


class TestS3BillingParams:
    def test_only_ship_path_is_persisted(self):
        collector = _make_collector(S3_PARAMS)

        assert collector._gather_config() is True

        persisted = _persisted_params(collector)
        assert persisted == {'ship_path': S3_PARAMS['ship_path']}
        for key in ('bucket_name', 'bucket_endpoint', 'bucket_region', 'bucket_access_key', 'bucket_secret_key'):
            assert key not in persisted

    def test_credentials_absent_from_serialized_config(self):
        collector = _make_collector(S3_PARAMS)

        collector._gather_config()

        config_data = collector.collections['config'].data
        assert S3_PARAMS['bucket_access_key'] not in config_data
        assert S3_PARAMS['bucket_secret_key'] not in config_data

    def test_other_config_keys_are_preserved(self):
        collector = _make_collector(S3_PARAMS)

        collector._gather_config()

        assert json.loads(collector.collections['config'].data)['install_uuid'] == 'test-install-uuid'

    def test_billing_provider_params_remain_intact_for_package_s3(self):
        original = dict(S3_PARAMS)
        collector = _make_collector(S3_PARAMS)

        collector._gather_config()

        assert collector.billing_provider_params == original
        package = PackageS3(collector)
        assert package.collector.billing_provider_params['bucket_access_key'] == S3_PARAMS['bucket_access_key']
        assert package.collector.billing_provider_params['bucket_secret_key'] == S3_PARAMS['bucket_secret_key']


class TestCRCBillingParams:
    def test_billing_ids_preserved_and_candlepin_pems_stripped(self):
        collector = _make_collector(CRC_PARAMS, ship_target='crc')

        assert collector._gather_config() is True

        persisted = _persisted_params(collector)
        assert persisted == {
            'billing_provider': CRC_PARAMS['billing_provider'],
            'billing_account_id': CRC_PARAMS['billing_account_id'],
            'red_hat_org_id': CRC_PARAMS['red_hat_org_id'],
        }
        assert 'candlepin_cert_pem' not in persisted
        assert 'candlepin_key_pem' not in persisted

    def test_billing_provider_params_remain_intact(self):
        original = dict(CRC_PARAMS)
        collector = _make_collector(CRC_PARAMS, ship_target='crc')

        collector._gather_config()

        assert collector.billing_provider_params == original
        assert collector.billing_provider_params['candlepin_cert_pem'] == CRC_PARAMS['candlepin_cert_pem']
        assert collector.billing_provider_params['candlepin_key_pem'] == CRC_PARAMS['candlepin_key_pem']


class TestDirectoryBillingParams:
    def test_ship_path_is_preserved(self):
        params = {'ship_path': './metrics_utility/test/test_data'}
        collector = _make_collector(params, ship_target='directory')

        assert collector._gather_config() is True

        assert _persisted_params(collector) == params


class TestFailClosedAllowlist:
    def test_unknown_and_future_secret_keys_are_excluded(self):
        params = {
            'ship_path': 'somewhere',
            'bucket_session_token': 'session-token',
            'custom_secret': 'custom-secret',
        }
        collector = _make_collector(params)

        collector._gather_config()

        assert _persisted_params(collector) == {'ship_path': 'somewhere'}

    @pytest.mark.parametrize('params', [None, {}])
    def test_missing_or_empty_params_produce_empty_dict(self, params):
        collector = _make_collector(params)

        assert collector._gather_config() is True

        assert _persisted_params(collector) == {}
        assert collector.billing_provider_params == params

    def test_returns_false_when_base_gather_config_fails(self):
        collector = _make_collector(S3_PARAMS)

        with patch.object(BaseCollector, '_gather_config', return_value=False):
            assert collector._gather_config() is False

        assert collector.collections['config'].data is None


class TestConfigArchive:
    def test_s3_credentials_do_not_appear_in_config_json_inside_tarball(self):
        collector = _make_collector(S3_PARAMS)
        assert collector._gather_config() is True
        package = PackageS3(collector)

        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w:gz') as tar:
            assert package._config_to_tar(tar) is True

        archive.seek(0)
        with tarfile.open(fileobj=archive, mode='r:gz') as tar:
            member = next(m for m in tar.getmembers() if m.name.endswith('config.json'))
            content = tar.extractfile(member).read().decode('utf-8')

        assert json.loads(content)['billing_provider_params'] == {'ship_path': S3_PARAMS['ship_path']}
        assert S3_PARAMS['bucket_access_key'] not in content
        assert S3_PARAMS['bucket_secret_key'] not in content
