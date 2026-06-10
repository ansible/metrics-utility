"""
Unit tests for the candlepin_manage management command.

All store access and Candlepin API calls are mocked.
The Command class is instantiated directly rather than via call_command
because metrics_utility is not in INSTALLED_APPS.
"""

from io import StringIO
from unittest.mock import MagicMock, patch

from metrics_utility.management.commands.candlepin_manage import Command
from metrics_utility.management.validation import CANDLEPIN_UUID_PLACEHOLDER


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

SAMPLE_CERT_PEM = '-----BEGIN CERTIFICATE-----\nMIIBtest==\n-----END CERTIFICATE-----\n'
SAMPLE_KEY_PEM = '-----BEGIN RSA PRIVATE KEY-----\nMIIEtest==\n-----END RSA PRIVATE KEY-----\n'
SAMPLE_NEW_CERT = '-----BEGIN CERTIFICATE-----\nnewcert==\n-----END CERTIFICATE-----\n'
SAMPLE_NEW_KEY = '-----BEGIN RSA PRIVATE KEY-----\nnewkey==\n-----END RSA PRIVATE KEY-----\n'
CONSUMER_UUID = 'aaaabbbb-cccc-dddd-eeee-ffffffffffff'
SAMPLE_ORG = '1234567'
SAMPLE_USERNAME = 'rh-user@example.com'
SAMPLE_PASSWORD = 'secret'

SAMPLE_CERT_INFO = {
    'serial': '9999',
    'cn': 'test-consumer',
    'not_after': '2027-01-01T00:00:00+00:00',
    'days_remaining': 365,
    'not_before': '2026-01-01T00:00:00+00:00',
    'validity_days': 365,
    'issuer_cn': 'Red Hat CA',
    'issuer_org': 'Red Hat',
}
SAMPLE_NEW_CERT_INFO = {**SAMPLE_CERT_INFO, 'serial': '10000', 'days_remaining': 365}


def _make_mock_store(cert_pem=None, key_pem=None, uuid=None):
    """Return a MagicMock store whose .load() returns the given triple."""
    store = MagicMock()
    store.load.return_value = (cert_pem, key_pem, uuid)
    store.save_registration.return_value = True
    store.save_cert.return_value = True
    return store


def _run(subcommand, *extra_args, **kwargs):
    """Invoke Command.handle() directly, return (stdout, stderr, exit_code)."""
    out = StringIO()
    err = StringIO()
    cmd = Command(stdout=out, stderr=err)

    defaults = {
        'subcommand': subcommand,
        'dry_run': kwargs.pop('dry_run', False),
        'force': kwargs.pop('force', False),
        'username': kwargs.pop('username', None),
        'password': kwargs.pop('password', None),
        'org': kwargs.pop('org', None),
        'candlepin_url': kwargs.pop('candlepin_url', None),
        'candlepin_ca': kwargs.pop('candlepin_ca', None),
        'proxy': kwargs.pop('proxy', None),
    }
    defaults.update(kwargs)

    exit_code = 0
    try:
        cmd.handle(**defaults)
    except SystemExit as e:
        exit_code = e.code

    return out.getvalue(), err.getvalue(), exit_code


# ---------------------------------------------------------------------------
# register subcommand
# ---------------------------------------------------------------------------


class TestRegisterSubcommand:
    def test_registers_and_prints_cert_info(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', SAMPLE_ORG)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(None, None, None)):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.register_consumer.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
                    with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                        stdout, _, exit_code = _run('register', username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, org=SAMPLE_ORG)

        assert exit_code == 0
        assert CONSUMER_UUID in stdout
        assert SAMPLE_CERT_INFO['serial'] in stdout

    def test_saves_via_store_on_success(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', SAMPLE_ORG)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(None, None, None)):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.register_consumer.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
                    with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                        _run('register', username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, org=SAMPLE_ORG)

        mock_store.save_registration.assert_called_once_with(SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)

    def test_dry_run_skips_save(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', SAMPLE_ORG)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(None, None, None)):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.register_consumer.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
                    with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                        stdout, _, exit_code = _run('register', username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, org=SAMPLE_ORG, dry_run=True)

        mock_store.save_registration.assert_not_called()
        assert exit_code == 0
        assert 'dry-run' in stdout

    def test_skips_if_cert_exists_without_force(self, monkeypatch):
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                stdout, _, exit_code = _run('register', username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, org=SAMPLE_ORG)

        MockClient.return_value.register_consumer.assert_not_called()
        assert exit_code == 0
        assert '--force' in stdout

    def test_force_re_registers_when_cert_exists(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', SAMPLE_ORG)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(None, None, None)):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.register_consumer.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY, CONSUMER_UUID)
                    with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_NEW_CERT_INFO):
                        _, _, exit_code = _run('register', username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD, org=SAMPLE_ORG, force=True)

        MockClient.return_value.register_consumer.assert_called_once()
        assert exit_code == 0

    def test_discovers_org_when_no_org_flag_or_env(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(None, None, None)):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.discover_org.return_value = SAMPLE_ORG
                    MockClient.return_value.register_consumer.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
                    with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                        _, _, exit_code = _run('register', username=SAMPLE_USERNAME, password=SAMPLE_PASSWORD)

        MockClient.return_value.discover_org.assert_called_once()
        assert exit_code == 0

    def test_reads_credentials_from_env_when_no_cli_args(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_RH_USERNAME', SAMPLE_USERNAME)
        monkeypatch.setenv('METRICS_UTILITY_RH_PASSWORD', SAMPLE_PASSWORD)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', SAMPLE_ORG)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                MockClient.return_value.register_consumer.return_value = (SAMPLE_CERT_PEM, SAMPLE_KEY_PEM, CONSUMER_UUID)
                with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                    _, _, exit_code = _run('register')

        assert exit_code == 0
        MockClient.return_value.register_consumer.assert_called_once()

    def test_exits_nonzero_when_username_missing(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_RH_USERNAME', raising=False)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch(
                'metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(None, SAMPLE_PASSWORD, None)
            ):
                _, stderr, exit_code = _run('register')

        assert exit_code != 0
        assert 'username' in stderr

    def test_exits_nonzero_when_password_missing(self, monkeypatch):
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch(
                'metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials', return_value=(SAMPLE_USERNAME, None, None)
            ):
                _, stderr, exit_code = _run('register')

        assert exit_code != 0
        assert 'password' in stderr

    def test_exits_nonzero_when_org_discovery_fails(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.delenv('METRICS_UTILITY_CANDLEPIN_ORG', raising=False)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch(
                'metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials',
                return_value=(SAMPLE_USERNAME, SAMPLE_PASSWORD, None),
            ):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.discover_org.return_value = None
                    _, stderr, exit_code = _run('register')

        assert exit_code != 0
        assert 'org' in stderr.lower()

    def test_exits_nonzero_when_api_fails(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        monkeypatch.setenv('METRICS_UTILITY_CANDLEPIN_ORG', SAMPLE_ORG)
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch(
                'metrics_utility.management.commands.candlepin_manage._resolve_registration_credentials',
                return_value=(SAMPLE_USERNAME, SAMPLE_PASSWORD, None),
            ):
                with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                    MockClient.return_value.register_consumer.side_effect = RuntimeError('Candlepin down')
                    _, stderr, exit_code = _run('register')

        assert exit_code != 0
        assert 'failed' in stderr.lower()


# ---------------------------------------------------------------------------
# renew subcommand
# ---------------------------------------------------------------------------


class TestRenewSubcommand:
    def test_checkin_and_no_renewal_when_cert_healthy(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                with patch('metrics_utility.management.commands.candlepin_manage.needs_renewal', return_value=False):
                    with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                        stdout, _, exit_code = _run('renew')

        MockClient.return_value.checkin.assert_called_once()
        MockClient.return_value.regenerate_cert.assert_not_called()
        assert exit_code == 0
        assert 'No renewal needed' in stdout

    def test_renews_when_cert_near_expiry(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', side_effect=[SAMPLE_CERT_INFO, SAMPLE_NEW_CERT_INFO]):
                with patch('metrics_utility.management.commands.candlepin_manage.needs_renewal', return_value=True):
                    with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                        MockClient.return_value.regenerate_cert.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
                        stdout, _, exit_code = _run('renew')

        MockClient.return_value.regenerate_cert.assert_called_once()
        mock_store.save_cert.assert_called_once_with(SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
        assert exit_code == 0
        assert 'renewed' in stdout.lower()

    def test_force_renews_even_when_cert_healthy(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', side_effect=[SAMPLE_CERT_INFO, SAMPLE_NEW_CERT_INFO]):
                with patch('metrics_utility.management.commands.candlepin_manage.needs_renewal', return_value=False):
                    with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                        MockClient.return_value.regenerate_cert.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
                        stdout, _, exit_code = _run('renew', force=True)

        MockClient.return_value.regenerate_cert.assert_called_once()
        assert exit_code == 0
        assert 'forced' in stdout

    def test_dry_run_skips_save_on_renewal(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', side_effect=[SAMPLE_CERT_INFO, SAMPLE_NEW_CERT_INFO]):
                with patch('metrics_utility.management.commands.candlepin_manage.needs_renewal', return_value=True):
                    with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                        MockClient.return_value.regenerate_cert.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
                        stdout, _, exit_code = _run('renew', dry_run=True)

        mock_store.save_cert.assert_not_called()
        assert exit_code == 0
        assert 'dry-run' in stdout

    def test_exits_nonzero_when_no_cert_in_store(self):
        mock_store = _make_mock_store()

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            _, stderr, exit_code = _run('renew')

        assert exit_code != 0
        assert 'register' in stderr

    def test_exits_nonzero_when_uuid_is_placeholder(self):
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CANDLEPIN_UUID_PLACEHOLDER)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                _, stderr, exit_code = _run('renew')

        assert exit_code != 0
        assert 'register' in stderr

    def test_exits_nonzero_when_renewal_api_fails(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', return_value=SAMPLE_CERT_INFO):
                with patch('metrics_utility.management.commands.candlepin_manage.needs_renewal', return_value=True):
                    with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                        MockClient.return_value.regenerate_cert.side_effect = RuntimeError('Candlepin down')
                        _, stderr, exit_code = _run('renew')

        assert exit_code != 0
        assert 'failed' in stderr.lower()

    def test_prints_old_and_new_serial_on_renewal(self, monkeypatch):
        monkeypatch.delenv('METRICS_UTILITY_PROXY_URL', raising=False)
        mock_store = _make_mock_store(cert_pem=SAMPLE_CERT_PEM, key_pem=SAMPLE_KEY_PEM, uuid=CONSUMER_UUID)

        with patch('metrics_utility.management.commands.candlepin_manage.get_candlepin_store', return_value=mock_store):
            with patch('metrics_utility.management.commands.candlepin_manage.parse_cert', side_effect=[SAMPLE_CERT_INFO, SAMPLE_NEW_CERT_INFO]):
                with patch('metrics_utility.management.commands.candlepin_manage.needs_renewal', return_value=True):
                    with patch('metrics_utility.management.commands.candlepin_manage.CandlepinClient') as MockClient:
                        MockClient.return_value.regenerate_cert.return_value = (SAMPLE_NEW_CERT, SAMPLE_NEW_KEY)
                        stdout, _, _ = _run('renew')

        assert SAMPLE_CERT_INFO['serial'] in stdout
        assert SAMPLE_NEW_CERT_INFO['serial'] in stdout
