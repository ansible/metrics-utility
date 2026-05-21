"""CRC ship target: billing_provider_params built from env and passed into config.json."""

import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.validation import handle_crc_ship_target


def test_handle_crc_ship_target_aws_billing_params(monkeypatch):
    """CRC + AWS env produces the billing triplet that gather embeds in config.json."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
    monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
    monkeypatch.setenv('METRICS_UTILITY_RED_HAT_ORG_ID', '99900001')
    config_params, ship_params = handle_crc_ship_target()
    assert config_params == {
        'billing_provider': 'aws',
        'billing_account_id': '123456789012',
        'red_hat_org_id': '99900001',
    }
    assert ship_params == {}


def test_handle_crc_ship_target_aws_requires_billing_account_id(monkeypatch):
    """CRC + AWS raises MissingRequiredEnvVar when billing account ID env var is absent."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
    monkeypatch.delenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', raising=False)
    with pytest.raises(MissingRequiredEnvVar, match='METRICS_UTILITY_BILLING_ACCOUNT_ID'):
        handle_crc_ship_target()
