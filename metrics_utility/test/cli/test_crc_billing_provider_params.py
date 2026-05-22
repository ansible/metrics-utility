"""CRC ship target: billing_provider_params built from env and passed into config.json."""

import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.commands.gather_automation_controller_billing_data import Command


def test_handle_crc_ship_target_aws_billing_params(monkeypatch):
    """CRC + AWS env produces the billing triplet that gather embeds in config.json."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
    monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
    monkeypatch.setenv('METRICS_UTILITY_RED_HAT_ORG_ID', '99900001')
    assert Command._read_crc_env() == {
        'billing_provider': 'aws',
        'billing_account_id': '123456789012',
        'red_hat_org_id': '99900001',
    }


def test_handle_crc_ship_target_aws_requires_billing_account_id(monkeypatch):
    """CRC + AWS raises MissingRequiredEnvVar when billing account ID env var is absent."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
    monkeypatch.delenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', raising=False)
    with pytest.raises(MissingRequiredEnvVar, match='METRICS_UTILITY_BILLING_ACCOUNT_ID'):
        Command._read_crc_env()


def test_handle_crc_ship_target_unsupported_provider_shows_value(monkeypatch):
    """Unsupported provider error includes the actual value."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'gcp')
    with pytest.raises(MissingRequiredEnvVar, match="'gcp'"):
        Command._read_crc_env()


def test_handle_crc_ship_target_missing_provider(monkeypatch):
    """Missing provider error shows None."""
    monkeypatch.delenv('METRICS_UTILITY_BILLING_PROVIDER', raising=False)
    with pytest.raises(MissingRequiredEnvVar, match='None'):
        Command._read_crc_env()
