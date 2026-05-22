"""CRC ship target: billing_provider_params built from env and passed into config.json."""

import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.commands.gather_automation_controller_billing_data import Command
from metrics_utility.test.util import temporary_env


def test_handle_crc_ship_target_aws_billing_params():
    """CRC + AWS env produces the billing triplet that gather embeds in config.json."""
    with temporary_env(
        {
            'METRICS_UTILITY_BILLING_PROVIDER': 'aws',
            'METRICS_UTILITY_BILLING_ACCOUNT_ID': '123456789012',
            'METRICS_UTILITY_RED_HAT_ORG_ID': '99900001',
        }
    ):
        assert Command._read_crc_env() == {
            'billing_provider': 'aws',
            'billing_account_id': '123456789012',
            'red_hat_org_id': '99900001',
        }


def test_handle_crc_ship_target_aws_requires_billing_account_id():
    """CRC + AWS raises MissingRequiredEnvVar when billing account ID env var is absent."""
    with temporary_env(
        {
            'METRICS_UTILITY_BILLING_PROVIDER': 'aws',
            'METRICS_UTILITY_BILLING_ACCOUNT_ID': None,
        }
    ):
        with pytest.raises(MissingRequiredEnvVar, match='METRICS_UTILITY_BILLING_ACCOUNT_ID'):
            Command._read_crc_env()


def test_handle_crc_ship_target_unsupported_provider_shows_value():
    """Unsupported provider error includes the actual value."""
    with temporary_env({'METRICS_UTILITY_BILLING_PROVIDER': 'gcp'}):
        with pytest.raises(MissingRequiredEnvVar, match="'gcp'"):
            Command._read_crc_env()


def test_handle_crc_ship_target_missing_provider():
    """Missing provider error shows None."""
    with temporary_env({'METRICS_UTILITY_BILLING_PROVIDER': None}):
        with pytest.raises(MissingRequiredEnvVar, match='None'):
            Command._read_crc_env()
