"""CRC ship target: billing_provider_params built from env and passed into config.json."""

from unittest.mock import patch

import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.validation import handle_crc_ship_target


@patch('metrics_utility.management.validation._fetch_candlepin_cert_from_db', return_value=(None, None))
def test_handle_crc_ship_target_aws_billing_params(_mock_fetch, monkeypatch):
    """CRC + AWS env produces the billing triplet that gather embeds in config.json."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
    monkeypatch.setenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', '123456789012')
    monkeypatch.setenv('METRICS_UTILITY_RED_HAT_ORG_ID', '99900001')
    result = handle_crc_ship_target()
    assert result == {
        'billing_provider': 'aws',
        'billing_account_id': '123456789012',
        'red_hat_org_id': '99900001',
    }


@patch('metrics_utility.management.validation._fetch_candlepin_cert_from_db', return_value=(None, None))
def test_handle_crc_ship_target_aws_requires_billing_account_id(_mock_fetch, monkeypatch):
    """CRC + AWS raises MissingRequiredEnvVar when billing account ID env var is absent."""
    monkeypatch.setenv('METRICS_UTILITY_BILLING_PROVIDER', 'aws')
    monkeypatch.delenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', raising=False)
    with pytest.raises(MissingRequiredEnvVar, match='METRICS_UTILITY_BILLING_ACCOUNT_ID'):
        handle_crc_ship_target()
