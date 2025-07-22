from unittest.mock import patch

import pytest

from metrics_utility.exceptions import BadShipTarget, MissingRequiredEnvVar
from metrics_utility.test.util import run_build_int, run_gather_int


unset = {
    'METRICS_UTILITY_BUCKET_ACCESS_KEY': None,
    'METRICS_UTILITY_BUCKET_ENDPOINT': None,
    'METRICS_UTILITY_BUCKET_NAME': None,
    'METRICS_UTILITY_BUCKET_REGION': None,
    'METRICS_UTILITY_BUCKET_SECRET_KEY': None,
}


# workaround, until we merge env var handling between _handle_* and handle_env_validation
# this test was written in a world without handle_env_validation, mocking it out
@patch('metrics_utility.management.commands.build_report.handle_env_validation')
def expect_build_error(env, klass, mocked):
    mocked.return_value = None

    with pytest.raises(klass) as e:
        run_build_int(
            {**unset, **env},
            {
                'since': '2022-01-01',
            },
        )
    return e.value


@patch('metrics_utility.management.commands.gather_automation_controller_billing_data.handle_env_validation')
def expect_gather_error(env, klass, mocked):
    mocked.return_value = None

    with pytest.raises(klass) as e:
        run_gather_int(
            {**unset, **env},
            {
                'dry-run': True,
            },
        )
    return e.value


def test_build_bad_target():
    e = expect_build_error(
        {
            'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
            'METRICS_UTILITY_SHIP_TARGET': 'crc',
        },
        BadShipTarget,
    )
    assert e.name == 'Unexpected value for METRICS_UTILITY_SHIP_TARGET env var (crc), allowed values: controller_db, directory, s3'


def test_gather_bad_target():
    e = expect_gather_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 'controller_db',
        },
        BadShipTarget,
    )
    assert e.name == 'Unexpected value for METRICS_UTILITY_SHIP_TARGET env var (controller_db), allowed values: crc, directory, s3'


def test_build_controller_db():
    e = expect_build_error(
        {
            'METRICS_UTILITY_REPORT_TYPE': 'RENEWAL_GUIDANCE',
            'METRICS_UTILITY_SHIP_TARGET': 'controller_db',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_SHIP_PATH - place for collected data and built reports'

    e = expect_build_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 'controller_db',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_REPORT_TYPE.'


def test_gather_crc(caplog):
    run_gather_int(
        {
            **unset,
            'METRICS_UTILITY_SHIP_TARGET': 'crc',
            'METRICS_UTILITY_BILLING_PROVIDER': 'aws',
            'METRICS_UTILITY_BILLING_ACCOUNT_ID': '123456789012',
            'METRICS_UTILITY_SHIP_PATH': 'unexpected',
        },
        {
            'dry-run': True,
        },
    )
    assert caplog.messages[0] == 'Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="controller_db", "directory", "s3"'


def test_build_directory(caplog):
    e = expect_build_error(
        {
            'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
            'METRICS_UTILITY_SHIP_TARGET': 'directory',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_SHIP_PATH - place for collected data and built reports'

    e = expect_build_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 'directory',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'unexpected',
            'METRICS_UTILITY_BILLING_PROVIDER': 'unexpected',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_REPORT_TYPE.'
    assert caplog.messages[-1] == 'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="s3": METRICS_UTILITY_BUCKET_NAME'
    assert caplog.messages[-2] == 'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="crc": METRICS_UTILITY_BILLING_PROVIDER'


def test_gather_directory():
    e = expect_gather_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 'directory',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_SHIP_PATH - place for collected data and built reports'

    run_gather_int(
        {
            **unset,
            'METRICS_UTILITY_SHIP_TARGET': 'directory',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
        },
        {
            'dry-run': True,
        },
    )


def test_build_s3():
    e = expect_build_error(
        {
            'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
            'METRICS_UTILITY_SHIP_TARGET': 's3',
        },
        MissingRequiredEnvVar,
    )
    assert (
        e.name == 'Missing some required env variables for S3 configuration, namely: '
        'METRICS_UTILITY_BUCKET_NAME - name of S3 bucket, '
        'METRICS_UTILITY_BUCKET_ENDPOINT - S3 endpoint, eg. https://s3.us-east.example.com, '
        'METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key, '
        'METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key, '
        'METRICS_UTILITY_SHIP_PATH - place for collected data and built reports.'
    )

    e = expect_build_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
            'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'S3 access key',
            'METRICS_UTILITY_BUCKET_SECRET_KEY': 'S3 secret key',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_REPORT_TYPE.'

    e = expect_build_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
            'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'S3 access key',
            'METRICS_UTILITY_BUCKET_SECRET_KEY': 'S3 secret key',
            'METRICS_UTILITY_BUCKET_REGION': 'optional',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_REPORT_TYPE.'


def test_gather_s3():
    e = expect_gather_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 's3',
        },
        MissingRequiredEnvVar,
    )
    assert (
        e.name == 'Missing some required env variables for S3 configuration, namely: '
        'METRICS_UTILITY_BUCKET_NAME - name of S3 bucket, '
        'METRICS_UTILITY_BUCKET_ENDPOINT - S3 endpoint, eg. https://s3.us-east.example.com, '
        'METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key, '
        'METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key, '
        'METRICS_UTILITY_SHIP_PATH - place for collected data and built reports.'
    )

    run_gather_int(
        {
            **unset,
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
            'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'S3 access key',
            'METRICS_UTILITY_BUCKET_SECRET_KEY': 'S3 secret key',
        },
        {
            'dry-run': True,
        },
    )

    run_gather_int(
        {
            **unset,
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
            'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'S3 access key',
            'METRICS_UTILITY_BUCKET_SECRET_KEY': 'S3 secret key',
            'METRICS_UTILITY_BUCKET_REGION': 'optional',
        },
        {
            'dry-run': True,
        },
    )
