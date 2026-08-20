from unittest.mock import patch

import pytest

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler
from metrics_utility.exceptions import BadShipTarget, MissingRequiredEnvVar
from metrics_utility.library.storage import StorageS3
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
    assert 'METRICS_UTILITY_BUCKET_NAME - name of S3 bucket' in e.name
    assert 'METRICS_UTILITY_BUCKET_ENDPOINT' in e.name
    assert 'METRICS_UTILITY_SHIP_PATH' in e.name
    assert 'METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key' not in e.name
    assert 'METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key' not in e.name

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


def test_build_s3_implicit_credentials():
    """S3 without explicit credentials should pass validation (IRSA, instance profiles)."""
    e = expect_build_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
        },
        MissingRequiredEnvVar,
    )
    assert e.name == 'Missing required env variable METRICS_UTILITY_REPORT_TYPE.'


def test_build_s3_mismatched_credentials():
    """Setting only one of access_key/secret_key should fail."""
    e = expect_build_error(
        {
            'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
            'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'only-access-key',
        },
        MissingRequiredEnvVar,
    )
    assert 'must both be set or both be omitted' in e.name


def test_gather_s3():
    e = expect_gather_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 's3',
        },
        MissingRequiredEnvVar,
    )
    assert 'METRICS_UTILITY_BUCKET_NAME - name of S3 bucket' in e.name
    assert 'METRICS_UTILITY_BUCKET_ENDPOINT' in e.name
    assert 'METRICS_UTILITY_SHIP_PATH' in e.name
    assert 'METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key' not in e.name
    assert 'METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key' not in e.name

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


def test_gather_s3_implicit_credentials():
    """S3 without explicit credentials should pass validation (IRSA, instance profiles)."""
    run_gather_int(
        {
            **unset,
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
        },
        {
            'dry-run': True,
        },
    )


def test_gather_s3_mismatched_credentials():
    """Setting only one of access_key/secret_key should fail."""
    e = expect_gather_error(
        {
            'METRICS_UTILITY_SHIP_TARGET': 's3',
            'METRICS_UTILITY_SHIP_PATH': 'wherever',
            'METRICS_UTILITY_BUCKET_NAME': 'something',
            'METRICS_UTILITY_BUCKET_ENDPOINT': 'https://s3.us-east.example.com',
            'METRICS_UTILITY_BUCKET_SECRET_KEY': 'only-secret-key',
        },
        MissingRequiredEnvVar,
    )
    assert 'must both be set or both be omitted' in e.name


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_session_with_explicit_credentials(mock_session):
    """S3Handler should pass credentials to boto3.Session when both are provided."""
    handler = S3Handler(
        {
            'bucket_access_key': 'AKIA_TEST',
            'bucket_secret_key': 'secret123',
            'bucket_region': 'us-east-1',
        }
    )
    _ = handler.session
    mock_session.assert_called_once_with(
        region_name='us-east-1',
        aws_access_key_id='AKIA_TEST',
        aws_secret_access_key='secret123',
    )


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_session_with_implicit_credentials(mock_session):
    """S3Handler should not pass credentials to boto3.Session when both are absent."""
    handler = S3Handler(
        {
            'bucket_region': 'us-west-2',
        }
    )
    _ = handler.session
    mock_session.assert_called_once_with(region_name='us-west-2')


@patch('metrics_utility.library.storage.s3.boto3.Session')
def test_storage_s3_client_with_explicit_credentials(mock_session):
    """StorageS3 should pass credentials to boto3.Session when both are provided."""
    storage = StorageS3(
        bucket='test-bucket',
        endpoint='https://s3.example.com',
        region='eu-west-1',
        access_key='AKIA_TEST',
        secret_key='secret123',
    )
    _ = storage.client
    mock_session.assert_called_once_with(
        region_name='eu-west-1',
        aws_access_key_id='AKIA_TEST',
        aws_secret_access_key='secret123',
    )


@patch('metrics_utility.library.storage.s3.boto3.Session')
def test_storage_s3_client_with_implicit_credentials(mock_session):
    """StorageS3 should not pass credentials to boto3.Session when both are absent."""
    storage = StorageS3(
        bucket='test-bucket',
        endpoint='https://s3.example.com',
        region='ap-southeast-2',
    )
    _ = storage.client
    mock_session.assert_called_once_with(region_name='ap-southeast-2')


def test_s3handler_rejects_mismatched_credentials():
    """S3Handler should reject a one-sided credential pair at construction time."""
    with pytest.raises(ValueError, match='must both be provided or both be omitted'):
        S3Handler({'bucket_access_key': 'AKIA_TEST'})

    with pytest.raises(ValueError, match='must both be provided or both be omitted'):
        S3Handler({'bucket_secret_key': 'secret123'})


def test_storage_s3_rejects_mismatched_credentials():
    """StorageS3 should reject a one-sided credential pair at construction time."""
    with pytest.raises(ValueError, match='must both be provided or both be omitted'):
        StorageS3(bucket='test-bucket', access_key='AKIA_TEST')

    with pytest.raises(ValueError, match='must both be provided or both be omitted'):
        StorageS3(bucket='test-bucket', secret_key='secret123')


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_implicit_credentials_not_found(mock_session_cls):
    """S3Handler should raise a clear error mentioning env vars when implicit credentials are absent."""
    mock_session = mock_session_cls.return_value
    mock_session.get_credentials.return_value = None

    handler = S3Handler({'bucket_region': 'us-east-1'})
    with pytest.raises(ValueError, match=r'METRICS_UTILITY_BUCKET_ACCESS_KEY.*METRICS_UTILITY_BUCKET_SECRET_KEY'):
        _ = handler.session


@patch('metrics_utility.library.storage.s3.boto3.Session')
def test_storage_s3_implicit_credentials_not_found(mock_session_cls):
    """StorageS3 should raise a clear error mentioning env vars when implicit credentials are absent."""
    mock_session = mock_session_cls.return_value
    mock_session.get_credentials.return_value = None

    storage = StorageS3(bucket='test-bucket', endpoint='https://s3.example.com', region='us-east-1')
    with pytest.raises(ValueError, match=r'METRICS_UTILITY_BUCKET_ACCESS_KEY.*METRICS_UTILITY_BUCKET_SECRET_KEY'):
        _ = storage.client
