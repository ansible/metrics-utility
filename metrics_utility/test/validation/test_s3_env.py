from unittest.mock import patch

import pytest

from botocore.exceptions import ClientError

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler
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


def test_s3handler_rejects_mismatched_credentials():
    """S3Handler should reject a one-sided credential pair at construction time."""
    with pytest.raises(ValueError, match='must both be provided or both be omitted'):
        S3Handler({'bucket_access_key': 'AKIA_TEST'})

    with pytest.raises(ValueError, match='must both be provided or both be omitted'):
        S3Handler({'bucket_secret_key': 'secret123'})


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_implicit_credentials_not_found(mock_session_cls):
    """S3Handler should raise a clear error mentioning env vars when implicit credentials are absent."""
    mock_session = mock_session_cls.return_value
    mock_session.get_credentials.return_value = None

    handler = S3Handler({'bucket_region': 'us-east-1'})
    with pytest.raises(ValueError, match=r'METRICS_UTILITY_BUCKET_ACCESS_KEY.*METRICS_UTILITY_BUCKET_SECRET_KEY'):
        _ = handler.session


def _mocked_handler(mock_session_cls):
    """Build an S3Handler backed by a mocked boto3 Session and return (handler, session)."""
    session = mock_session_cls.return_value
    handler = S3Handler(
        {
            'bucket_name': 'test-bucket',
            'bucket_endpoint': 'https://s3.example.com',
            'bucket_region': 'us-east-1',
            'bucket_access_key': 'AKIA_TEST',
            'bucket_secret_key': 'secret123',
        }
    )
    return handler, session


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_client_is_cached(mock_session_cls):
    """get_s3_client() should construct the client once and reuse it."""
    handler, session = _mocked_handler(mock_session_cls)

    first = handler.get_s3_client()
    second = handler.get_s3_client()
    third = handler.get_s3_client()

    assert first is second is third
    session.client.assert_called_once_with('s3', endpoint_url='https://s3.example.com')


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_resource_is_cached(mock_session_cls):
    """get_s3_resource() should construct the resource once and reuse it."""
    handler, session = _mocked_handler(mock_session_cls)

    first = handler.get_s3_resource()
    second = handler.get_s3_resource()
    third = handler.get_s3_resource()

    assert first is second is third
    session.resource.assert_called_once_with('s3', endpoint_url='https://s3.example.com')


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_operations_share_cached_client(mock_session_cls, tmp_path):
    """list_files/download_file/upload_file should share one client and never build a resource."""
    handler, session = _mocked_handler(mock_session_cls)
    client = session.client.return_value

    paginator = client.get_paginator.return_value
    paginator.paginate.return_value = [
        {'Contents': [{'Key': 'data/a.csv'}]},
        {'Contents': [{'Key': 'data/b.csv'}]},
    ]

    local_file = tmp_path / 'out.csv'
    local_file.write_text('x')

    assert list(handler.list_files('data')) == ['data/a.csv', 'data/b.csv']
    assert list(handler.list_files('data')) == ['data/a.csv', 'data/b.csv']
    assert handler.download_file('data/a.csv', str(local_file)) is True
    assert handler.download_file('data/a.csv', str(local_file)) is True
    assert handler.upload_file(str(local_file), 'data/out.csv') is None
    assert handler.upload_file(str(local_file), 'data/out.csv') is None

    session.client.assert_called_once_with('s3', endpoint_url='https://s3.example.com')
    session.resource.assert_not_called()

    assert client.download_file.call_count == 2
    assert client.upload_file.call_count == 2
    client.upload_file.assert_called_with(str(local_file), 'test-bucket', 'data/out.csv')
    client.get_paginator.assert_called_with('list_objects')


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_download_file_404_returns_false(mock_session_cls, tmp_path):
    """download_file() should return False on a 404 ClientError, using the cached client."""
    handler, session = _mocked_handler(mock_session_cls)
    client = session.client.return_value
    client.download_file.side_effect = ClientError({'Error': {'Code': '404'}}, 'HeadObject')

    assert handler.download_file('missing.csv', str(tmp_path / 'missing.csv')) is False
    client.download_file.assert_called_once_with('test-bucket', 'missing.csv', str(tmp_path / 'missing.csv'))
    session.client.assert_called_once()


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_download_file_other_error_reraises(mock_session_cls, tmp_path):
    """download_file() should re-raise non-404 ClientErrors."""
    handler, session = _mocked_handler(mock_session_cls)
    client = session.client.return_value
    error = ClientError({'Error': {'Code': '403'}}, 'HeadObject')
    client.download_file.side_effect = error

    with pytest.raises(ClientError) as excinfo:
        handler.download_file('forbidden.csv', str(tmp_path / 'forbidden.csv'))
    assert excinfo.value is error


@patch('metrics_utility.automation_controller_billing.base.s3_handler.boto3.Session')
def test_s3handler_upload_file_client_error_returns_false(mock_session_cls, tmp_path, caplog):
    """upload_file() should log and return False on ClientError, using the cached client."""
    handler, session = _mocked_handler(mock_session_cls)
    client = session.client.return_value
    error = ClientError({'Error': {'Code': '403'}}, 'PutObject')
    client.upload_file.side_effect = error

    local_file = tmp_path / 'out.csv'
    local_file.write_text('x')

    assert handler.upload_file(str(local_file), 'data/out.csv') is False
    client.upload_file.assert_called_once_with(str(local_file), 'test-bucket', 'data/out.csv')
    session.client.assert_called_once()
    assert str(error) in caplog.text
