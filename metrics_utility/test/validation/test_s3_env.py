import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.test.util import run_gather_int


unset = {
    'METRICS_UTILITY_BUCKET_ACCESS_KEY': None,
    'METRICS_UTILITY_BUCKET_ENDPOINT': None,
    'METRICS_UTILITY_BUCKET_NAME': None,
    'METRICS_UTILITY_BUCKET_REGION': None,
    'METRICS_UTILITY_BUCKET_SECRET_KEY': None,
}


def test_gather_bad_target():
    with pytest.raises(MissingRequiredEnvVar) as e:
        run_gather_int(
            {
                'METRICS_UTILITY_SHIP_TARGET': 'controller_db',
            },
            {
                'dry-run': True,
            },
        )
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET: controller_db' in e.value.name


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
    assert caplog.messages[0] == 'Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="directory", "s3"'


def test_gather_directory():
    with pytest.raises(MissingRequiredEnvVar) as e:
        run_gather_int(
            {
                **unset,
                'METRICS_UTILITY_SHIP_TARGET': 'directory',
            },
            {
                'dry-run': True,
            },
        )
    assert e.value.name == 'Missing required env variable METRICS_UTILITY_SHIP_PATH - place for collected data'

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


def test_gather_s3():
    with pytest.raises(MissingRequiredEnvVar) as e:
        run_gather_int(
            {
                **unset,
                'METRICS_UTILITY_SHIP_TARGET': 's3',
            },
            {
                'dry-run': True,
            },
        )
    assert (
        e.value.name == 'Missing some required env variables for S3 configuration, namely: '
        'METRICS_UTILITY_BUCKET_NAME - name of S3 bucket, '
        'METRICS_UTILITY_BUCKET_ENDPOINT - S3 endpoint, eg. https://s3.us-east.example.com, '
        'METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key, '
        'METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key, '
        'METRICS_UTILITY_SHIP_PATH - place for collected data.'
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
