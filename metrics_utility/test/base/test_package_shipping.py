import pytest

from metrics_utility.base import Collector, Package


@pytest.fixture
def package(tmp_path):
    collector = Collector()
    collector.last_gathered_entries = {}
    package = Package(collector)
    package.tar_path = tmp_path / 'collection.tar.gz'
    package.tar_path.touch()
    return package


@pytest.mark.parametrize(
    ('missing', 'error'),
    [
        ('get_s3_configured', 'S3 configuration is not set'),
        ('_get_rh_user', 'aws_access_key_id is not set'),
        ('_get_rh_password', 'aws_secret_access_key is not set'),
        ('_get_rh_region', 'aws_region is not set'),
        ('_get_rh_bucket', 'aws_bucket is not set'),
        (None, None),
    ],
)
def test_s3_shipping_requires_each_setting(package, mocker, caplog, missing, error):
    mocker.patch.object(package, 'shipping_auth_mode', return_value=Package.SHIPPING_AUTH_S3_USERPASS)
    for getter in ('get_s3_configured', '_get_rh_user', '_get_rh_password', '_get_rh_region', '_get_rh_bucket'):
        mocker.patch.object(package, getter, return_value=getter != missing)

    assert package.is_shipping_configured() is (missing is None)
    assert caplog.messages == ([error] if error else [])


def test_certificate_shipping_does_not_require_s3_settings(package, mocker):
    mocker.patch.object(package, 'shipping_auth_mode', return_value=Package.SHIPPING_AUTH_CERTIFICATES)

    assert package.is_shipping_configured() is True
