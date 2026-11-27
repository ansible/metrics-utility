"""Tests for the shared PackageBase used by the directory and S3 packages."""

import datetime
import os

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from django.conf import settings

from metrics_utility import base
from metrics_utility.automation_controller_billing.package.package_base import PackageBase
from metrics_utility.automation_controller_billing.package.package_directory import PackageDirectory
from metrics_utility.automation_controller_billing.package.package_s3 import PackageS3


SINCE = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
UNTIL = datetime.datetime(2024, 1, 2, 4, 5, 6, tzinfo=datetime.UTC)


def _make_collector(ship_path='/tmp/ship'):
    collector = MagicMock()
    collector.billing_provider_params = {'ship_path': ship_path}
    return collector


def _make_package(cls=PackageBase, ship_path='/tmp/ship'):
    package = cls(_make_collector(ship_path))
    package.collections = [
        SimpleNamespace(since=SINCE, until=UNTIL),
        SimpleNamespace(since=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), until=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)),
    ]
    return package


class TestSharedMethods:
    def test_batch_since_and_until_uses_first_collection(self):
        assert _make_package()._batch_since_and_until() == (SINCE, UNTIL)

    def test_tarname_base(self, monkeypatch):
        monkeypatch.setattr(settings, 'INSTALL_UUID', 'test-uuid')
        assert _make_package()._tarname_base() == 'test-uuid-2024-01-02-030405+0000-2024-01-02-040506+0000'

    def test_destination_path_partitions_by_date(self):
        timestamp = datetime.datetime(2024, 3, 7, tzinfo=datetime.UTC)
        expected = os.path.join('/ship', 'data/2024/03/07', 'billing.tar.gz')
        assert _make_package()._destination_path('/ship', timestamp, 'billing.tar.gz') == expected


class TestIsShippingConfigured:
    def test_returns_false_when_tar_path_is_missing(self):
        assert _make_package().is_shipping_configured() is False

    def test_returns_false_when_tar_file_does_not_exist(self, tmp_path):
        package = _make_package()
        package.tar_path = str(tmp_path / 'missing.tar.gz')
        assert package.is_shipping_configured() is False

    def test_returns_true_when_tar_file_exists(self, tmp_path):
        tar = tmp_path / 'billing.tar.gz'
        tar.write_text('data')
        package = _make_package()
        package.tar_path = str(tar)
        assert package.is_shipping_configured() is True

    def test_returns_false_when_tar_path_contains_error(self, monkeypatch):
        package = _make_package()
        package.tar_path = 'Error: broken.tar.gz'
        monkeypatch.setattr(os.path, 'exists', lambda _path: True)
        assert package.is_shipping_configured() is False


class TestPackageHierarchy:
    @pytest.mark.parametrize('cls', [PackageDirectory, PackageS3])
    def test_subclasses_inherit_package_base(self, cls):
        assert issubclass(cls, PackageBase)
        assert issubclass(cls, base.Package)

    @pytest.mark.parametrize('method', ['_batch_since_and_until', '_tarname_base', 'is_shipping_configured', '_destination_path'])
    @pytest.mark.parametrize('cls', [PackageDirectory, PackageS3])
    def test_shared_methods_come_from_package_base(self, cls, method):
        assert getattr(cls, method) is getattr(PackageBase, method)

    def test_s3_ship_docstring_describes_s3(self):
        assert 'S3' in PackageS3.ship.__doc__


class TestShip:
    def test_directory_ship_copies_tarball(self, tmp_path):
        tar = tmp_path / 'billing.tar.gz'
        tar.write_text('data')
        ship_path = str(tmp_path / 'ship')
        package = _make_package(PackageDirectory, ship_path)
        package.tar_path = str(tar)

        assert package.ship() is True
        assert package.shipping_successful is True
        assert (tmp_path / 'ship' / 'data' / '2024' / '01' / '02' / 'billing.tar.gz').read_text() == 'data'

    def test_s3_ship_uploads_to_date_partitioned_key(self, tmp_path, monkeypatch):
        tar = tmp_path / 'billing.tar.gz'
        tar.write_text('data')
        ship_path = str(tmp_path / 'ship')
        package = _make_package(PackageS3, ship_path)
        package.tar_path = str(tar)

        s3_handler = MagicMock()
        monkeypatch.setattr('metrics_utility.automation_controller_billing.package.package_s3.S3Handler', MagicMock(return_value=s3_handler))

        assert package.ship() is True
        assert package.shipping_successful is True
        s3_handler.upload_file.assert_called_once_with(str(tar), object_name=os.path.join(ship_path, 'data/2024/01/02', 'billing.tar.gz'))
