"""Shared base for package implementations that ship to local storage (directory or S3)."""

import os

from django.conf import settings

from metrics_utility.gather.package.package import Package
from metrics_utility.logger import logger


class PackageLocal(Package):
    """Base for packages that ship tarballs to a date-partitioned local or S3 destination."""

    def _batch_since_and_until(self):
        return self.collections[0].since, self.collections[0].until

    def _tarname_base(self):
        since, until = self._batch_since_and_until()
        return f'{settings.INSTALL_UUID}-{since.strftime("%Y-%m-%d-%H%M%S%z")}-{until.strftime("%Y-%m-%d-%H%M%S%z")}'

    def is_shipping_configured(self):
        if not self.tar_path:
            logger.error('Insights for Ansible Automation Platform TAR not found')
            return False

        if not os.path.exists(self.tar_path):
            logger.error(f'Insights for Ansible Automation Platform TAR {self.tar_path} not found')
            return False

        if 'Error:' in str(self.tar_path):
            return False

        return True

    def _destination_path(self, base_path, timestamp, filename):
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        day = timestamp.strftime('%d')

        path = f'data/{year}/{month}/{day}'

        return os.path.join(base_path, path, filename)

    def ship(self):
        if not self.is_shipping_configured():
            self.shipping_successful = False
            return False

        logger.debug(f'shipping analytics file: {self.tar_path}')

        since, _ = self._batch_since_and_until()
        destination_path = self._destination_path(self.collector.billing_provider_params['ship_path'], since, os.path.basename(self.tar_path))

        self._do_ship(destination_path)

        logger.debug(f'tarball saved to: {destination_path}')

        self.shipping_successful = True
        return True

    def _do_ship(self, destination_path):
        raise NotImplementedError
