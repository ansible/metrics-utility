"""Shared base for packages that ship billing tarballs into a date-partitioned location."""

import os

from django.conf import settings

from metrics_utility import base
from metrics_utility.logger import logger


class PackageBase(base.Package):
    """Package base with shared tarball naming and date-partitioned destination logic."""

    def _batch_since_and_until(self):
        # TODO: how to verify this is the daily batch of job_host_summary?
        # self.collection_keys is: ['job_host_summary', 'manifest']
        # So we can take this and acess cherrypicked collections, if we need to
        # collect more collections in the future
        # But the main collection should always be first.

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
