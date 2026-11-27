"""Package implementation that ships billing tarballs to a local directory."""

import os
import shutil

from metrics_utility.automation_controller_billing.package.package_base import PackageBase
from metrics_utility.logger import logger


class PackageDirectory(PackageBase):
    """Package that copies the generated tarball into a local date-partitioned directory."""

    def ship(self):
        """
        Ship gathered metrics to the Directory
        """
        if not self.is_shipping_configured():
            self.shipping_successful = False
            return False

        logger.debug(f'shipping analytics file: {self.tar_path}')

        since, _ = self._batch_since_and_until()
        destination_path = self._destination_path(self.collector.billing_provider_params['ship_path'], since, os.path.basename(self.tar_path))

        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        shutil.copyfile(self.tar_path, destination_path)

        logger.debug(f'tarball saved to: {destination_path}')

        self.shipping_successful = True
        return True
