"""Package implementation that ships billing tarballs to an S3-compatible object store."""

import os

from metrics_utility.automation_controller_billing.base.s3_handler import S3Handler
from metrics_utility.automation_controller_billing.package.package_base import PackageBase
from metrics_utility.logger import logger


class PackageS3(PackageBase):
    """Package that uploads the generated tarball into a date-partitioned S3 prefix."""

    def ship(self):
        """
        Ship gathered metrics to S3
        """
        if not self.is_shipping_configured():
            self.shipping_successful = False
            return False

        logger.debug(f'shipping analytics file: {self.tar_path}')

        since, _ = self._batch_since_and_until()
        destination_path = self._destination_path(self.collector.billing_provider_params['ship_path'], since, os.path.basename(self.tar_path))

        s3_handler = S3Handler(params=self.collector.billing_provider_params)
        s3_handler.upload_file(self.tar_path, object_name=destination_path)

        logger.debug(f'tarball saved to: {destination_path}')

        self.shipping_successful = True
        return True
