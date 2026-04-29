"""Factory for selecting the correct Package class for the configured ship target."""

from metrics_utility.automation_controller_billing.package.package_crc import PackageCRC
from metrics_utility.automation_controller_billing.package.package_directory import PackageDirectory
from metrics_utility.automation_controller_billing.package.package_s3 import PackageS3
from metrics_utility.exceptions import NotSupportedFactory


class Factory:
    """Returns the Package *class* (not instance) appropriate for *ship_target*."""

    def __init__(self, ship_target):
        """Initialise the package factory.

        Args:
            ship_target: One of ``'crc'``, ``'directory'``, or ``'s3'``.
        """
        self.ship_target = ship_target

    def create(self):
        """Return the Package class for the configured ship target.

        Returns:
            A :class:`~metrics_utility.base.package.Package` subclass (uninstantiated).

        Raises:
            :exc:`~metrics_utility.exceptions.NotSupportedFactory`: For unknown
                ship targets.
        """
        if self.ship_target == 'crc':
            return PackageCRC
        elif self.ship_target == 'directory':
            return PackageDirectory
        elif self.ship_target == 's3':
            return PackageS3
        else:
            raise NotSupportedFactory(f'Factory for {self.ship_target} not supported')
