from metrics_utility.automation_controller_billing.package.package_crc import PackageCRC
from metrics_utility.automation_controller_billing.package.package_directory import PackageDirectory
from metrics_utility.automation_controller_billing.package.package_s3 import PackageS3
from metrics_utility.exceptions import NotSupportedFactory


class Factory:
    def __init__(self, ship_target):
        self.ship_target = ship_target

    def create(self):
        if self.ship_target == 'crc':
            return PackageCRC
        elif self.ship_target == 'directory':
            return PackageDirectory
        elif self.ship_target == 's3':
            return PackageS3
        else:
            raise NotSupportedFactory(f'Factory for {self.ship_target} not supported')
