from metrics_utility.automation_controller_billing.package.package_crc import PackageCRC
from metrics_utility.automation_controller_billing.package.package_directory import PackageDirectory

class Factory:
    def __init__(self, ship_target):
        self.ship_target = ship_target

    def create(self):
        if self.ship_target == "crc":
            return PackageCRC
        elif self.ship_target == "directory":
            return PackageDirectory
