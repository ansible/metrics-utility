from metrics_utility.automation_controller_billing.extract.extractor_directory import ExtractorDirectory
from metrics_utility.automation_controller_billing.extract.extractor_controller_db import ExtractorControllerDB

class Factory:
    def __init__(self, ship_target, extra_params):
        self.ship_target = ship_target
        self.extra_params = extra_params

    def create(self):
        if self.ship_target == "directory":
            return self._get_extractor_directory()
        elif self.ship_target == "controller_db":
            return self._get_extractor_controller_db()

    def _get_extractor_directory(self):
        # Return default S3 loader
        return ExtractorDirectory(self.extra_params)

    def _get_extractor_controller_db(self):
        # Return default S3 loader
        return ExtractorControllerDB(self.extra_params)
