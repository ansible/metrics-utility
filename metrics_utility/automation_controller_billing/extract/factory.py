from metrics_utility.automation_controller_billing.extract.extractor_controller_db import ExtractorControllerDB
from metrics_utility.automation_controller_billing.extract.extractor_directory import ExtractorDirectory
from metrics_utility.automation_controller_billing.extract.extractor_s3 import ExtractorS3
from metrics_utility.exceptions import NotSupportedFactory


class Factory:
    def __init__(self, ship_target, extra_params):
        self.ship_target = ship_target
        self.extra_params = extra_params

    def create(self):
        if self.ship_target == 'directory':
            return self._get_extractor_directory()
        elif self.ship_target == 'controller_db':
            return self._get_extractor_controller_db()
        elif self.ship_target == 's3':
            return self._get_extractor_s3()
        else:
            raise NotSupportedFactory(f'Factory for {self.ship_target} not supported')

    def _get_extractor_directory(self):
        # Return default directory loader
        return ExtractorDirectory(self.extra_params)

    def _get_extractor_controller_db(self):
        # Return default DB loader
        return ExtractorControllerDB(self.extra_params)

    def _get_extractor_s3(self):
        # Return default S3 loader
        return ExtractorS3(self.extra_params)
