# Cleanup worker for eg. reports:

from TODO import SETTINGS

from metrics_utility import library


worker_key = 'cleanup-ccsp'

s3_storage = library.storage.StorageS3(SETTINGS)

until = library.instants.months_ago(SETTINGS.retention_months)

files = s3_storage.glob(glob='CCSP*.xlsx', until=until)

s3_storage.remove(files)
