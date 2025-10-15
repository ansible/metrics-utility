# Cleanup worker for eg. reports:

from settings import settings

from metrics_utility import library


worker_key = 'cleanup-ccsp'

s3_storage = library.storage.StorageS3(settings.s3_storage)

until = library.instants.months_ago(settings.retention.get(worker_key) or 12)

files = s3_storage.glob(glob='CCSP*.xlsx', until=until)

for file in files:
    s3_storage.remove(file)
