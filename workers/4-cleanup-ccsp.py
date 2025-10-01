# Cleanup worker for eg. reports:

from metrics_utility import library
from settings import settings


worker_key = 'cleanup-ccsp'

s3_storage = library.storage.StorageS3(settings.s3_storage)

until = library.instants.months_ago(settings.retention)

files = s3_storage.glob(glob='CCSP*.xlsx', until=until)

s3_storage.remove(files)
