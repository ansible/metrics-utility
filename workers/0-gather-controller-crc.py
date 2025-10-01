# Just one JSON collector, shipping to CRC.

from django.db import connection

from metrics_utility import library
from settings import settings


worker_key = 'gather-controller-crc'

# assume exceptions are logged & saved in task results by what's running the worker
controller_db = connection(settings.controller_db)
metrics_db = connection(settings.metrics_db)
crc_storage = library.storage.StorageCRCMutual(settings.s3_storage)

# wrappers around datetime, timedelta, timezone - always a datetime with timezone
since = library.instants.last_day()
until = library.instants.this_day()

# sets up a CollectionJSON instance, etc, does not run yet
collector = library.collectors.anonymous(db=controller_db, since=since, until=until, custom_params=True)

# DB lock, in *our* DB
with library.lock(db=metrics_db, key=worker_key):
    # run gather, get json (buffer/string/filelist)
    data = collector.gather()

    # send data to CRC
    # storage can handle json -> protobuf too, or we can add a format=protobuf for @register
    crc_storage.ship(data)
