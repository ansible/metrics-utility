# Just one JSON collector, shipping to CRC.

from django.db.utils import ConnectionHandler
from settings import settings

from metrics_utility import library


worker_key = 'gather-controller-crc'

# assume exceptions are logged & saved in task results by what's running the worker
controller_db = ConnectionHandler(settings.controller_db)
metrics_db = ConnectionHandler(settings.metrics_db)
crc_storage = library.storage.StorageCRCMutual(settings.crc_storage)

# wrappers around datetime, timedelta, timezone - always a datetime with timezone
since = library.instants.last_day()
until = library.instants.this_day()

# sets up a CollectionJSON instance, etc, does not run yet
collector = library.collectors.controller.anonymous(db=controller_db, since=since, until=until, custom_params=True)

# DB lock, in *our* DB
with library.lock(db=metrics_db, key=worker_key):
    # run gather, get json (buffer/string/filelist)
    data = collector.gather()

    # send data to CRC
    # storage can handle json -> protobuf too, or we can add a format=protobuf for @register
    crc_storage.put(None, dict=data)
