# metrics-utility gather_automation_controller_billing_data --since=2d --until=1d --ship equivalent with S3 and all traditional collectors

from django.db.utils import ConnectionHandler
from settings import settings

from metrics_utility import library


worker_key = 'gather-controller-full'

controller_db = ConnectionHandler(settings.controller_db)
metrics_db = ConnectionHandler(settings.metrics_db)
s3_storage = library.storage.StorageS3(settings.s3_storage)

since = library.instants.last_day()
until = library.instants.this_day()  # or library.instants.minutes_ago(10)

# collectors return Collection instances, have .gather, .add_to_tar, .key ; disregards slicing
c = library.collectors.controller
collectors = [
    c.config(db=controller_db),
    c.job_host_summary(db=controller_db, since=since, until=until),
    c.main_host(db=controller_db),
    c.main_jobevent(db=controller_db, since=since, until=until),
    c.main_indirectmanagednodeaudit(db=controller_db, since=since, until=until),
]

# nothing should remain in local filesystem (outside StorageDirectory) even when everything blows up
with library.tempdir(prefix=worker_key):
    # config, manifest & data_collection_status in every tarball,
    # a tarball for each collectors file
    package = library.package.PackageTarballs(
        config=collectors[0],
        collectors=collectors[1:],
        max_size='100M',
        tarball_format='{uuid}-{since}-{until}-{collection_key}-{index:06}.tar.gz',
        payload_format='{collection_key}.{collection_type}',
    )

    # we *may* want to wait=False and abort the task instead
    with library.lock(db=metrics_db, key=worker_key, wait=True):
        while not package.done():  # 100M-sized tarballs
            # actually calls collectors gather
            with package.next() as tarball:
                s3_storage.put(tarball.name, filename=tarball.file)
                # add a crc_storage.put(None, filename=tarball) to ship to both
            # tarball auto-cleanup
    # lock auto-release
# tempdir auto-cleanup

# gather returns .. for csv, a file generator ; for json, just a serializable dict

# return/save a worker exit status object
# Storage.shipping_successful, Collection.gathering_successful, Collection.gathering_{started,finished}_at
