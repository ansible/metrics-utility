# Hypothetical renewal report (might actually go separate collect, rollup, report too)

from django.db.utils import ConnectionHandler
from settings import settings

from metrics_utility import library


worker_key = 'report-renewal'

controller_db = ConnectionHandler(settings.controller_db)
metrics_db = ConnectionHandler(settings.metrics_db)
s3_storage = library.storage.StorageS3(settings.s3_storage)

# our db, no lock needed
since = library.last_gather(db=metrics_db, key=worker_key) or library.instants.last_month()
until = library.instants.now()

dataframe = library.dataframes.DataframeHostMetric()
collector = library.collectors.controller.host_metric(db=controller_db, since=since)

with library.lock(db=metrics_db, key=worker_key):
    dataframe.add(collector.gather())

with library.tempdir(prefix=worker_key):
    report = library.reports.ReportRenewalGuidance(
        dataframes={
            'host_metric': dataframe,
        },
        extra_params={
            # ...
        },
    ).create()

    # save, ship, clean
    with report.to_xlsx() as local:
        s3_storage.put(path=f'RENEWAL-{until}.xlsx', filename=local)

library.save_last_gather(db=metrics_db, key=worker_key, value=until)
