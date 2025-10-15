# Hosts rollup worker.

from settings import settings

from metrics_utility import library


worker_key = 'rollup-controller-hosts'

s3_storage = library.storage.StorageS3(settings.s3_storage)

since = library.instants.last_week()
until = library.instants.this_week()

dataframe = library.dataframes.DataframeHost()
extractor = library.extractors.ExtractorTarballs()

with library.tempdir(prefix=worker_key):
    # or a metrics_db metadata lookup
    files = s3_storage.glob(glob='*-main_host-*.tar.gz', since=since, until=until)

    for remote in files:
        with s3_storage.get(remote) as local:
            for csv in extractor.extract(local, only='main_host.csv'):
                dataframe.add_csv(csv)

    s3_storage.put(path=f'rollup-host-{until}', filename=dataframe.to_parquet())
    # add a db_storage & dataframe.to_sql to also save rollups to DB
