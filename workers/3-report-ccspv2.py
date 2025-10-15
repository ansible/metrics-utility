# CCSPv2 report worker (2/4 dataframes, combining tarballs and rollups just as an example)

from settings import settings

from metrics_utility import library


worker_key = 'report-ccspv2'

s3_storage = library.storage.StorageS3(settings.s3_storage)

since = library.instants.last_month()
until = library.instants.this_month()

extractor = library.extractors.ExtractorTarballs()

dataframeJ = library.dataframes.DataframeJobHostSummary()
dataframeH = library.dataframes.DataframeHost()
dataframeS = library.dataframes.DataframeCollectionStatus()

with library.tempdir(prefix=worker_key):
    # tarballs
    job_host_summaries = s3_storage.glob(glob='*-job_host_summary-*.tar.gz', since=since, until=until)
    for remote in job_host_summaries:
        with s3_storage.get(remote) as local:
            for csv in extractor.extract(local, only=['job_host_summary.csv', 'data_collection_status.csv']):
                if csv.tarname == './data_collection_status.csv':
                    dataframeS.add_csv(csv)
                else:
                    dataframeJ.add_csv(csv)

    # rollups
    hosts = s3_storage.glob(glob='rollup-host-*', since=since, until=until)
    for remote in hosts:
        with s3_storage.get(remote) as local:
            dataframeH.add_parquet(local)

    # build the report objects
    report = library.reports.ReportCCSPv2(
        dataframes={
            'data_collection_status': dataframeS,
            'job_host_summary': dataframeJ,
            'main_host': dataframeH,
        },
        extra_params={
            'price_per_node': '123',
            'report_company_name': 'Abc',
            # ...
        },
    ).create()

    # save, ship, clean
    with report.to_xlsx() as local:
        s3_storage.put(path=f'CCSPv2-{until}.xlsx', filename=local)
