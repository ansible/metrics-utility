# Gather config and job_host_summary collectors, upload to PostgreSQL storage

from django.db.utils import ConnectionHandler
from settings import settings

from metrics_utility import library


worker_key = 'gather-postgres'

# Setup database connections
controller_db = ConnectionHandler({'default': settings.controller_db})['default']
metrics_db = ConnectionHandler({'default': settings.metrics_db})['default']

# Setup PostgreSQL storage using metrics_db
postgres_storage = library.storage.StoragePostgres(
    db=metrics_db,
    table='metrics_data',
    key_field='key',
    value_field='value',
    timestamp_field='updated_at',
)

# Time range for collectors
since = library.instants.last_day()
until = library.instants.this_day()

# Setup collectors
c = library.collectors.controller
config_collector = c.config(db=controller_db)
job_host_summary_collector = c.job_host_summary(db=controller_db, since=since, until=until)

# Gather and upload with database lock
with library.lock(db=metrics_db, key=worker_key):
    # Gather config (returns dict)
    config_data = config_collector.gather()

    # Upload config to PostgreSQL storage
    config_key = f'config-{since.date()}-{until.date()}'
    postgres_storage.put(config_key, dict=config_data)

    # Gather job_host_summary (returns list of CSV files)
    job_host_summary_files = job_host_summary_collector.gather()

    # Upload each CSV file to PostgreSQL storage
    for i, csv_file in enumerate(job_host_summary_files):
        jhs_key = f'job_host_summary-{since.date()}-{until.date()}-{i:06}'
        postgres_storage.put(jhs_key, filename=csv_file)
