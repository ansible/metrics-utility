#!/bin/sh
set -e
set -x

# cleanup
uv run ./manage.py shell <<EOF
HourlyMetricsCollection.objects.all().delete()
DailyMetricsSummary.objects.all().delete()
AnonymizedMetricsPayload.objects.all().delete()
EOF

# 24 hourly collections, .collection_timestamp = since
for hour in `seq 0 23`; do
  uv run tools/tasks/run_task.py collect_hourly_metrics '{"collector_type": "credentials_service", "hour_timestamp": "20250613T'`printf %02d "$hour"`':00:00Z"}'
  uv run tools/tasks/run_task.py collect_hourly_metrics '{"collector_type": "job_host_summary_service", "hour_timestamp": "20250613T'`printf %02d "$hour"`':00:00Z"}'
  # disabled for now: uv run tools/tasks/run_task.py collect_hourly_metrics '{"collector_type": "main_jobevent_service", "hour_timestamp": "20250613T'`printf %02d "$hour"`':00:00Z"}'
  uv run tools/tasks/run_task.py collect_hourly_metrics '{"collector_type": "unified_jobs", "hour_timestamp": "20250613T'`printf %02d "$hour"`':00:00Z"}'
done

# these just run once per day, same table (misnomer but works)
# .collection_timestamp = since:23:0:0
uv run tools/tasks/run_task.py collect_snapshot_metrics '{"collector_type": "config", "collection_timestamp": "20250613T23:00:00Z"}'
uv run tools/tasks/run_task.py collect_snapshot_metrics '{"collector_type": "controller_version_service", "collection_timestamp": "20250613T23:00:00Z"}'
uv run tools/tasks/run_task.py collect_snapshot_metrics '{"collector_type": "execution_environments", "collection_timestamp": "20250613T23:00:00Z"}'
uv run tools/tasks/run_task.py collect_snapshot_metrics '{"collector_type": "table_metadata", "collection_timestamp": "20250613T23:00:00Z"}'

# dump to hourly_dumps/
uv run tools/tasks/dump_hourly.py

# this reads from tasks_hourlymetricscollection and writes to tasks_dailymetricssummary; doesn't accept datetime, just date
uv run tools/tasks/run_task.py daily_metrics_rollup '{"summary_date": "20250613"}'

# this reads from tasks_dailymetricssummary and writes to tasks_anonymizedmetricspayload; doesn't accept datetime, just date
uv run tools/tasks/run_task.py daily_anonymize_and_prepare '{"summary_date": "20250613"}'

# dump to daily_dumps/
uv run tools/tasks/dump_daily_anonymized.py

# this reads from tasks_anonymizedmetricspayload and sends to segment
uv run tools/tasks/run_task.py send_anonymized_to_segment

echo SUCCESS
