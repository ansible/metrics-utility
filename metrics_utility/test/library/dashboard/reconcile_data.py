"""Fixed Controller rows for the dashboard reconciliation database tests.

The rows are inserted in a transaction that is always rolled back, so the tests do not depend on (or change)
the seeded test database, apart from advancing the ID sequences, which a rollback does not reset.

The foreign keys of the Controller tables are ``DEFERRABLE INITIALLY DEFERRED``, so they are never checked
for these rolled-back rows and only the columns the queries read need realistic values.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from django.db import connection, transaction

from metrics_utility.library.collectors.dashboard import job_ids_digest


SINCE = datetime(2001, 2, 3, tzinfo=UTC)
UNTIL = SINCE + timedelta(days=1)
STATUSES = ['failed', 'successful']

MINUTE_0000 = SINCE
MINUTE_1005 = SINCE.replace(hour=10, minute=5)
MINUTE_1006 = SINCE.replace(hour=10, minute=6)

# key: (status, finished, launch_type, number of host summaries, number of labels)
JOBS = {
    # Two jobs in the same minute, so the per-minute count and digest cover more than one job.
    'job_successful': ('successful', MINUTE_1005.replace(second=10), 'manual', 2, 2),
    'job_failed': ('failed', MINUTE_1005.replace(second=50), 'scheduled', 1, 0),
    # A sync launch (project update) without host summaries but with a label: stored by the dashboard, so it must be counted.
    'sync_successful': ('successful', MINUTE_1006, 'sync', 0, 1),
    # Exactly at ``since`` (inclusive).
    'workflow_at_since': ('successful', SINCE, 'workflow', 0, 0),
    # Excluded: status not requested, but in the same minute and with a host summary and a label.
    'job_running': ('running', MINUTE_1005.replace(second=30), 'manual', 1, 1),
    # Excluded: exactly at ``until`` (exclusive) and just before ``since``.
    'job_at_until': ('successful', UNTIL, 'manual', 1, 1),
    'job_before_since': ('successful', SINCE - timedelta(microseconds=1), 'manual', 1, 1),
}

_INSERT_UNIFIED_JOB = """
    INSERT INTO main_unifiedjob (
        created, modified, finished, description, name, launch_type, cancel_flag, status, failed, elapsed,
        job_args, job_cwd, job_explanation, start_args, result_traceback, celery_task_id, execution_node,
        emitted_events, controller_node, dependencies_processed, installed_collections, ansible_version,
        task_impact, job_env
    )
    VALUES (
        %s, %s, %s, '', %s, %s, false, %s, %s, 0,
        '', '', '', '', '', '', '',
        0, '', false, '{}'::jsonb, '',
        0, '{}'::jsonb
    )
    RETURNING id
"""

_INSERT_HOST_SUMMARY = """
    INSERT INTO main_jobhostsummary (
        created, modified, host_name, changed, dark, failures, ok, processed, skipped, failed, job_id, ignored, rescued
    )
    VALUES (%s, %s, %s, 0, 0, 0, 1, 1, 0, false, %s, 0, 0)
"""

# The label IDs need no main_label rows: the foreign key is deferred and the rows are rolled back.
_INSERT_JOB_LABEL = """
    INSERT INTO main_unifiedjob_labels (unifiedjob_id, label_id)
    VALUES (%s, %s)
"""


@dataclass
class ReconcileJobs:
    """IDs and finish times of the inserted jobs, keyed by the names in ``JOBS``."""

    ids: dict[str, int] = field(default_factory=dict)
    finished: dict[str, datetime] = field(default_factory=dict)

    def digest(self, *names: str) -> str:
        return job_ids_digest(self.ids[name] for name in names)


def insert_unified_job(cursor, name: str, status: str, finished: datetime, launch_type: str = 'manual') -> int:
    """Insert one unified job finished at *finished* and return its ID."""
    created = finished - timedelta(minutes=1)
    cursor.execute(_INSERT_UNIFIED_JOB, [created, finished, finished, name, launch_type, status, status == 'failed'])
    return cursor.fetchone()[0]


@contextmanager
def reconcile_jobs():
    """Insert ``JOBS`` (and their host summaries and labels) and roll everything back on exit."""
    with transaction.atomic():
        with connection.cursor() as cursor:
            window = (SINCE - timedelta(days=1), UNTIL + timedelta(days=1))
            cursor.execute('SELECT COUNT(*) FROM main_unifiedjob WHERE finished >= %s AND finished < %s', window)
            assert cursor.fetchone()[0] == 0, f'the test database must not contain jobs finished in {window}'

            jobs = ReconcileJobs()
            for name, (status, finished, launch_type, host_count, label_count) in JOBS.items():
                job_id = insert_unified_job(cursor, name, status, finished, launch_type)
                jobs.ids[name] = job_id
                jobs.finished[name] = finished
                for host in range(host_count):
                    cursor.execute(_INSERT_HOST_SUMMARY, [finished, finished, f'host-{host}', job_id])
                for label in range(label_count):
                    cursor.execute(_INSERT_JOB_LABEL, [job_id, label + 1])
        try:
            yield jobs
        finally:
            transaction.set_rollback(True)
