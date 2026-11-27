"""Tests for the daily dashboard reconciliation queries in ``dashboard/queries.py``."""

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from django.db import connection, transaction

from metrics_utility.library.collectors.dashboard import (
    get_job_host_summary_counts_query,
    get_job_ids_for_minutes_query,
    get_job_label_counts_query,
    get_job_minute_aggregate_query,
    job_ids_digest,
)
from metrics_utility.test.library.dashboard import reconcile_data
from metrics_utility.test.library.dashboard.reconcile_data import MINUTE_0000, MINUTE_1005, MINUTE_1006


SINCE = datetime(2024, 1, 1, tzinfo=UTC)
UNTIL = datetime(2024, 1, 2, tzinfo=UTC)
STATUSES = ['failed', 'successful']
MINUTES = [datetime(2024, 1, 1, 10, 5, tzinfo=UTC), datetime(2024, 1, 1, 23, 59, tzinfo=UTC)]

QUERY_BUILDERS = [
    pytest.param(lambda since, until, statuses: get_job_minute_aggregate_query(since, until, statuses), id='aggregate'),
    pytest.param(lambda since, until, statuses: get_job_ids_for_minutes_query(since, until, statuses, MINUTES), id='ids'),
    pytest.param(lambda since, until, statuses: get_job_host_summary_counts_query(since, until, statuses), id='host_summaries'),
    pytest.param(lambda since, until, statuses: get_job_label_counts_query(since, until, statuses), id='labels'),
]


class TestReconcileQueriesSql:
    """Structure and parameters of the generated SQL."""

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_filters_on_finished_and_statuses(self, build):
        query, params = build(SINCE, UNTIL, STATUSES)
        assert 'uj.finished >= %s' in query
        assert 'uj.finished < %s' in query
        assert 'uj.status = ANY(%s::text[])' in query
        assert params[:3] == [SINCE, UNTIL, STATUSES]

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_includes_every_job_type(self, build):
        """The dashboard stores every unified job type, so the reconciliation must not narrow it."""
        query, _ = build(SINCE, UNTIL, STATUSES)
        assert 'main_job ' not in query
        assert 'main_job\n' not in query
        assert 'launch_type' not in query
        assert 'modified' not in query

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_statuses_are_parameters(self, build):
        query, params = build(SINCE, UNTIL, ('successful',))
        assert 'successful' not in query
        assert params[2] == ['successful']

    def test_minute_aggregate_query(self):
        query, params = get_job_minute_aggregate_query(SINCE, UNTIL, STATUSES)
        assert "date_trunc('minute', uj.finished, 'UTC') AS minute" in query
        assert 'COUNT(*) AS job_count' in query
        assert "encode(sha256(convert_to(string_agg(uj.id::text, ',' ORDER BY uj.id), 'UTF8')), 'hex') AS job_ids_sha256" in query
        assert 'FROM main_unifiedjob uj' in query
        assert 'GROUP BY 1' in query
        assert 'ORDER BY 1' in query
        assert len(params) == 3

    def test_job_ids_for_minutes_query(self):
        query, params = get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, MINUTES)
        assert 'SELECT uj.id AS job_id, uj.finished' in query
        assert "date_trunc('minute', uj.finished, 'UTC') = ANY(%s::timestamptz[])" in query
        assert 'ORDER BY uj.id' in query
        assert params == [SINCE, UNTIL, STATUSES, MINUTES]

    def test_job_ids_for_minutes_query_accepts_tuple(self):
        _, params = get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, tuple(MINUTES))
        assert params[3] == MINUTES

    def test_host_summary_counts_query(self):
        query, params = get_job_host_summary_counts_query(SINCE, UNTIL, STATUSES)
        assert 'SELECT hs.job_id, COUNT(*) AS host_summary_count' in query
        assert 'FROM main_jobhostsummary hs' in query
        assert 'JOIN main_unifiedjob uj ON uj.id = hs.job_id' in query
        assert 'GROUP BY hs.job_id' in query
        assert 'ORDER BY hs.job_id' in query
        assert len(params) == 3

    def test_label_counts_query(self):
        query, params = get_job_label_counts_query(SINCE, UNTIL, STATUSES)
        assert 'SELECT l.unifiedjob_id AS job_id, COUNT(*) AS label_count' in query
        assert 'FROM main_unifiedjob_labels l' in query
        assert 'JOIN main_unifiedjob uj ON uj.id = l.unifiedjob_id' in query
        assert 'GROUP BY l.unifiedjob_id' in query
        assert 'ORDER BY l.unifiedjob_id' in query
        assert len(params) == 3


class TestJobIdsDigest:
    """The digest the minute aggregate computes in SQL, reproducible in Python."""

    def test_digest_is_sha256_of_sorted_comma_separated_ids(self):
        assert job_ids_digest([7, 3]) == 'adc0d2b391a5218d93c900f87226ce222fca03d61ee5537620d45450d86a10ea'
        assert job_ids_digest((3, 7)) == job_ids_digest(np.array([7, 3]))

    def test_digest_tells_apart_sets_with_equal_count_and_sum(self):
        """A missing and an extra job in the same minute (e.g. {4, 6} against {3, 7}) are detected."""
        assert job_ids_digest([4, 6]) != job_ids_digest([3, 7])

    def test_digest_does_not_confuse_concatenated_ids(self):
        assert job_ids_digest([1, 23]) != job_ids_digest([12, 3])

    def test_rejects_non_lists(self):
        with pytest.raises(ValueError, match='job_ids must be an iterable of integers, got str'):
            job_ids_digest('3,7')

    @pytest.mark.parametrize('job_ids', [['9', '10'], [3.0, 7.0], [True, 3], [np.True_], [None]], ids=['str', 'float', 'bool', 'numpy_bool', 'none'])
    def test_rejects_values_the_database_digest_never_produces(self, job_ids):
        """Strings sort as text and floats render as '3.0', so their digest would silently never match."""
        with pytest.raises(ValueError, match='job_ids must contain integers'):
            job_ids_digest(job_ids)

    def test_accepts_numpy_integers(self):
        assert job_ids_digest(np.array([7, 3], dtype=np.int64)) == job_ids_digest([3, 7])

    def test_duplicates_are_ignored(self):
        """Job IDs are unique on the Controller, so the digest is the one of the distinct IDs."""
        assert job_ids_digest([3, 7, 3]) == job_ids_digest([3, 7])


class TestReconcileQueriesValidation:
    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    @pytest.mark.parametrize('statuses', [[], (), 'successful', None, [''], [1], ['successful', None]])
    def test_rejects_invalid_statuses(self, build, statuses):
        with pytest.raises(ValueError, match='statuses'):
            build(SINCE, UNTIL, statuses)

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_rejects_naive_window(self, build):
        with pytest.raises(ValueError, match='since must be timezone-aware'):
            build(SINCE.replace(tzinfo=None), UNTIL, STATUSES)
        with pytest.raises(ValueError, match='until must be timezone-aware'):
            build(SINCE, UNTIL.replace(tzinfo=None), STATUSES)

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_rejects_non_datetime_window(self, build):
        with pytest.raises(TypeError, match='since must be a datetime'):
            build('2024-01-01', UNTIL, STATUSES)

    @pytest.mark.parametrize('minutes', [[], None, '2024-01-01T10:05:00Z'])
    def test_rejects_invalid_minutes(self, minutes):
        with pytest.raises(ValueError, match='minutes'):
            get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, minutes)

    def test_rejects_naive_minute(self):
        with pytest.raises(ValueError, match='timezone-aware'):
            get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, [datetime(2024, 1, 1, 10, 5)])

    def test_rejects_non_datetime_minute(self):
        with pytest.raises(TypeError, match='minutes must contain datetimes, got str'):
            get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, ['2024-01-01T10:05:00Z'])

    @pytest.mark.parametrize('minute', [datetime(2024, 1, 1, 10, 5, 30, tzinfo=UTC), datetime(2024, 1, 1, 10, 5, 0, 1, tzinfo=UTC)])
    def test_rejects_minute_that_is_not_truncated(self, minute):
        with pytest.raises(ValueError, match='truncated to the minute'):
            get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, [MINUTES[0], minute])

    def test_accepts_iterable_statuses_and_minutes(self):
        minutes = pd.Series(pd.to_datetime([m.isoformat() for m in MINUTES], utc=True))
        _, params = get_job_ids_for_minutes_query(SINCE, UNTIL, np.array(STATUSES), minutes)
        assert params[2] == STATUSES
        assert params[3] == MINUTES

    def test_rejects_datetime64_minutes(self):
        """``Series.values`` of a tz-aware column is a naive datetime64 array; the Series itself works."""
        minutes = pd.Series(pd.to_datetime([m.isoformat() for m in MINUTES], utc=True))
        with pytest.raises(TypeError, match='minutes must contain datetimes, got datetime64'):
            get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, minutes.values)
        _, params = get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, minutes.to_numpy())
        assert params[3] == MINUTES

    def test_errors_name_only_the_offending_value(self):
        with pytest.raises(ValueError) as error:
            get_job_minute_aggregate_query(SINCE, UNTIL, ['successful', ''])
        assert str(error.value) == "statuses must contain non-empty strings, got ''"
        with pytest.raises(ValueError) as error:
            get_job_ids_for_minutes_query(SINCE, UNTIL, STATUSES, 'not-a-list')
        assert str(error.value) == 'minutes must be an iterable, got str'


def _fetch(query, params):
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()


def _utc(value):
    return value.astimezone(UTC)


class TestReconcileQueriesDatabase:
    """Run the queries against fixed rows inserted into the Controller test database (rolled back afterwards)."""

    window = (reconcile_data.SINCE, reconcile_data.UNTIL)

    def test_minute_aggregate(self, reconcile_jobs):
        rows = _fetch(*get_job_minute_aggregate_query(*self.window, reconcile_data.STATUSES))

        assert [(_utc(minute), count, digest) for minute, count, digest in rows] == [
            (MINUTE_0000, 1, reconcile_jobs.digest('workflow_at_since')),
            (MINUTE_1005, 2, reconcile_jobs.digest('job_successful', 'job_failed')),
            (MINUTE_1006, 1, reconcile_jobs.digest('sync_successful')),
        ]

    def test_minute_aggregate_filters_statuses(self, reconcile_jobs):
        rows = _fetch(*get_job_minute_aggregate_query(*self.window, ['failed']))

        assert [(_utc(minute), count, digest) for minute, count, digest in rows] == [
            (MINUTE_1005, 1, reconcile_jobs.digest('job_failed')),
        ]

    def test_job_ids_for_minutes(self, reconcile_jobs):
        rows = _fetch(*get_job_ids_for_minutes_query(*self.window, reconcile_data.STATUSES, [MINUTE_1005, MINUTE_0000]))

        expected = sorted((reconcile_jobs.ids[name], reconcile_jobs.finished[name]) for name in ('job_successful', 'job_failed', 'workflow_at_since'))
        assert [(job_id, _utc(finished)) for job_id, finished in rows] == expected

    def test_job_ids_for_minutes_from_aggregate_dataframe(self, reconcile_jobs):
        """The caller can pass the ``minute`` column of the aggregate result as a pandas Series."""
        aggregate = pd.DataFrame(
            _fetch(*get_job_minute_aggregate_query(*self.window, reconcile_data.STATUSES)), columns=['minute', 'job_count', 'job_ids_sha256']
        )
        mismatched = aggregate.loc[aggregate['job_count'] > 1, 'minute']

        rows = _fetch(*get_job_ids_for_minutes_query(*self.window, reconcile_data.STATUSES, mismatched))

        assert [job_id for job_id, _ in rows] == sorted(reconcile_jobs.ids[name] for name in ('job_successful', 'job_failed'))

    def test_job_ids_for_minutes_filters_statuses(self, reconcile_jobs):
        rows = _fetch(*get_job_ids_for_minutes_query(*self.window, ['running'], [MINUTE_1005]))

        assert [job_id for job_id, _ in rows] == [reconcile_jobs.ids['job_running']]

    def test_job_ids_for_minute_without_jobs(self, reconcile_jobs):
        empty_minute = MINUTE_1006.replace(hour=12)
        assert _fetch(*get_job_ids_for_minutes_query(*self.window, reconcile_data.STATUSES, [empty_minute])) == []

    def test_host_summary_counts(self, reconcile_jobs):
        rows = _fetch(*get_job_host_summary_counts_query(*self.window, reconcile_data.STATUSES))

        # Jobs without host summaries (sync_successful, workflow_at_since) are not returned.
        assert rows == sorted([(reconcile_jobs.ids['job_successful'], 2), (reconcile_jobs.ids['job_failed'], 1)])

    def test_host_summary_counts_filter_statuses(self, reconcile_jobs):
        rows = _fetch(*get_job_host_summary_counts_query(*self.window, ['running']))

        assert rows == [(reconcile_jobs.ids['job_running'], 1)]

    def test_label_counts(self, reconcile_jobs):
        rows = _fetch(*get_job_label_counts_query(*self.window, reconcile_data.STATUSES))

        # Jobs without labels (job_failed, workflow_at_since) are not returned; sync jobs are included.
        assert rows == sorted([(reconcile_jobs.ids['job_successful'], 2), (reconcile_jobs.ids['sync_successful'], 1)])

    def test_label_counts_filter_statuses(self, reconcile_jobs):
        rows = _fetch(*get_job_label_counts_query(*self.window, ['running']))

        assert rows == [(reconcile_jobs.ids['job_running'], 1)]

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_unknown_status_returns_no_rows(self, reconcile_jobs, build):
        assert _fetch(*build(*self.window, ['no-such-status'])) == []

    @pytest.mark.parametrize('build', QUERY_BUILDERS)
    def test_empty_window_returns_no_rows(self, reconcile_jobs, build):
        since = reconcile_data.UNTIL + timedelta(hours=1)
        assert _fetch(*build(since, since + timedelta(hours=1), reconcile_data.STATUSES)) == []


class TestReconcileQueriesRawConnection:
    """Run the queries the way metrics-service does: on the raw psycopg connection, not Django's wrapper."""

    window = (reconcile_data.SINCE, reconcile_data.UNTIL)

    @staticmethod
    def _fetch_raw(query, params, timezone=None):
        with connection.connection.cursor() as cursor:
            if timezone:
                # Local to the test transaction, which reconcile_jobs rolls back.
                cursor.execute(f"SET LOCAL TimeZone = '{timezone}'")
            cursor.execute(query, params)
            return cursor.fetchall()

    def test_queries_on_raw_connection(self, reconcile_jobs):
        statuses = tuple(reconcile_data.STATUSES)
        aggregate = self._fetch_raw(*get_job_minute_aggregate_query(*self.window, statuses))
        minutes = [minute for minute, _, _ in aggregate]

        ids = self._fetch_raw(*get_job_ids_for_minutes_query(*self.window, statuses, minutes))
        host_summaries = self._fetch_raw(*get_job_host_summary_counts_query(*self.window, statuses))
        labels = self._fetch_raw(*get_job_label_counts_query(*self.window, statuses))

        assert aggregate == _fetch(*get_job_minute_aggregate_query(*self.window, statuses))
        expected_ids = ('workflow_at_since', 'job_successful', 'job_failed', 'sync_successful')
        assert sorted(job_id for job_id, _ in ids) == sorted(reconcile_jobs.ids[name] for name in expected_ids)
        assert host_summaries == sorted([(reconcile_jobs.ids['job_successful'], 2), (reconcile_jobs.ids['job_failed'], 1)])
        assert labels == sorted([(reconcile_jobs.ids['job_successful'], 2), (reconcile_jobs.ids['sync_successful'], 1)])

    @pytest.mark.parametrize('timezone', ['Asia/Kathmandu', 'America/St_Johns', 'Europe/Ljubljana'])
    def test_results_are_the_same_under_other_session_timezones(self, reconcile_jobs, timezone):
        """Regression check: other session time zones (including offsets that are not whole hours) give the same rows.

        Every current offset is a whole number of minutes, so this passes with or without the ``'UTC'`` argument of
        ``date_trunc``; ``test_minutes_are_truncated_in_utc_under_an_offset_with_seconds`` covers that argument. The
        minutes are compared as epoch seconds: Django's timestamptz loader assumes a UTC session (Django sets it on
        connect), so it would misread the values returned under another session time zone.
        """
        query, params = get_job_minute_aggregate_query(*self.window, reconcile_data.STATUSES)
        aggregate = self._fetch_raw(f'SELECT extract(epoch FROM minute)::bigint, job_count FROM ({query}) q', params, timezone=timezone)

        expected_minutes = [MINUTE_0000, MINUTE_1005, MINUTE_1006]
        assert aggregate == [(int(minute.timestamp()), count) for minute, count in zip(expected_minutes, [1, 2, 1])]
        ids = self._fetch_raw(
            *get_job_ids_for_minutes_query(*self.window, reconcile_data.STATUSES, [MINUTE_1005]),
            timezone=timezone,
        )
        assert [job_id for job_id, _ in ids] == sorted(reconcile_jobs.ids[name] for name in ('job_successful', 'job_failed'))

    def test_minutes_are_truncated_in_utc_under_an_offset_with_seconds(self):
        """Africa/Monrovia was UTC-00:44:30 until 1972: truncating in that session time zone would put a job that
        finished at 10:05:10 UTC into the minute starting at 10:04:30 UTC instead of 10:05:00 UTC."""
        since = datetime(1971, 6, 1, tzinfo=UTC)
        until = since + timedelta(days=1)
        minute = since.replace(hour=10, minute=5)
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM main_unifiedjob WHERE finished >= %s AND finished < %s', [since, until])
                assert cursor.fetchone()[0] == 0, f'the test database must not contain jobs finished in [{since}, {until})'
                job_id = reconcile_data.insert_unified_job(cursor, 'monrovia', 'successful', minute.replace(second=10))
            try:
                query, params = get_job_minute_aggregate_query(since, until, reconcile_data.STATUSES)
                aggregate = self._fetch_raw(
                    f'SELECT extract(epoch FROM minute)::bigint, job_count FROM ({query}) q', params, timezone='Africa/Monrovia'
                )
                ids = self._fetch_raw(*get_job_ids_for_minutes_query(since, until, reconcile_data.STATUSES, [minute]), timezone='Africa/Monrovia')
            finally:
                transaction.set_rollback(True)

        assert aggregate == [(int(minute.timestamp()), 1)]
        assert [found for found, _ in ids] == [job_id]
