"""Tests for bookkeeping/last-gather fallback paths in slicing and collection."""

from metrics_utility.gather.slicing import daily_slicing
from metrics_utility.test.util import utcdt


class TestDailySlicingSinceNone:
    """When since=None, daily_slicing should fall back to last_gathered_entries."""

    def test_uses_last_gathered_entry_for_key(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-29')
        last_gathered = utcdt('2024-01-20')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
                last_gathered_entries={'job_host_summary': last_gathered},
            )
        )

        assert slices[0][0] == last_gathered
        assert slices[-1][1] == until

    def test_falls_back_to_horizon_when_no_entry(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-03')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
                last_gathered_entries={},
            )
        )

        assert slices[0][0] == horizon

    def test_falls_back_to_horizon_when_entry_is_none(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-03')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
                last_gathered_entries={'job_host_summary': None},
            )
        )

        assert slices[0][0] == horizon

    def test_clamps_to_horizon_when_entry_is_older(self):
        horizon = utcdt('2024-01-10')
        until = utcdt('2024-02-07')
        old_entry = utcdt('2023-12-01')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
                last_gathered_entries={'job_host_summary': old_entry},
            )
        )

        assert slices[0][0] == horizon

    def test_handles_stale_non_datetime_entry(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-03')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
                last_gathered_entries={'job_host_summary': 'not-a-datetime'},
            )
        )

        assert slices[0][0] == horizon

    def test_ignores_entries_for_other_keys(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-05')
        other_entry = utcdt('2024-01-03')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
                last_gathered_entries={'some_other_collector': other_entry},
            )
        )

        assert slices[0][0] == horizon

    def test_defaults_to_empty_when_no_entries_kwarg(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-03')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=None,
                until=until,
            )
        )

        assert slices[0][0] == horizon


class TestDailySlicingWithSince:
    """When since is provided, daily_slicing should use it directly."""

    def test_uses_explicit_since(self):
        horizon = utcdt('2024-01-01')
        since = utcdt('2024-01-15')
        until = utcdt('2024-01-17')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=since,
                until=until,
                last_gathered_entries={'job_host_summary': utcdt('2024-01-10')},
            )
        )

        assert slices[0][0] == since

    def test_ignores_last_gathered_entries_when_since_given(self):
        horizon = utcdt('2024-01-01')
        since = utcdt('2024-01-05')
        until = utcdt('2024-01-07')

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=since,
                until=until,
                last_gathered_entries={'job_host_summary': utcdt('2024-01-20')},
            )
        )

        assert slices[0][0] == since


class TestDailySlicingDayBoundaries:
    """Verify daily_slicing produces correct day-aligned slices."""

    def test_single_day(self):
        horizon = utcdt('2024-01-01')
        until = utcdt('2024-01-02')

        slices = list(
            daily_slicing(
                key='key',
                last_gather=horizon,
                since=horizon,
                until=until,
            )
        )

        assert slices == [(utcdt('2024-01-01'), utcdt('2024-01-02'))]

    def test_partial_first_day(self):
        start = utcdt('2024-01-01T15:00:00')
        until = utcdt('2024-01-03')

        slices = list(
            daily_slicing(
                key='key',
                last_gather=start,
                since=start,
                until=until,
            )
        )

        assert slices[0] == (utcdt('2024-01-01T15:00:00'), utcdt('2024-01-02'))
        assert slices[1] == (utcdt('2024-01-02'), utcdt('2024-01-03'))
        assert len(slices) == 2

    def test_three_full_days(self):
        since = utcdt('2024-01-01')
        until = utcdt('2024-01-04')

        slices = list(
            daily_slicing(
                key='key',
                last_gather=since,
                since=since,
                until=until,
            )
        )

        assert len(slices) == 3
        assert slices[0] == (utcdt('2024-01-01'), utcdt('2024-01-02'))
        assert slices[1] == (utcdt('2024-01-02'), utcdt('2024-01-03'))
        assert slices[2] == (utcdt('2024-01-03'), utcdt('2024-01-04'))
