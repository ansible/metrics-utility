"""Tests for bookkeeping/last-gather fallback paths in slicing and collection."""

import datetime

from metrics_utility.gather.slicing import daily_slicing


tz = datetime.timezone.utc


def dt(year, month, day, hour=0):
    return datetime.datetime(year, month, day, hour, tzinfo=tz)


class TestDailySlicingSinceNone:
    """When since=None, daily_slicing should fall back to last_gathered_entries."""

    def test_uses_last_gathered_entry_for_key(self):
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 29)
        last_gathered = dt(2024, 1, 20)

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
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 3)

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
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 3)

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
        horizon = dt(2024, 1, 10)
        until = dt(2024, 2, 7)
        old_entry = dt(2023, 12, 1)

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
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 3)

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
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 5)
        other_entry = dt(2024, 1, 3)

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
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 3)

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
        horizon = dt(2024, 1, 1)
        since = dt(2024, 1, 15)
        until = dt(2024, 1, 17)

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=since,
                until=until,
                last_gathered_entries={'job_host_summary': dt(2024, 1, 10)},
            )
        )

        assert slices[0][0] == since

    def test_ignores_last_gathered_entries_when_since_given(self):
        horizon = dt(2024, 1, 1)
        since = dt(2024, 1, 5)
        until = dt(2024, 1, 7)

        slices = list(
            daily_slicing(
                key='job_host_summary',
                last_gather=horizon,
                since=since,
                until=until,
                last_gathered_entries={'job_host_summary': dt(2024, 1, 20)},
            )
        )

        assert slices[0][0] == since


class TestDailySlicingDayBoundaries:
    """Verify daily_slicing produces correct day-aligned slices."""

    def test_single_day(self):
        horizon = dt(2024, 1, 1)
        until = dt(2024, 1, 2)

        slices = list(
            daily_slicing(
                key='key',
                last_gather=horizon,
                since=horizon,
                until=until,
            )
        )

        assert slices == [(dt(2024, 1, 1), dt(2024, 1, 2))]

    def test_partial_first_day(self):
        start = dt(2024, 1, 1, 15)
        until = dt(2024, 1, 3)

        slices = list(
            daily_slicing(
                key='key',
                last_gather=start,
                since=start,
                until=until,
            )
        )

        assert slices[0] == (dt(2024, 1, 1, 15), dt(2024, 1, 2))
        assert slices[1] == (dt(2024, 1, 2), dt(2024, 1, 3))
        assert len(slices) == 2

    def test_three_full_days(self):
        since = dt(2024, 1, 1)
        until = dt(2024, 1, 4)

        slices = list(
            daily_slicing(
                key='key',
                last_gather=since,
                since=since,
                until=until,
            )
        )

        assert len(slices) == 3
        assert slices[0] == (dt(2024, 1, 1), dt(2024, 1, 2))
        assert slices[1] == (dt(2024, 1, 2), dt(2024, 1, 3))
        assert slices[2] == (dt(2024, 1, 3), dt(2024, 1, 4))
