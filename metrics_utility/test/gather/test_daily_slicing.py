"""Unit tests for daily_slicing with configurable METRICS_UTILITY_GATHER_INTERVAL_HOURS."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


UTC = timezone.utc


def _run_slicing(start_dt, until_dt, interval_hours=24, last_entries=None):
    """Helper: run daily_slicing and return list of (since, until) tuples."""
    from metrics_utility.automation_controller_billing.collectors import daily_slicing

    last_entries = last_entries or {}

    env_override = {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': str(interval_hours)}
    with (
        patch('metrics_utility.base.utils.os.getenv', side_effect=lambda k, d=None: env_override.get(k, d)),
        patch('metrics_utility.automation_controller_billing.collectors.get_last_entries_from_db', return_value=last_entries),
        patch('metrics_utility.automation_controller_billing.collectors.get_max_gather_period_days', return_value=28),
    ):
        return list(daily_slicing('test_key', start_dt, since=start_dt, until=until_dt))


class TestDailySlicing24h:
    """Default 24-hour interval — must match original behaviour."""

    def test_partial_first_day_then_full_day(self):
        start = datetime(2024, 1, 15, 15, 0, tzinfo=UTC)
        until = datetime(2024, 1, 17, 0, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 24)
        assert slices == [
            (datetime(2024, 1, 15, 15, 0, tzinfo=UTC), datetime(2024, 1, 16, 0, 0, tzinfo=UTC)),
            (datetime(2024, 1, 16, 0, 0, tzinfo=UTC), datetime(2024, 1, 17, 0, 0, tzinfo=UTC)),
        ]

    def test_exactly_one_day(self):
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 24)
        assert len(slices) == 1
        assert slices[0] == (start, until)

    def test_partial_last_day(self):
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 24)
        assert len(slices) == 1
        assert slices[0] == (start, until)


class TestDailySlicingSubDay:
    """Sub-day interval — slices never cross midnight."""

    def test_4h_interval_produces_correct_slices(self):
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 4)
        assert len(slices) == 6
        for s, e in slices:
            assert (e - s) == timedelta(hours=4)

    def test_slices_never_cross_midnight(self):
        start = datetime(2024, 1, 15, 22, 0, tzinfo=UTC)
        until = datetime(2024, 1, 16, 6, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 4)
        for s, e in slices:
            # end must be on same calendar day OR exactly midnight
            assert s.date() == e.date() or (e.hour == 0 and e.minute == 0 and e.second == 0)

    def test_1h_interval_full_day_is_24_slices(self):
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 1)
        assert len(slices) == 24

    def test_partial_interval_at_end_of_day(self):
        """Last interval of the day may be shorter than the configured interval."""
        start = datetime(2024, 1, 15, 22, 30, tzinfo=UTC)
        until = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 4)
        last_since, last_until = slices[-1]
        # Should stop at midnight, not 22:30 + 4h
        assert last_until == datetime(2024, 1, 16, 0, 0, tzinfo=UTC)

    def test_no_empty_slices(self):
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        until = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        slices = _run_slicing(start, until, 6)
        for s, e in slices:
            assert s < e


class TestDailySlicingValidation:
    """daily_slicing respects get_gather_interval_hours validation."""

    def test_interval_zero_raises(self):
        from metrics_utility.base.utils import get_gather_interval_hours

        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': '0'}):
            with pytest.raises(ValueError, match='must be >= 1'):
                get_gather_interval_hours()

    def test_interval_negative_raises(self):
        from metrics_utility.base.utils import get_gather_interval_hours

        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': '-2'}):
            with pytest.raises(ValueError, match='must be >= 1'):
                get_gather_interval_hours()
