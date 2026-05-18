from unittest.mock import patch

from metrics_utility.automation_controller_billing.collectors import hourly_slicing
from metrics_utility.test.util import utcdt


class TestHourlySlicing:
    def test_single_partial_hour(self):
        since = utcdt('2024-01-15T10:15:00')
        until = utcdt('2024-01-15T10:45:00')
        slices = list(hourly_slicing('main_jobevent', None, since=since, until=until))
        assert slices == [(since, until)]

    def test_crosses_one_hour_boundary(self):
        since = utcdt('2024-01-15T10:15:00')
        until = utcdt('2024-01-15T11:30:00')
        slices = list(hourly_slicing('main_jobevent', None, since=since, until=until))
        assert slices == [
            (utcdt('2024-01-15T10:15:00'), utcdt('2024-01-15T11:00:00')),
            (utcdt('2024-01-15T11:00:00'), utcdt('2024-01-15T11:30:00')),
        ]

    def test_multiple_full_hours(self):
        since = utcdt('2024-01-15T10:00:00')
        until = utcdt('2024-01-15T13:00:00')
        slices = list(hourly_slicing('main_jobevent', None, since=since, until=until))
        assert slices == [
            (utcdt('2024-01-15T10:00:00'), utcdt('2024-01-15T11:00:00')),
            (utcdt('2024-01-15T11:00:00'), utcdt('2024-01-15T12:00:00')),
            (utcdt('2024-01-15T12:00:00'), utcdt('2024-01-15T13:00:00')),
        ]

    def test_partial_start_and_end(self):
        since = utcdt('2024-01-15T10:30:00')
        until = utcdt('2024-01-15T13:15:00')
        slices = list(hourly_slicing('main_jobevent', None, since=since, until=until))
        assert slices == [
            (utcdt('2024-01-15T10:30:00'), utcdt('2024-01-15T11:00:00')),
            (utcdt('2024-01-15T11:00:00'), utcdt('2024-01-15T12:00:00')),
            (utcdt('2024-01-15T12:00:00'), utcdt('2024-01-15T13:00:00')),
            (utcdt('2024-01-15T13:00:00'), utcdt('2024-01-15T13:15:00')),
        ]

    def test_since_equals_until(self):
        t = utcdt('2024-01-15T10:00:00')
        slices = list(hourly_slicing('main_jobevent', None, since=t, until=t))
        assert slices == []

    @patch('metrics_utility.automation_controller_billing.collectors.get_last_entries_from_db')
    @patch('metrics_utility.automation_controller_billing.collectors.now')
    def test_falls_back_to_db_entries(self, mock_now, mock_get_last_entries):
        mock_now.return_value = utcdt('2024-01-15T12:00:00')
        mock_get_last_entries.return_value = {'main_jobevent': utcdt('2024-01-15T10:00:00')}

        slices = list(hourly_slicing('main_jobevent', utcdt('2024-01-01T00:00:00')))
        assert slices[0][0] == utcdt('2024-01-15T10:00:00')
        assert len(slices) == 2

    @patch('metrics_utility.automation_controller_billing.collectors.get_last_entries_from_db')
    @patch('metrics_utility.automation_controller_billing.collectors.now')
    def test_horizon_clamp(self, mock_now, mock_get_last_entries):
        mock_now.return_value = utcdt('2024-02-15T12:00:00')
        mock_get_last_entries.return_value = {'main_jobevent': utcdt('2024-01-01T00:00:00')}

        slices = list(hourly_slicing('main_jobevent', utcdt('2023-01-01T00:00:00')))
        horizon = utcdt('2024-01-18T12:00:00')  # 28 days back from until
        assert slices[0][0] == horizon

    def test_crosses_midnight(self):
        since = utcdt('2024-01-15T23:30:00')
        until = utcdt('2024-01-16T01:30:00')
        slices = list(hourly_slicing('main_jobevent', None, since=since, until=until))
        assert slices == [
            (utcdt('2024-01-15T23:30:00'), utcdt('2024-01-16T00:00:00')),
            (utcdt('2024-01-16T00:00:00'), utcdt('2024-01-16T01:00:00')),
            (utcdt('2024-01-16T01:00:00'), utcdt('2024-01-16T01:30:00')),
        ]
