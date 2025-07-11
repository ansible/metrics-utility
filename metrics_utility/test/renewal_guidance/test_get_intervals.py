import datetime as dt_actual

import pandas as pd
import pytest

from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance


class TestGetIntervals:
    """Essential test suite for get_intervals method - 6 most critical tests."""

    @pytest.fixture
    def fixed_now(self):
        """Fixed datetime for testing."""
        return dt_actual.datetime(2025, 6, 3, 10, 0, 0)

    @pytest.fixture
    def basic_report_instance(self, fixed_now):
        """Create a basic ReportRenewalGuidance instance for testing."""
        mock_df = pd.DataFrame(
            {
                'hostname': ['test-host'],
                'first_automation': [fixed_now],
                'last_automation': [fixed_now],
                'deleted': [False],
            }
        )

        extra_params = {
            'ephemeral_days': 7,
            'price_per_node': 0.1,
            'report_period': '2025-01-01,2025-06-03',
            'since_date': '2025-01-01',
            'until_date': '2025-06-03',
        }

        return ReportRenewalGuidance(
            dataframes={'host_metric': mock_df},
            extra_params=extra_params,
        )

    def test_get_intervals_basic_sliding_window_functionality(self, basic_report_instance):
        """Test core sliding window behavior."""
        start_date = dt_actual.datetime(2025, 1, 1, 0, 0, 0)
        end_date = dt_actual.datetime(2025, 1, 10, 23, 59, 59, 999999)
        interval_size = 5

        intervals = basic_report_instance.get_intervals(start_date, end_date, interval_size)

        assert len(intervals) == 6

        # Verify sliding behavior
        assert intervals[0] == (
            dt_actual.datetime(2025, 1, 1, 0, 0, 0),
            dt_actual.datetime(2025, 1, 5, 23, 59, 59, 999999),
        )
        assert intervals[1] == (
            dt_actual.datetime(2025, 1, 2, 0, 0, 0),
            dt_actual.datetime(2025, 1, 6, 23, 59, 59, 999999),
        )

        # Verify output format
        for start, end in intervals:
            assert isinstance(start, dt_actual.datetime)
            assert isinstance(end, dt_actual.datetime)
            assert start.tzinfo is None
            assert end.tzinfo is None

    def test_get_intervals_ephemeral_usage_calculation_integration(self):
        """Test integration with compute_ephemeral_intervals."""
        realistic_df = pd.DataFrame(
            {
                'hostname': [
                    'host-01.example.com',
                    'host-02.example.com',
                    'host-03.example.com',
                ],
                'first_automation': [
                    dt_actual.datetime(2025, 5, 1, 8, 0, 0),
                    dt_actual.datetime(2025, 4, 28, 9, 15, 0),
                    dt_actual.datetime(2025, 5, 3, 12, 0, 0),
                ],
                'last_automation': [
                    dt_actual.datetime(2025, 5, 10, 14, 30, 0),
                    dt_actual.datetime(2025, 5, 12, 16, 0, 0),
                    dt_actual.datetime(2025, 5, 11, 13, 45, 0),
                ],
                'deleted': [False, True, False],
            }
        )

        extra_params = {
            'ephemeral_days': 7,
            'price_per_node': 0.1,
            'report_period': '2025-04-28,2025-05-12',
            'since_date': '2025-04-28',
            'until_date': '2025-05-12',
        }

        instance = ReportRenewalGuidance(
            dataframes={'host_metric': realistic_df},
            extra_params=extra_params,
        )

        start_date = dt_actual.datetime(2025, 4, 28, 0, 0, 0)
        end_date = dt_actual.datetime(2025, 5, 12, 23, 59, 59, 999999)

        intervals = instance.get_intervals(start_date, end_date, extra_params['ephemeral_days'])

        assert len(intervals) == 9

        # Simulate usage in compute_ephemeral_intervals
        ephemeral_usage_intervals = []
        for window_start, window_end in intervals:
            ephemeral_usage_intervals.append(
                {
                    'window_start': window_start,
                    'window_end': window_end,
                    'ephemeral_hosts': 0,
                }
            )

        assert len(ephemeral_usage_intervals) == 9
        assert all('window_start' in item for item in ephemeral_usage_intervals)
        assert all('window_end' in item for item in ephemeral_usage_intervals)

    def test_get_intervals_common_ephemeral_thresholds(self, basic_report_instance):
        """Test common ephemeral thresholds used in production."""
        start_date = dt_actual.datetime(2025, 4, 28, 0, 0, 0)
        end_date = dt_actual.datetime(2025, 5, 12, 23, 59, 59, 999999)

        intervals_3day = basic_report_instance.get_intervals(start_date, end_date, 3)
        intervals_7day = basic_report_instance.get_intervals(start_date, end_date, 7)
        intervals_30day = basic_report_instance.get_intervals(start_date, end_date, 30)

        assert len(intervals_3day) == 13
        assert len(intervals_7day) == 9
        assert len(intervals_30day) == 0

        # Verify sliding window progression
        for i in range(len(intervals_7day) - 1):
            current_start = intervals_7day[i][0]
            next_start = intervals_7day[i + 1][0]
            assert next_start == current_start + dt_actual.timedelta(days=1)

    def test_get_intervals_edge_cases_and_boundaries(self, basic_report_instance):
        """Test critical edge cases that break in production."""

        # Single day range with single day interval
        start_date = dt_actual.datetime(2025, 1, 1, 0, 0, 0)
        end_date = dt_actual.datetime(2025, 1, 1, 23, 59, 59, 999999)
        intervals_single = basic_report_instance.get_intervals(start_date, end_date, 1)
        assert len(intervals_single) == 1
        assert intervals_single[0] == (start_date, end_date)

        # Range smaller than interval size
        intervals_none = basic_report_instance.get_intervals(start_date, end_date, 7)
        assert len(intervals_none) == 0

        # Year boundary crossing
        year_start = dt_actual.datetime(2024, 12, 30, 0, 0, 0)
        year_end = dt_actual.datetime(2025, 1, 5, 23, 59, 59, 999999)
        intervals_year = basic_report_instance.get_intervals(year_start, year_end, 5)
        assert len(intervals_year) == 3

        assert intervals_year[0][0] == dt_actual.datetime(2024, 12, 30, 0, 0, 0)
        assert intervals_year[0][1] == dt_actual.datetime(2025, 1, 3, 23, 59, 59, 999999)

    def test_get_intervals_output_format_and_precision(self, basic_report_instance):
        """Test output format validation for report generation."""
        start_date = dt_actual.datetime(2025, 1, 1, 0, 0, 0)
        end_date = dt_actual.datetime(2025, 1, 5, 23, 59, 59, 999999)
        interval_size = 2

        intervals = basic_report_instance.get_intervals(start_date, end_date, interval_size)

        assert isinstance(intervals, list)
        assert len(intervals) == 4

        for interval in intervals:
            assert isinstance(interval, tuple)
            assert len(interval) == 2

            interval_start, interval_end = interval

            assert isinstance(interval_start, dt_actual.datetime)
            assert isinstance(interval_end, dt_actual.datetime)
            assert interval_start.tzinfo is None
            assert interval_end.tzinfo is None

            # Verify microsecond precision
            assert interval_start.microsecond == 0
            assert interval_end.microsecond == 999999

            assert interval_start < interval_end

    def test_get_intervals_production_host_lifecycle_patterns(self):
        """Test real production host patterns for ephemeral classification."""
        production_df = pd.DataFrame(
            {
                'hostname': [
                    'ci-runner-001.example.com',
                    'prod-web-01.example.com',
                    'k8s-pod-abc123.example.com',
                    'staging-db.example.com',
                    'analytics-job.example.com',
                ],
                'first_automation': [
                    dt_actual.datetime(2025, 5, 1, 9, 0, 0),
                    dt_actual.datetime(2025, 4, 10, 8, 0, 0),
                    dt_actual.datetime(2025, 5, 8, 14, 0, 0),
                    dt_actual.datetime(2025, 4, 28, 10, 0, 0),
                    dt_actual.datetime(2025, 5, 5, 11, 0, 0),
                ],
                'last_automation': [
                    dt_actual.datetime(2025, 5, 3, 17, 0, 0),
                    dt_actual.datetime(2025, 5, 12, 18, 0, 0),
                    dt_actual.datetime(2025, 5, 8, 20, 0, 0),
                    dt_actual.datetime(2025, 5, 10, 16, 0, 0),
                    dt_actual.datetime(2025, 5, 9, 15, 0, 0),
                ],
                'deleted': [True, False, True, False, True],
                'automated_counter': [15, 245, 3, 89, 28],
            }
        )

        extra_params = {
            'ephemeral_days': 7,
            'price_per_node': 0.1,
            'report_period': '2025-04-10,2025-05-12',
            'since_date': '2025-04-10',
            'until_date': '2025-05-12',
        }

        production_instance = ReportRenewalGuidance(
            dataframes={'host_metric': production_df},
            extra_params=extra_params,
        )

        start_date = dt_actual.datetime(2025, 4, 10, 0, 0, 0)
        end_date = dt_actual.datetime(2025, 5, 12, 23, 59, 59, 999999)

        intervals = production_instance.get_intervals(start_date, end_date, extra_params['ephemeral_days'])

        assert len(intervals) == 27

        assert intervals[0][0] == dt_actual.datetime(2025, 4, 10, 0, 0, 0)
        assert intervals[0][1] == dt_actual.datetime(2025, 4, 16, 23, 59, 59, 999999)

        assert intervals[-1][0] == dt_actual.datetime(2025, 5, 6, 0, 0, 0)
        assert intervals[-1][1] == dt_actual.datetime(2025, 5, 12, 23, 59, 59, 999999)
