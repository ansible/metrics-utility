"""Test suite for base.utils module."""

from unittest.mock import patch

import pytest

from metrics_utility.base.utils import bool_from_env, get_gather_interval_hours, get_max_gather_period_days


class TestBoolFromEnv:
    """Test bool_from_env utility function."""

    def test_returns_true_for_string_1(self):
        """Test that '1' returns True."""
        with patch.dict('os.environ', {'TEST_VAR': '1'}):
            assert bool_from_env('TEST_VAR') is True

    def test_returns_true_for_string_true_lowercase(self):
        """Test that 'true' returns True."""
        with patch.dict('os.environ', {'TEST_VAR': 'true'}):
            assert bool_from_env('TEST_VAR') is True

    def test_returns_true_for_string_true_uppercase(self):
        """Test that 'TRUE' returns True."""
        with patch.dict('os.environ', {'TEST_VAR': 'TRUE'}):
            assert bool_from_env('TEST_VAR') is True

    def test_returns_true_for_string_true_mixedcase(self):
        """Test that 'TrUe' returns True."""
        with patch.dict('os.environ', {'TEST_VAR': 'TrUe'}):
            assert bool_from_env('TEST_VAR') is True

    def test_returns_false_for_string_0(self):
        """Test that '0' returns False."""
        with patch.dict('os.environ', {'TEST_VAR': '0'}):
            assert bool_from_env('TEST_VAR') is False

    def test_returns_false_for_string_false_lowercase(self):
        """Test that 'false' returns False."""
        with patch.dict('os.environ', {'TEST_VAR': 'false'}):
            assert bool_from_env('TEST_VAR') is False

    def test_returns_false_for_string_false_uppercase(self):
        """Test that 'FALSE' returns False."""
        with patch.dict('os.environ', {'TEST_VAR': 'FALSE'}):
            assert bool_from_env('TEST_VAR') is False

    def test_returns_false_for_arbitrary_string(self):
        """Test that arbitrary strings return False."""
        with patch.dict('os.environ', {'TEST_VAR': 'random'}):
            assert bool_from_env('TEST_VAR') is False

    def test_returns_false_for_empty_string(self):
        """Test that empty string returns False."""
        with patch.dict('os.environ', {'TEST_VAR': ''}):
            assert bool_from_env('TEST_VAR') is False

    def test_returns_none_when_not_set_and_no_default(self):
        """Test that missing env var returns None when no default provided."""
        with patch.dict('os.environ', {}, clear=True):
            assert bool_from_env('MISSING_VAR') is None

    def test_returns_default_when_not_set(self):
        """Test that missing env var returns default value."""
        with patch.dict('os.environ', {}, clear=True):
            assert bool_from_env('MISSING_VAR', default=False) is False

    def test_returns_custom_default_when_not_set(self):
        """Test that missing env var returns custom default."""
        with patch.dict('os.environ', {}, clear=True):
            assert bool_from_env('MISSING_VAR', default=True) is True

    def test_returns_none_default_explicitly(self):
        """Test that explicit None default works."""
        with patch.dict('os.environ', {}, clear=True):
            assert bool_from_env('MISSING_VAR', default=None) is None


class TestGetGatherIntervalHours:
    """Test get_gather_interval_hours utility function."""

    def test_returns_default_24_when_not_set(self):
        with patch.dict('os.environ', {}, clear=True):
            assert get_gather_interval_hours() == 24

    def test_returns_configured_value(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': '4'}):
            assert get_gather_interval_hours() == 4

    def test_returns_1_minimum(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': '1'}):
            assert get_gather_interval_hours() == 1

    def test_raises_on_zero(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': '0'}):
            with pytest.raises(ValueError, match='must be >= 1'):
                get_gather_interval_hours()

    def test_raises_on_negative(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': '-5'}):
            with pytest.raises(ValueError, match='must be >= 1'):
                get_gather_interval_hours()

    def test_raises_on_non_integer(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_GATHER_INTERVAL_HOURS': 'bad'}):
            with pytest.raises((ValueError, TypeError)):
                get_gather_interval_hours()


class TestGetMaxGatherPeriodDays:
    """Test get_max_gather_period_days utility function."""

    def test_returns_default_28_when_not_set(self):
        with patch.dict('os.environ', {}, clear=True):
            assert get_max_gather_period_days() == 28

    def test_returns_configured_value(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '14'}):
            assert get_max_gather_period_days() == 14

    def test_raises_on_non_integer(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': 'bad'}):
            with pytest.raises((ValueError, TypeError)):
                get_max_gather_period_days()
