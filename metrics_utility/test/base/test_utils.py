"""Test suite for base.utils module."""

from unittest.mock import patch

import pytest

from metrics_utility.base.utils import bool_from_env, get_max_gather_period_days, get_optional_collectors


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


class TestGetMaxGatherPeriodDays:
    """Test get_max_gather_period_days utility function."""

    def test_returns_default_28_when_env_not_set(self):
        with patch.dict('os.environ', {}, clear=True):
            assert get_max_gather_period_days() == 28

    def test_returns_int_from_env_var(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '14'}):
            assert get_max_gather_period_days() == 14

    def test_returns_1_when_set_to_1(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '1'}):
            assert get_max_gather_period_days() == 1

    def test_raises_on_non_integer_value(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': 'not_a_number'}), pytest.raises(ValueError):
            get_max_gather_period_days()


class TestGetOptionalCollectors:
    """Test get_optional_collectors utility function."""

    def test_returns_default_main_jobevent_and_indirect_when_env_not_set(self):
        with patch.dict('os.environ', {}, clear=True):
            assert get_optional_collectors() == ['main_jobevent', 'main_indirectmanagednodeaudit']

    def test_returns_single_value(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host'}):
            assert get_optional_collectors() == ['main_host']

    def test_returns_multiple_comma_separated_values(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,main_jobevent'}):
            assert get_optional_collectors() == ['main_host', 'main_jobevent']

    def test_strips_leading_trailing_whitespace_from_whole_string(self):
        """The function strips the whole string but not individual element whitespace."""
        with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': ' main_host,main_jobevent '}):
            result = get_optional_collectors()
            assert 'main_host' in result
            assert 'main_jobevent' in result

    def test_empty_string_returns_empty_list(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': ''}):
            assert get_optional_collectors() == []

    def test_filters_empty_segments_from_trailing_comma(self):
        with patch.dict('os.environ', {'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,'}):
            result = get_optional_collectors()
            assert '' not in result
            assert 'main_host' in result
