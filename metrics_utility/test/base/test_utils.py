"""Test suite for base.utils module."""

from unittest.mock import patch

from metrics_utility.base.utils import bool_from_env


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
