"""
Unit tests for simplified KubernetesClient class.

This module contains comprehensive tests for the simplified KubernetesClient class,
which assumes running in a Kubernetes pod with mounted service account files.
"""

from unittest.mock import mock_open, patch

import pytest

from metrics_utility.automation_controller_billing.kubernetes_client import KubernetesClient
from metrics_utility.exceptions import MetricsException


class TestKubernetesClient:
    """Test suite for the simplified KubernetesClient class."""

    def test_init_success(self):
        """Test successful initialization when service account files exist."""
        with (
            patch('os.path.exists') as mock_exists,
        ):
            # Setup mocks - both files exist
            mock_exists.side_effect = lambda path: path in [
                '/var/run/secrets/kubernetes.io/serviceaccount/token',
                '/var/run/secrets/kubernetes.io/serviceaccount/namespace',
            ]

            # Create client
            client_instance = KubernetesClient()

            # Verify initialization succeeded
            assert client_instance is not None

    def test_init_failure_no_token(self):
        """Test initialization failure when token file doesn't exist."""
        with (
            patch('os.path.exists') as mock_exists,
        ):
            # Setup mocks - only namespace exists
            mock_exists.side_effect = lambda path: path == '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

            # Test that MetricsException is raised
            with pytest.raises(MetricsException, match='Service account token not found'):
                KubernetesClient()

    def test_init_failure_no_namespace(self):
        """Test initialization failure when namespace file doesn't exist."""
        with (
            patch('os.path.exists') as mock_exists,
        ):
            # Setup mocks - only token exists
            mock_exists.side_effect = lambda path: path == '/var/run/secrets/kubernetes.io/serviceaccount/token'

            # Test that MetricsException is raised
            with pytest.raises(MetricsException, match='Service account namespace not found'):
                KubernetesClient()

    def test_init_failure_no_files(self):
        """Test initialization failure when no service account files exist."""
        with (
            patch('os.path.exists', return_value=False),
        ):
            # Test that MetricsException is raised
            with pytest.raises(MetricsException, match='Service account token not found'):
                KubernetesClient()

    def test_get_current_token_success(self):
        """Test successful token retrieval."""
        with (
            patch('os.path.exists', return_value=True),
            patch('builtins.open', mock_open(read_data='test-token-12345\n')),
        ):
            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_current_token()

            # Assertions
            assert token == 'test-token-12345'

    def test_get_current_token_failure(self):
        """Test token retrieval failure when file cannot be read."""
        with (
            patch('os.path.exists', return_value=True),
            patch('builtins.open', side_effect=IOError('Permission denied')),
        ):
            # Create client and test
            client_instance = KubernetesClient()

            with pytest.raises(MetricsException, match='Error reading token'):
                client_instance.get_current_token()

    def test_multiple_token_operations(self):
        """Test multiple token operations on the same client instance."""
        with (
            patch('os.path.exists', return_value=True),
            patch('builtins.open', mock_open(read_data='multi-op-token\n')),
        ):
            # Create client and perform multiple operations
            client_instance = KubernetesClient()

            token1 = client_instance.get_current_token()
            token2 = client_instance.get_current_token()

            # Assertions - both calls should return the same token
            assert token1 == 'multi-op-token'
            assert token2 == 'multi-op-token'
