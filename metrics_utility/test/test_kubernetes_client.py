"""
Unit tests for KubernetesClient class.

This module contains comprehensive tests for the KubernetesClient class,
including mocking of Kubernetes API calls and file system operations.
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from kubernetes.config import ConfigException

from metrics_utility.automation_controller_billing.kubernetes_client import KubernetesClient
from metrics_utility.exceptions import MetricsException


class TestKubernetesClient:
    """Test cases for KubernetesClient class."""

    def test_init_success_incluster_config(self):
        """Test successful initialization with in-cluster config."""
        with (
            patch('kubernetes.config.load_incluster_config') as mock_incluster,
            patch('kubernetes.config.load_kube_config') as mock_kubeconfig,
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client
            client_instance = KubernetesClient()

            # Assertions
            mock_incluster.assert_called_once()
            mock_kubeconfig.assert_not_called()
            mock_core_api.assert_called_once()
            mock_auth_api.assert_called_once()
            assert client_instance.api_instance is not None
            assert client_instance.auth_api is not None

    def test_init_success_kubeconfig_fallback(self):
        """Test successful initialization with kubeconfig fallback."""
        with (
            patch('kubernetes.config.load_incluster_config') as mock_incluster,
            patch('kubernetes.config.load_kube_config') as mock_kubeconfig,
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_incluster.side_effect = ConfigException('Not in cluster')
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client
            client_instance = KubernetesClient()

            # Assertions
            mock_incluster.assert_called_once()
            mock_kubeconfig.assert_called_once()
            mock_core_api.assert_called_once()
            mock_auth_api.assert_called_once()
            assert client_instance.api_instance is not None
            assert client_instance.auth_api is not None

    def test_init_failure_no_config(self):
        """Test initialization failure when no config is available."""
        with patch('kubernetes.config.load_incluster_config') as mock_incluster, patch('kubernetes.config.load_kube_config') as mock_kubeconfig:
            # Setup mocks to fail
            mock_incluster.side_effect = ConfigException('Not in cluster')
            mock_kubeconfig.side_effect = ConfigException('No kubeconfig')

            # Test that MetricsException is raised
            with pytest.raises(MetricsException, match='Could not configure Kubernetes Python client'):
                KubernetesClient()

    def test_init_failure_no_core_api(self):
        """Test initialization failure when CoreV1Api cannot be created."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = None
            mock_auth_api.return_value = MagicMock()

            # Test that MetricsException is raised
            with pytest.raises(MetricsException, match='Could not get a Kube CoreV1Api client'):
                KubernetesClient()

    def test_get_service_account_token_success(self):
        """Test successful service account token retrieval."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mock response
            mock_response = MagicMock()
            mock_response.status.token = 'test-token-12345'

            mock_api_instance = MagicMock()
            mock_api_instance.create_namespaced_service_account_token.return_value = mock_response
            mock_core_api.return_value = mock_api_instance
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_service_account_token('test-sa', 'test-namespace')

            # Assertions
            assert token == 'test-token-12345'
            mock_api_instance.create_namespaced_service_account_token.assert_called_once_with(
                name='test-sa',
                namespace='test-namespace',
                body={'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest', 'spec': {'expirationSeconds': 3600}},
            )

    def test_get_service_account_token_failure(self):
        """Test service account token retrieval failure."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mock to raise exception
            mock_api_instance = MagicMock()
            mock_api_instance.create_namespaced_service_account_token.side_effect = Exception('API Error')
            mock_core_api.return_value = mock_api_instance
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_service_account_token('test-sa', 'test-namespace')

            # Assertions
            assert token is None

    def test_get_service_account_token_no_token_in_response(self):
        """Test service account token retrieval when response has no token."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mock response without token
            mock_response = MagicMock()
            mock_response.status = None

            mock_api_instance = MagicMock()
            mock_api_instance.create_namespaced_service_account_token.return_value = mock_response
            mock_core_api.return_value = mock_api_instance
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_service_account_token('test-sa', 'test-namespace')

            # Assertions
            assert token is None

    def test_verify_service_account_exists_true(self):
        """Test service account existence verification when it exists."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mock
            mock_api_instance = MagicMock()
            mock_api_instance.read_namespaced_service_account.return_value = MagicMock()
            mock_core_api.return_value = mock_api_instance
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            exists = client_instance.verify_service_account_exists('test-sa', 'test-namespace')

            # Assertions
            assert exists is True
            mock_api_instance.read_namespaced_service_account.assert_called_once_with(name='test-sa', namespace='test-namespace')

    def test_verify_service_account_exists_false(self):
        """Test service account existence verification when it doesn't exist."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mock to raise exception
            mock_api_instance = MagicMock()
            mock_api_instance.read_namespaced_service_account.side_effect = Exception('Not found')
            mock_core_api.return_value = mock_api_instance
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            exists = client_instance.verify_service_account_exists('test-sa', 'test-namespace')

            # Assertions
            assert exists is False

    def test_get_current_pod_info_success(self):
        """Test successful retrieval of current pod information."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists') as mock_exists,
            patch.dict('os.environ', {'SERVICE_ACCOUNT_NAME': 'custom-sa'}),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()
            mock_exists.side_effect = lambda path: path in [
                '/var/run/secrets/kubernetes.io/serviceaccount/namespace',
                '/var/run/secrets/kubernetes.io/serviceaccount/token',
            ]

            # Create client and test
            client_instance = KubernetesClient()
            pod_info = client_instance.get_current_pod_info()

            # Assertions
            assert pod_info['namespace'] == 'test-namespace'
            assert pod_info['has_mounted_token'] is True
            assert pod_info['service_account_name'] == 'custom-sa'

    def test_get_current_pod_info_default_sa(self):
        """Test pod info retrieval with default service account."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists') as mock_exists,
            patch.dict('os.environ', {}, clear=True),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()
            mock_exists.side_effect = lambda path: path == '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

            # Create client and test
            client_instance = KubernetesClient()
            pod_info = client_instance.get_current_pod_info()

            # Assertions
            assert pod_info['namespace'] == 'test-namespace'
            assert pod_info.get('has_mounted_token') is None
            assert pod_info['service_account_name'] == 'default'

    def test_get_current_pod_info_no_files(self):
        """Test pod info retrieval when no pod files exist."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists', return_value=False),
            patch.dict('os.environ', {}, clear=True),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            pod_info = client_instance.get_current_pod_info()

            # Assertions
            assert pod_info.get('namespace') is None
            assert pod_info.get('has_mounted_token') is None
            assert pod_info['service_account_name'] == 'default'

    def test_get_current_pod_info_exception(self):
        """Test pod info retrieval when an exception occurs."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists', side_effect=Exception('File system error')),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            pod_info = client_instance.get_current_pod_info()

            # Assertions
            assert pod_info == {}

    def test_get_current_mounted_token_success(self):
        """Test successful retrieval of mounted token."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists', return_value=True),
            patch('builtins.open', mock_open(read_data='mounted-token-12345\n')),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_current_mounted_token()

            # Assertions
            assert token == 'mounted-token-12345'

    def test_get_current_mounted_token_no_file(self):
        """Test mounted token retrieval when file doesn't exist."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists', return_value=False),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_current_mounted_token()

            # Assertions
            assert token is None

    def test_get_current_mounted_token_exception(self):
        """Test mounted token retrieval when an exception occurs."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
            patch('os.path.exists', return_value=True),
            patch('builtins.open', side_effect=IOError('Permission denied')),
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and test
            client_instance = KubernetesClient()
            token = client_instance.get_current_mounted_token()

            # Assertions
            assert token is None

    def test_create_token_for_current_service_account_use_mounted_token(self):
        """Test token creation using mounted token."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and mock the method
            client_instance = KubernetesClient()
            client_instance.get_current_mounted_token = MagicMock(return_value='mounted-token')

            # Test
            token = client_instance.create_token_for_current_service_account(use_mounted_token=True)

            # Assertions
            assert token == 'mounted-token'
            client_instance.get_current_mounted_token.assert_called_once()

    def test_create_token_for_current_service_account_create_new(self):
        """Test token creation via API when mounted token not requested."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and mock methods
            client_instance = KubernetesClient()
            client_instance.get_current_pod_info = MagicMock(return_value={'namespace': 'test-namespace', 'service_account_name': 'test-sa'})
            client_instance.verify_service_account_exists = MagicMock(return_value=True)
            client_instance.get_service_account_token = MagicMock(return_value='new-token')

            # Test
            token = client_instance.create_token_for_current_service_account(use_mounted_token=False)

            # Assertions
            assert token == 'new-token'
            client_instance.get_current_pod_info.assert_called_once()
            client_instance.verify_service_account_exists.assert_called_once_with('test-sa', 'test-namespace')
            client_instance.get_service_account_token.assert_called_once_with('test-sa', 'test-namespace')

    def test_create_token_for_current_service_account_no_pod_info(self):
        """Test token creation failure when pod info cannot be retrieved."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and mock methods
            client_instance = KubernetesClient()
            client_instance.get_current_pod_info = MagicMock(return_value={})

            # Test
            token = client_instance.create_token_for_current_service_account(use_mounted_token=False)

            # Assertions
            assert token is None

    def test_create_token_for_current_service_account_missing_info(self):
        """Test token creation failure when namespace or service account is missing."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and mock methods
            client_instance = KubernetesClient()
            client_instance.get_current_pod_info = MagicMock(
                return_value={
                    'namespace': 'test-namespace'
                    # Missing service_account_name
                }
            )

            # Test
            token = client_instance.create_token_for_current_service_account(use_mounted_token=False)

            # Assertions
            assert token is None

    def test_create_token_for_current_service_account_sa_not_exists(self):
        """Test token creation failure when service account doesn't exist."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and mock methods
            client_instance = KubernetesClient()
            client_instance.get_current_pod_info = MagicMock(return_value={'namespace': 'test-namespace', 'service_account_name': 'test-sa'})
            client_instance.verify_service_account_exists = MagicMock(return_value=False)

            # Test
            token = client_instance.create_token_for_current_service_account(use_mounted_token=False)

            # Assertions
            assert token is None

    def test_create_token_for_current_service_account_exception(self):
        """Test token creation failure when an exception occurs."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mocks
            mock_core_api.return_value = MagicMock()
            mock_auth_api.return_value = MagicMock()

            # Create client and mock methods
            client_instance = KubernetesClient()
            client_instance.get_current_pod_info = MagicMock(side_effect=Exception('Unexpected error'))

            # Test
            token = client_instance.create_token_for_current_service_account(use_mounted_token=False)

            # Assertions
            assert token is None

    def test_default_namespace_parameter(self):
        """Test that default namespace parameter is used correctly."""
        with (
            patch('kubernetes.config.load_incluster_config'),
            patch('kubernetes.client.CoreV1Api') as mock_core_api,
            patch('kubernetes.client.AuthenticationV1Api') as mock_auth_api,
        ):
            # Setup mock response
            mock_response = MagicMock()
            mock_response.status.token = 'test-token'

            mock_api_instance = MagicMock()
            mock_api_instance.create_namespaced_service_account_token.return_value = mock_response
            mock_core_api.return_value = mock_api_instance
            mock_auth_api.return_value = MagicMock()

            # Create client and test with default namespace
            client_instance = KubernetesClient()
            token = client_instance.get_service_account_token('test-sa')  # No namespace provided

            # Assertions
            assert token == 'test-token'
            mock_api_instance.create_namespaced_service_account_token.assert_called_once_with(
                name='test-sa',
                namespace='default',  # Should use default
                body={'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest', 'spec': {'expirationSeconds': 3600}},
            )
