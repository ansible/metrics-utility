import os

from typing import Optional

from kubernetes import client
from kubernetes import config as kube_config

from metrics_utility.exceptions import MetricsException
from metrics_utility.logger import logger


class KubernetesClient:
    """
    Kubernetes client for service account token operations.

    This class handles:
    - Kubernetes API client initialization (in-cluster and local kubeconfig)
    - Service account token retrieval using multiple methods
    - Error handling for Kubernetes operations
    """

    def __init__(self):
        self.api_instance = None
        self.auth_api = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize Kubernetes API clients"""
        try:
            # Try in-cluster config first (when running inside a pod)
            kube_config.load_incluster_config()
            logger.info('Loaded in-cluster Kubernetes config')
        except kube_config.ConfigException:
            try:
                # Fall back to kubeconfig file (local development)
                kube_config.load_kube_config()
                logger.info('Loaded kubeconfig from local file')
            except kube_config.ConfigException as e:
                error_msg = f'Could not configure Kubernetes Python client ERROR: {e}'
                raise MetricsException(error_msg)

        # Create API clients
        self.api_instance = client.CoreV1Api()
        if not self.api_instance:
            raise MetricsException('Could not get a Kube CoreV1Api client')

        self.auth_api = client.AuthenticationV1Api()

        logger.info('Kubernetes API clients initialized')

    def get_service_account_token(self, service_account_name: str, namespace: str = 'default') -> Optional[str]:
        """
        Get a token for a service account in a given namespace.

        Args:
            service_account_name: Name of the service account
            namespace: Kubernetes namespace (default: "default")

        Returns:
            Service account token string, or None if not found
        """

        try:
            # Create a token request
            token_request = {'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest', 'spec': {'expirationSeconds': 3600}}

            response = self.api_instance.create_namespaced_service_account_token(name=service_account_name, namespace=namespace, body=token_request)

            if response.status and response.status.token:
                logger.info('Got token via CoreV1Api')
                return response.status.token

        except Exception as e:
            logger.info(f'TokenRequest API failed: {e}')
            return None

    def verify_service_account_exists(self, service_account_name: str, namespace: str = 'default') -> bool:
        """
        Verify that a service account exists.

        Args:
            service_account_name: Name of the service account
            namespace: Kubernetes namespace

        Returns:
            True if service account exists, False otherwise
        """
        try:
            self.api_instance.read_namespaced_service_account(name=service_account_name, namespace=namespace)
            return True
        except Exception as e:
            logger.info(f"Service account '{service_account_name}' not found in namespace '{namespace}': {e}")
            return False

    def get_current_pod_info(self) -> dict:
        """
        Get information about the current pod's service account and namespace.

        This works when running inside a Kubernetes pod, where Kubernetes
        automatically mounts service account information.

        Returns:
            Dictionary with pod information, or empty dict if not in a pod
        """
        info = {}

        try:
            # Standard Kubernetes service account mount paths
            namespace_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
            token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'

            # Get namespace
            if os.path.exists(namespace_path):
                with open(namespace_path, 'r') as f:
                    info['namespace'] = f.read().strip()
                logger.info(f'Current pod namespace: {info["namespace"]}')

            # Check if token exists (indicates we're in a pod)
            if os.path.exists(token_path):
                info['has_mounted_token'] = True
                logger.info('Pod has mounted service account token')

            # Try to get service account name from environment or pod metadata
            # Some deployments set this as an environment variable
            sa_name = os.environ.get('SERVICE_ACCOUNT_NAME')
            if sa_name:
                info['service_account_name'] = sa_name
                logger.info(f'Service account from environment: {sa_name}')
            else:
                # Default service account name if not specified
                info['service_account_name'] = 'default'
                logger.info('Using default service account name')

            return info

        except Exception as e:
            logger.error(f'Error getting current pod info: {e}')
            return {}

    def create_token_for_current_service_account(self, use_mounted_token: bool = False, expiration_seconds: int = 3600) -> Optional[str]:
        """
        Get or create a token for the current pod's service account.

        This method:
        1. First checks for existing mounted token (if use_mounted_token=True)
        2. If no mounted token, detects current pod's service account and namespace
        3. Creates a new token using the TokenRequest API
        4. Returns the token for use with external services

        Args:
            expiration_seconds: Token expiration time for new tokens (default: 1 hour)
            use_mounted_token: Whether to use existing mounted token first (default: True)

        Returns:
            Service account token (mounted or newly created), or None if creation fails
        """
        try:
            # First, try to use the existing mounted token if requested
            if use_mounted_token:
                mounted_token = self.get_current_mounted_token()
                if mounted_token:
                    logger.info('Using existing mounted service account token')
                    return mounted_token
                else:
                    logger.info('No mounted token found, will create new token via API')

            # Get current pod information for creating new token
            pod_info = self.get_current_pod_info()

            if not pod_info:
                logger.error('Could not determine current pod information')
                return None

            namespace = pod_info.get('namespace')
            service_account_name = pod_info.get('service_account_name')

            if not namespace or not service_account_name:
                logger.error('Missing namespace or service account name from pod info')
                return None

            logger.info(f"Creating new token for current service account '{service_account_name}' in namespace '{namespace}'")

            # Verify the service account exists
            if not self.verify_service_account_exists(service_account_name, namespace):
                logger.error(f"Service account '{service_account_name}' does not exist in namespace '{namespace}'")
                return None

            # Create the token using the existing method
            token = self.get_service_account_token(service_account_name, namespace)

            if token:
                logger.info('Successfully created new token for current service account')
                logger.info(f'   Service Account: {service_account_name}')
                logger.info(f'   Namespace: {namespace}')
                logger.info(f'   Token Length: {len(token)} characters')
                return token
            else:
                logger.error('Failed to create token for current service account')
                return None

        except Exception as e:
            logger.error(f'Error getting/creating token for current service account: {e}')
            return None

    def get_current_mounted_token(self) -> Optional[str]:
        """
        Get the current pod's mounted service account token.

        This is different from create_token_for_current_service_account() -
        this just reads the existing mounted token, while the other method
        creates a new token via the API.

        Returns:
            Current mounted token, or None if not available
        """
        token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'

        try:
            if os.path.exists(token_path):
                with open(token_path, 'r') as f:
                    token = f.read().strip()
                logger.info("Retrieved current pod's mounted token")
                logger.info(f'   Token Length: {len(token)} characters')
                return token
            else:
                logger.warning('Not running in a Kubernetes pod (no mounted token found)')
                return None

        except Exception as e:
            logger.error(f'Error reading mounted token: {e}')
            return None
