#!/usr/bin/env python3
"""
Standalone test for total_workers_vcpu function
This test doesn't require Django setup and can be run directly.
"""

import os
import sys

from unittest.mock import MagicMock, patch


# Add the metrics_utility directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'metrics_utility'))


def test_config_exception_fix():
    """Test that ConfigException is properly handled in the mock setup for K8s API calls."""

    # Mock the collectors module functions
    with (
        patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
        patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
        patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
    ):
        # Import the function after setting up the mocks
        from metrics_utility.automation_controller_billing.collectors import total_workers_vcpu

        # Set up the mocks
        mock_get.return_value = ['total_workers_vcpu']
        mock_kube_config.ConfigException = Exception  # Mock the exception class
        mock_kube_config.load_incluster_config.side_effect = mock_kube_config.ConfigException('not in cluster')
        mock_kube_config.load_kube_config.return_value = None

        # Mock the API instance and nodes
        mock_api = MagicMock()
        mock_client.CoreV1Api.return_value = mock_api

        # Create mock nodes
        mock_node1 = MagicMock()
        mock_node1.metadata.name = 'node1'
        mock_node1.status.capacity = {'cpu': '4', 'memory': '8Gi'}

        mock_node2 = MagicMock()
        mock_node2.metadata.name = 'node2'
        mock_node2.status.capacity = {'cpu': '2', 'memory': '4Gi'}

        mock_nodes = MagicMock()
        mock_nodes.items = [mock_node1, mock_node2]
        mock_api.list_node.return_value = mock_nodes

        # Set environment variables - need to enable vcpu count to test K8s API
        os.environ['METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME'] = 'test-cluster'
        os.environ['METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED'] = 'true'

        try:
            result = total_workers_vcpu(None, None, None)

            # Verify the result
            assert result is not None, 'Function returned None instead of expected result'
            assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 6}
            print('✅ ConfigException test passed!')

        finally:
            # Clean up environment variables
            os.environ.pop('METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME', None)
            os.environ.pop('METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED', None)


def test_kubernetes_config_failure():
    """Test that the function raises exception when Kubernetes configuration fails."""

    # Mock the collectors module functions
    with (
        patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
        patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
        patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
    ):
        # Import the function after setting up the mocks
        from metrics_utility.automation_controller_billing.collectors import total_workers_vcpu

        # Set up the mocks - both config methods fail
        mock_get.return_value = ['total_workers_vcpu']
        mock_kube_config.ConfigException = Exception  # Mock the exception class
        mock_kube_config.load_incluster_config.side_effect = mock_kube_config.ConfigException('not in cluster')
        mock_kube_config.load_kube_config.side_effect = mock_kube_config.ConfigException('no kube config')

        # Set environment variables - need to enable vcpu count to reach K8s config code
        os.environ['METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME'] = 'test-cluster'
        os.environ['METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED'] = 'true'

        try:
            exception_raised = False
            try:
                total_workers_vcpu(None, None, None)
            except Exception as e:
                exception_raised = True
                assert 'Could not configure Kubernetes Python client ERROR:' in str(e)

            # Verify that an exception was raised
            assert exception_raised, 'Function should raise exception when Kubernetes config fails'

            # Verify that an error was logged
            mock_logger.error.assert_called_once()

            print('✅ Kubernetes config failure test passed!')

        finally:
            # Clean up environment variables
            os.environ.pop('METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME', None)
            os.environ.pop('METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED', None)


def test_cluster_name_not_set():
    """Test that the function raises MissingRequiredEnvVar when cluster name is not set."""

    # Mock the collectors module functions
    with (
        patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
        patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
    ):
        # Import the function after setting up the mocks
        from metrics_utility.automation_controller_billing.collectors import total_workers_vcpu
        from metrics_utility.exceptions import MissingRequiredEnvVar

        # Set up the mocks
        mock_get.return_value = ['total_workers_vcpu']

        # Make sure cluster name is not set
        os.environ.pop('METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME', None)

        try:
            exception_raised = False
            try:
                total_workers_vcpu(None, None, None)
            except MissingRequiredEnvVar as e:
                exception_raised = True
                assert 'environment variable METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME is not set' in str(e)

            # Verify that an exception was raised
            assert exception_raised, 'Function should raise MissingRequiredEnvVar when cluster name is not set'

            # Verify that an error was logged
            mock_logger.error.assert_called_once_with('environment variable METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME is not set')

            print('✅ Cluster name not set test passed!')

        finally:
            pass  # No cleanup needed since we removed the env var


def test_vcpu_count_disabled_default_behavior():
    """Test that the function returns hardcoded value when vcpu count is disabled (default)."""

    # Mock the collectors module functions
    with (
        patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
    ):
        # Import the function after setting up the mocks
        from metrics_utility.automation_controller_billing.collectors import total_workers_vcpu

        # Set up the mocks
        mock_get.return_value = ['total_workers_vcpu']

        # Set environment variables - don't set vcpu count enabled (default behavior)
        os.environ['METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME'] = 'test-cluster'
        os.environ.pop('METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED', None)

        try:
            result = total_workers_vcpu(None, None, None)

            # Verify the result - should return hardcoded value
            assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': '1'}

            print('✅ vCPU count disabled default behavior test passed!')

        finally:
            # Clean up environment variables
            os.environ.pop('METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME', None)


if __name__ == '__main__':
    test_config_exception_fix()
    test_kubernetes_config_failure()
    test_cluster_name_not_set()
    test_vcpu_count_disabled_default_behavior()
    print('All tests passed!')
