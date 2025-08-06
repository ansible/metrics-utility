import json

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.automation_controller_billing.collectors import total_workers_vcpu
from metrics_utility.exceptions import MetricsException, MissingRequiredEnvVar
from metrics_utility.test.util import temporary_env


class TestTotalWorkersVcpu:
    """Test suite for the total_workers_vcpu collector function."""

    def test_returns_none_when_not_in_optional_collectors(self):
        """Test that the function returns None when total_workers_vcpu is not in optional collectors."""
        with patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get:
            mock_get.return_value = []
            result = total_workers_vcpu(None, None, None)
            assert result is None

    def test_raises_metrics_exception_when_cluster_name_not_set(self):
        """Test that the function raises MissingRequiredEnvVar when METRICS_UTILITY_CLUSTER_NAME is not set."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': None}):
                with pytest.raises(MissingRequiredEnvVar) as exc_info:
                    total_workers_vcpu(None, None, None)
                assert 'environment variable METRICS_UTILITY_CLUSTER_NAME is not set' in str(exc_info.value)
                mock_logger.error.assert_called_once_with('environment variable METRICS_UTILITY_CLUSTER_NAME is not set')

    def test_returns_hardcoded_value_when_vcpu_count_disabled(self):
        """Test that the function returns hardcoded value when METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED is not set or false (default behavior)."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            # Test when not set (default behavior)
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster'}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 1}

                # Verify the logged JSON contains usage_based_billing_enabled = False
                logged_json = json.loads(mock_logger_info.info.call_args[0][0])
                assert not logged_json['usage_based_billing_enabled']

            # Test when explicitly set to false
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'false'}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 1}

                # Verify the logged JSON contains usage_based_billing_enabled = False
                logged_json = json.loads(mock_logger_info.info.call_args[0][0])
                assert not logged_json['usage_based_billing_enabled']

    def test_usage_based_billing_enabled_case_insensitive(self):
        """Test that METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED is case insensitive."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance and nodes
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            mock_node1 = MagicMock()
            mock_node1.metadata.name = 'worker-node-1'
            mock_node1.status.capacity = {'cpu': '4'}

            mock_nodes = MagicMock()
            mock_nodes.items = [mock_node1]
            mock_api.list_node.return_value = mock_nodes

            # Test TRUE (case insensitive)
            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'TRUE'}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 4}

    def test_usage_based_billing_enabled_true_continues_to_k8s_api(self):
        """Test that when METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED is true, it continues to K8s API."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
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

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 6}

    def test_usage_based_billing_disabled_unset_returns_hardcoded_value(self):
        """Test that when METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED is unset, it returns hardcoded value."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': None}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 1}

                # Verify the logged JSON contains usage_based_billing_enabled = False
                logged_json = json.loads(mock_logger_info.info.call_args[0][0])
                assert not logged_json['usage_based_billing_enabled']

    def test_kubernetes_config_exception_handling(self):
        """Test that the function properly handles Kubernetes configuration exceptions."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.logger') as mock_logger,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.ConfigException = Exception  # Mock the exception class
            mock_kube_config.load_incluster_config.side_effect = mock_kube_config.ConfigException('not in cluster')
            mock_kube_config.load_kube_config.side_effect = mock_kube_config.ConfigException('no kube config')

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                with pytest.raises(Exception) as exc_info:
                    total_workers_vcpu(None, None, None)
                assert 'Could not configure Kubernetes Python client ERROR:' in str(exc_info.value)
                mock_logger.error.assert_called_once()

    def test_corev1api_client_none_raises_exception(self):
        """Test that the function raises MetricsException when CoreV1Api client is None."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock CoreV1Api to return None
            mock_client.CoreV1Api.return_value = None

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                with pytest.raises(MetricsException) as exc_info:
                    total_workers_vcpu(None, None, None)
                assert 'Could not get a Kube CoreV1Api client' in str(exc_info.value)

    def test_successful_kubernetes_api_call_with_multiple_nodes(self):
        """Test successful K8s API call with multiple nodes and CPU calculation."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance and nodes
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            # Create mock nodes with different CPU capacities
            mock_node1 = MagicMock()
            mock_node1.metadata.name = 'worker-node-1'
            mock_node1.status.capacity = {'cpu': '16', 'memory': '32Gi', 'storage': '100Gi'}

            mock_node2 = MagicMock()
            mock_node2.metadata.name = 'worker-node-2'
            mock_node2.status.capacity = {'cpu': '8', 'memory': '16Gi'}

            mock_node3 = MagicMock()
            mock_node3.metadata.name = 'worker-node-3'
            mock_node3.status.capacity = {'cpu': '4', 'memory': '8Gi'}

            mock_nodes = MagicMock()
            mock_nodes.items = [mock_node1, mock_node2, mock_node3]
            mock_api.list_node.return_value = mock_nodes

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'my-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)

                expected_total = 16 + 8 + 4  # 28 vCPUs
                assert result == {'cluster_name': 'my-cluster', 'total_workers_vcpu': expected_total}

    def test_nodes_with_no_cpu_capacity(self):
        """Test handling of nodes that don't have CPU capacity information."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance and nodes
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            # Create mock nodes - one with CPU, one without
            mock_node1 = MagicMock()
            mock_node1.metadata.name = 'worker-node-1'
            mock_node1.status.capacity = {'cpu': '4', 'memory': '8Gi'}

            mock_node2 = MagicMock()
            mock_node2.metadata.name = 'worker-node-2'
            mock_node2.status.capacity = {'memory': '8Gi'}  # No CPU capacity

            mock_nodes = MagicMock()
            mock_nodes.items = [mock_node1, mock_node2]
            mock_api.list_node.return_value = mock_nodes

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 4}

    def test_empty_node_list(self):
        """Test handling of empty node list from Kubernetes API."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance with empty node list
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            mock_nodes = MagicMock()
            mock_nodes.items = []
            mock_api.list_node.return_value = mock_nodes

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 0}

    def test_cpu_values_as_strings_are_converted_to_int(self):
        """Test that CPU values from K8s API (strings) are properly converted to integers."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance and nodes
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            mock_node1 = MagicMock()
            mock_node1.metadata.name = 'worker-node-1'
            mock_node1.status.capacity = {'cpu': '12'}  # String value

            mock_nodes = MagicMock()
            mock_nodes.items = [mock_node1]
            mock_api.list_node.return_value = mock_nodes

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)

                assert result is not None, 'Function returned None instead of expected result'
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 12}
                assert isinstance(result['total_workers_vcpu'], int)

    @patch('metrics_utility.automation_controller_billing.collectors.datetime')
    def test_timestamp_in_output(self, mock_datetime):
        """Test that the function includes a proper timestamp in the output."""
        # Mock the datetime.now() call
        mock_now = datetime(2023, 12, 25, 15, 30, 45, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        mock_datetime.timezone = timezone  # Keep the timezone reference

        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
            patch('metrics_utility.automation_controller_billing.collectors.logger_info_level') as mock_logger_info,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance and nodes
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            mock_node1 = MagicMock()
            mock_node1.metadata.name = 'worker-node-1'
            mock_node1.status.capacity = {'cpu': '4'}

            mock_nodes = MagicMock()
            mock_nodes.items = [mock_node1]
            mock_api.list_node.return_value = mock_nodes

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)

                # Check that logger_info_level.info was called with JSON containing timestamp
                mock_logger_info.info.assert_called_once()

                logged_json = json.loads(mock_logger_info.info.call_args[0][0])
                assert 'timestamp' in logged_json
                assert logged_json['timestamp'] == '2023-12-25T15:30:45+00:00'
                assert logged_json['cluster_name'] == 'test-cluster'
                assert logged_json['total_workers_vcpu'] == 4
                assert logged_json['usage_based_billing_enabled']
                assert 'nodes' in logged_json

                # Also verify the return value
                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 4}

    def test_kube_config_fallback_from_incluster_to_file(self):
        """Test that the function falls back from in-cluster config to file config."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            # First config method fails, second succeeds
            mock_kube_config.ConfigException = Exception  # Mock the exception class
            mock_kube_config.load_incluster_config.side_effect = mock_kube_config.ConfigException('not in cluster')
            mock_kube_config.load_kube_config.return_value = None

            # Mock the API instance and nodes
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api

            mock_node1 = MagicMock()
            mock_node1.metadata.name = 'worker-node-1'
            mock_node1.status.capacity = {'cpu': '2'}

            mock_nodes = MagicMock()
            mock_nodes.items = [mock_node1]
            mock_api.list_node.return_value = mock_nodes

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                result = total_workers_vcpu(None, None, None)

                # Verify both config methods were called
                mock_kube_config.load_incluster_config.assert_called_once()
                mock_kube_config.load_kube_config.assert_called_once()

                assert result == {'cluster_name': 'test-cluster', 'total_workers_vcpu': 2}

    def test_unexpected_exception_handling(self):
        """Test that the function properly handles unexpected exceptions when listing nodes."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance to raise unexpected exception
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api
            mock_api.list_node.side_effect = RuntimeError('Unexpected error')

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                with pytest.raises(MetricsException) as exc_info:
                    total_workers_vcpu(None, None, None)
                assert 'Unexpected error when retrieving nodes:' in str(exc_info.value)

    def test_none_nodes_handling(self):
        """Test that the function properly handles when nodes is None."""
        with (
            patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get,
            patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config,
            patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client,
        ):
            mock_get.return_value = ['total_workers_vcpu']
            mock_kube_config.load_incluster_config.return_value = None

            # Mock the API instance to return None
            mock_api = MagicMock()
            mock_client.CoreV1Api.return_value = mock_api
            mock_api.list_node.return_value = None

            with temporary_env({'METRICS_UTILITY_CLUSTER_NAME': 'test-cluster', 'METRICS_UTILITY_USAGE_BASED_BILLING_ENABLED': 'true'}):
                with pytest.raises(MetricsException) as exc_info:
                    total_workers_vcpu(None, None, None)
                assert 'No nodes found' in str(exc_info.value)
