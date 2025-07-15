#!/usr/bin/env python3
"""
Standalone test for total_workers_vcpu function
This test doesn't require Django setup and can be run directly.
"""

import os
import sys
import tempfile
from unittest.mock import patch, MagicMock

# Add the metrics_utility directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'metrics_utility'))

def test_config_exception_fix():
    """Test that ConfigException is properly handled in the mock setup."""
    
    # Mock the collectors module functions
    with patch('metrics_utility.automation_controller_billing.collectors.get_optional_collectors') as mock_get, \
         patch('metrics_utility.automation_controller_billing.collectors.kube_config') as mock_kube_config, \
         patch('metrics_utility.automation_controller_billing.collectors.client') as mock_client:
        
        # Import the function after setting up the mocks
        from metrics_utility.automation_controller_billing.collectors import total_workers_vcpu
        
        # Set up the mocks
        mock_get.return_value = ['total_workers_vcpu']
        mock_kube_config.ConfigException = Exception  # Mock the exception class
        mock_kube_config.load_incluster_config.side_effect = mock_kube_config.ConfigException("not in cluster")
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
        
        # Set environment variables
        os.environ['METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME'] = 'test-cluster'
        os.environ.pop('METRICS_UTILITY_VCPU_COUNT_ENABLED', None)  # Ensure it's not set
        
        try:
            with patch('builtins.print'):  # Mock print to avoid output during tests
                result = total_workers_vcpu(None, None, None)
            
            # Verify the result
            assert result is not None, "Function returned None instead of expected result"
            assert result == {'cluster_name': 'TOBEADDED', 'total_workers_vcpu': 6}
            print("✅ ConfigException test passed!")
            
        finally:
            # Clean up environment variables
            os.environ.pop('METRICS_UTILITY_ANSIBLE_SAAS_CLUSTER_NAME', None)


if __name__ == "__main__":
    test_config_exception_fix()
    print("All tests passed!") 
