import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
from django.conf import settings
from django.test import TestCase, override_settings

from metrics_utility.automation_controller_billing.package.package_segment import PackageSegment


class TestPackageSegment(TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.collector = Mock()
        self.collector.tmp_dir = Mock()
        self.collector.tmp_dir.parent = tempfile.mkdtemp()
        
        # Mock collections
        self.mock_collection = Mock()
        self.mock_collection.key = 'job_host_summary'
        self.mock_collection.data_type = 'csv'
        self.mock_collection.path = '/tmp/test_data.csv'
        self.mock_collection.since = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.mock_collection.until = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        self.mock_collection.is_empty.return_value = False
        self.mock_collection.func_collecting = Mock()
        self.mock_collection.func_collecting.__insights_analytics_version__ = '1.0'
        
        self.package = PackageSegment(self.collector)
        self.package.collections = [self.mock_collection]

    @override_settings(INSTALL_UUID='test-uuid-123')
    def test_tarname_base(self):
        """Test tarname generation includes anonymized suffix"""
        tarname = self.package._tarname_base()
        
        self.assertIn('test-uuid-123', tarname)
        self.assertIn('2024-01-01', tarname)
        self.assertIn('2024-01-02', tarname)
        self.assertTrue(tarname.endswith('-anonymized'))

    @patch.dict(os.environ, {'METRICS_UTILITY_SEGMENT_WRITE_KEY': 'test_key'})
    def test_is_shipping_configured_with_key(self):
        """Test shipping configuration with valid write key"""
        self.assertTrue(self.package.is_shipping_configured())

    @patch.dict(os.environ, {}, clear=True)
    def test_is_shipping_configured_without_key(self):
        """Test shipping configuration fails without write key"""
        self.assertFalse(self.package.is_shipping_configured())

    @patch.dict(os.environ, {'METRICS_UTILITY_SEGMENT_ENDPOINT': 'https://custom.segment.io/track'})
    def test_get_segment_endpoint_custom(self):
        """Test custom Segment endpoint configuration"""
        endpoint = self.package.get_segment_endpoint()
        self.assertEqual(endpoint, 'https://custom.segment.io/track')

    def test_get_segment_endpoint_default(self):
        """Test default Segment endpoint"""
        endpoint = self.package.get_segment_endpoint()
        self.assertEqual(endpoint, 'https://api.segment.io/v1/track')

    @patch.dict(os.environ, {'METRICS_UTILITY_SEGMENT_WRITE_KEY': 'test_key'})
    @override_settings(INSTALL_UUID='test-uuid-123')
    def test_ship_success(self):
        """Test successful shipping to Segment"""
        # Mock the analytics module import
        mock_analytics = Mock()
        
        with patch.object(self.package, '_anonymize_and_create_segment_events') as mock_anonymize:
            with patch('builtins.__import__', return_value=mock_analytics) as mock_import:
                mock_anonymize.return_value = [
                    {
                        'user_id': 'test-uuid-123',
                        'event': 'AAP Metrics - Job Template Performance',
                        'properties': {
                            'job_template_name': 'test_template',
                            'jobs_total': 5,
                            'metric_type': 'job_template_aggregation'
                        }
                    }
                ]
                
                result = self.package.ship()
                
                self.assertTrue(result)
                self.assertTrue(self.package.shipping_successful)
                mock_analytics.track.assert_called_once()
                mock_analytics.flush.assert_called_once()

    @patch.dict(os.environ, {}, clear=True)
    def test_ship_fails_without_configuration(self):
        """Test shipping fails without proper configuration"""
        result = self.package.ship()
        
        self.assertFalse(result)
        self.assertFalse(self.package.shipping_successful)

    def test_ship_fails_without_analytics_package(self):
        """Test shipping fails when analytics package is not installed"""
        with patch.dict(os.environ, {'METRICS_UTILITY_SEGMENT_WRITE_KEY': 'test_key'}):
            with patch('builtins.__import__', side_effect=ImportError("No module named 'analytics'")):
                result = self.package.ship()
                
                self.assertFalse(result)
                self.assertFalse(self.package.shipping_successful)

    @override_settings(INSTALL_UUID='test-uuid-123')
    def test_process_jobhost_summary_anonymization(self):
        """Test job host summary anonymization processing"""
        expected_result = [
            {
                'job_template_name': 'template1',
                'jobs_total': 1,
                'hosts_total': 2,
                'ok_total': 5,
                'failures_total': 1
            },
            {
                'job_template_name': 'template2',
                'jobs_total': 1,
                'hosts_total': 1,
                'ok_total': 5,
                'failures_total': 0
            }
        ]
        
        # Mock the pandas module and rollup directly
        mock_pd = Mock()
        mock_df = Mock()
        mock_pd.read_csv.return_value = mock_df
        
        with patch('builtins.open', mock_open(read_data="test")):
            with patch('builtins.__import__') as mock_import:
                def import_side_effect(name, *args):
                    if name == 'pandas':
                        return mock_pd
                    return MagicMock()  # Default for other imports
                
                mock_import.side_effect = import_side_effect
                
                with patch('metrics_utility.automation_controller_billing.package.package_segment.JobHostSummaryAnonymizedRollup.base') as mock_rollup:
                    mock_rollup.return_value = expected_result
                    
                    result = self.package._process_jobhost_summary_anonymization(self.mock_collection)
                    
                    self.assertEqual(len(result), 2)
                    self.assertEqual(result[0]['job_template_name'], 'template1')
                    self.assertEqual(result[0]['hosts_total'], 2)
                    self.assertEqual(result[1]['job_template_name'], 'template2')
                    mock_rollup.assert_called_once_with(mock_df)

    @override_settings(INSTALL_UUID='test-uuid-123')
    def test_anonymize_json_collection(self):
        """Test JSON collection anonymization"""
        test_json_data = {
            'platform': {'system': 'Linux', 'type': 'traditional'},
            'controller_version': '4.5.0',
            'license_type': 'enterprise',
            'valid_key': True,
            'total_licensed_instances': 100,
            'current_instances': 75,
            'compliant': True,
            'metrics_utility_version': '1.0.0',
            'sensitive_data': 'should_not_appear'
        }
        
        result = self.package._anonymize_json_collection(Mock(data=json.dumps(test_json_data)))
        
        # Check that only expected anonymized fields are present
        expected_fields = {
            'platform_system', 'install_type', 'controller_version',
            'license_type', 'has_valid_license', 'total_licensed_instances',
            'current_instances', 'compliant', 'metrics_utility_version'
        }
        
        self.assertEqual(set(result.keys()), expected_fields)
        self.assertEqual(result['platform_system'], 'Linux')
        self.assertEqual(result['install_type'], 'traditional')
        self.assertTrue(result['has_valid_license'])
        self.assertNotIn('sensitive_data', result)

    @override_settings(INSTALL_UUID='test-uuid-123')
    def test_anonymize_and_create_segment_events_jobhost_summary(self):
        """Test creating Segment events for job host summary"""
        collection = Mock()
        collection.key = 'job_host_summary'
        collection.data_type = 'csv'
        collection.func_collecting = Mock()
        collection.func_collecting.__insights_analytics_version__ = '1.2'
        
        test_anonymized_data = [
            {
                'job_template_name': 'test_template',
                'jobs_total': 3,
                'hosts_total': 5,
                'ok_total': 45,
                'failures_total': 2
            }
        ]
        
        with patch.object(self.package, '_process_jobhost_summary_anonymization') as mock_process:
            mock_process.return_value = test_anonymized_data
            
            events = self.package._anonymize_and_create_segment_events(collection)
            
            self.assertEqual(len(events), 1)
            event = events[0]
            
            self.assertEqual(event['user_id'], 'test-uuid-123')
            self.assertEqual(event['event'], 'AAP Metrics - Job Template Performance')
            self.assertEqual(event['properties']['collection_key'], 'job_host_summary')
            self.assertEqual(event['properties']['collection_version'], '1.2')
            self.assertEqual(event['properties']['data_mode'], 'anonymized')
            self.assertEqual(event['properties']['metric_type'], 'job_template_aggregation')
            self.assertEqual(event['properties']['job_template_name'], 'test_template')
            self.assertEqual(event['properties']['jobs_total'], 3)

    @override_settings(INSTALL_UUID='test-uuid-123')
    def test_anonymize_and_create_segment_events_json_config(self):
        """Test creating Segment events for JSON config collection"""
        collection = Mock()
        collection.key = 'config'
        collection.data_type = 'json'
        collection.func_collecting = Mock()
        collection.func_collecting.__insights_analytics_version__ = '1.0'
        
        test_anonymized_data = {
            'platform_system': 'Linux',
            'controller_version': '4.5.0',
            'license_type': 'enterprise'
        }
        
        with patch.object(self.package, '_anonymize_json_collection') as mock_anonymize:
            mock_anonymize.return_value = test_anonymized_data
            
            events = self.package._anonymize_and_create_segment_events(collection)
            
            self.assertEqual(len(events), 1)
            event = events[0]
            
            self.assertEqual(event['user_id'], 'test-uuid-123')
            self.assertEqual(event['event'], 'AAP Metrics - Config Summary')
            self.assertEqual(event['properties']['collection_key'], 'config')
            self.assertEqual(event['properties']['data_mode'], 'anonymized')
            self.assertEqual(event['properties']['metric_type'], 'configuration_summary')
            self.assertEqual(event['properties']['platform_system'], 'Linux')

    def test_anonymize_and_create_segment_events_handles_exceptions(self):
        """Test that anonymization handles exceptions gracefully"""
        collection = Mock()
        collection.key = 'job_host_summary'
        collection.data_type = 'csv'
        collection.func_collecting = Mock()
        collection.func_collecting.__insights_analytics_version__ = '1.0'
        
        with patch.object(self.package, '_process_jobhost_summary_anonymization') as mock_process:
            mock_process.side_effect = Exception("Test error")
            
            events = self.package._anonymize_and_create_segment_events(collection)
            
            # Should return empty list when anonymization fails
            self.assertEqual(len(events), 0)

    def test_empty_collections_skipped(self):
        """Test that empty collections are skipped during shipping"""
        self.mock_collection.is_empty.return_value = True
        mock_analytics = Mock()
        
        with patch.dict(os.environ, {'METRICS_UTILITY_SEGMENT_WRITE_KEY': 'test_key'}):
            with patch('builtins.__import__', return_value=mock_analytics):
                with patch.object(self.package, '_anonymize_and_create_segment_events') as mock_anonymize:
                    self.package.ship()
                    
                    # Should not be called for empty collections
                    mock_anonymize.assert_not_called()

    def test_abstract_methods_return_appropriate_values(self):
        """Test that abstract method implementations return expected values"""
        self.assertEqual(self.package._get_http_request_headers(), {})
        self.assertIsNone(self.package._get_rh_user())
        self.assertIsNone(self.package._get_rh_password())
        self.assertIsNone(self.package._get_rh_region())
        self.assertIsNone(self.package._get_rh_bucket())
        self.assertFalse(self.package.get_s3_configured())
        self.assertEqual(self.package.get_ingress_url(), self.package.get_segment_endpoint())


if __name__ == '__main__':
    unittest.main()