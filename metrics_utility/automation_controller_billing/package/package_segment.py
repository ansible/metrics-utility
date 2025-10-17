import os
import json
import segment.analytics as analytics
from datetime import datetime

from django.conf import settings

import metrics_utility.base as base

from metrics_utility.logger import logger
from metrics_utility.anonymized_rollups.jobhostsummary_anonymized_rollup import JobHostSummaryAnonymizedRollup
from metrics_utility.anonymized_rollups.jobs_anonymized_rollup import JobsAnonymizedRollups
from metrics_utility.anonymized_rollups.events_modules_anonymized_rollup import EventModulesAnonymizedRollups


class PackageSegment(base.Package):
    """
    Package class for shipping anonymized metrics to Segment.com analytics platform
    Processes data through anonymized rollups before transmission
    """

    def _batch_since_and_until(self):
        return self.collections[0].since, self.collections[0].until

    def _tarname_base(self):
        since, until = self._batch_since_and_until()
        return f'{settings.INSTALL_UUID}-{since.strftime("%Y-%m-%d-%H%M%S%z")}-{until.strftime("%Y-%m-%d-%H%M%S%z")}-anonymized'

    def get_segment_write_key(self):
        return os.getenv('METRICS_UTILITY_SEGMENT_WRITE_KEY')

    def get_segment_endpoint(self):
        return os.getenv('METRICS_UTILITY_SEGMENT_ENDPOINT', 'https://api.segment.io/v1/track')

    def is_shipping_configured(self):
        """Check if Segment shipping is properly configured"""
        if not self.get_segment_write_key():
            logger.error('METRICS_UTILITY_SEGMENT_WRITE_KEY is not set')
            return False

        return True

    def ship(self):
        """
        Ship anonymized metrics to Segment.com
        Processes data through anonymized rollups before transmission
        """
        if not self.is_shipping_configured():
            self.shipping_successful = False
            return False

        logger.debug(f'shipping anonymized analytics data to Segment: {self.tar_path}')

        try:
            # Import Segment analytics library
            
            # Configure Segment client
            analytics.write_key = self.get_segment_write_key()
            analytics.debug = os.getenv('METRICS_UTILITY_SEGMENT_DEBUG', 'false').lower() == 'true'
            
            # Process collections through anonymized rollups and send as track events
            for collection in self.collections:
                if collection.is_empty():
                    continue
                    
                # Apply anonymization and convert to Segment events
                anonymized_events = self._anonymize_and_create_segment_events(collection)
                
                for event in anonymized_events:
                    analytics.track(**event)
                    logger.debug(f'Sent anonymized event: {event["event"]}')
            
            # Flush to ensure events are sent
            analytics.flush()
            
            self.shipping_successful = True
            logger.info(f'Successfully shipped {len(self.collections)} anonymized collections to Segment')
            return True
            
        except ImportError:
            logger.error('analytics-python package not installed. Run: pip install analytics-python')
            self.shipping_successful = False
            return False
        except Exception as e:
            logger.error(f'Failed to ship anonymized metrics to Segment: {e}')
            self.shipping_successful = False
            return False

    def _anonymize_and_create_segment_events(self, collection):
        """
        Apply anonymized rollups to collection data and convert to Segment track events
        """
        events = []
        since, until = self._batch_since_and_until()
        
        # Base event properties for anonymized events
        base_properties = {
            'collection_key': collection.key,
            'collection_version': getattr(collection.func_collecting, '__insights_analytics_version__', '1.0'),
            'since': since.isoformat(),
            'until': until.isoformat(),
            'install_uuid': str(settings.INSTALL_UUID),
            'data_mode': 'anonymized',
        }

        try:
            if collection.key == 'job_host_summary' and collection.data_type == 'csv':
                # Process job host summary through anonymized rollup
                anonymized_data = self._process_jobhost_summary_anonymization(collection)
                
                for rollup_result in anonymized_data:
                    event = {
                        'user_id': str(settings.INSTALL_UUID),
                        'event': 'AAP Metrics - Job Template Performance',
                        'properties': {
                            **base_properties,
                            **rollup_result,
                            'metric_type': 'job_template_aggregation',
                        }
                    }
                    events.append(event)

            elif collection.key == 'unified_jobs' and collection.data_type == 'csv':
                # Process jobs through anonymized rollup
                anonymized_data = self._process_jobs_anonymization(collection)
                
                for rollup_result in anonymized_data:
                    event = {
                        'user_id': str(settings.INSTALL_UUID),
                        'event': 'AAP Metrics - Job Duration Analytics',
                        'properties': {
                            **base_properties,
                            **rollup_result,
                            'metric_type': 'job_performance_aggregation',
                        }
                    }
                    events.append(event)

            elif collection.key == 'main_jobevent_service' and collection.data_type == 'csv':
                # Process events through anonymized rollup
                anonymized_data = self._process_events_anonymization(collection)
                
                for rollup_result in anonymized_data:
                    event = {
                        'user_id': str(settings.INSTALL_UUID),
                        'event': 'AAP Metrics - Module Usage Analytics',
                        'properties': {
                            **base_properties,
                            **rollup_result,
                            'metric_type': 'module_usage_aggregation',
                        }
                    }
                    events.append(event)

            elif collection.data_type == 'json':
                # For JSON collections (like config), send anonymized summary
                anonymized_summary = self._anonymize_json_collection(collection)
                
                event = {
                    'user_id': str(settings.INSTALL_UUID),
                    'event': f'AAP Metrics - {collection.key.title()} Summary',
                    'properties': {
                        **base_properties,
                        **anonymized_summary,
                        'metric_type': 'configuration_summary',
                    }
                }
                events.append(event)

        except Exception as e:
            logger.warning(f'Failed to anonymize collection {collection.key}: {e}')

        return events

    def _process_jobhost_summary_anonymization(self, collection):
        """Apply JobHostSummaryAnonymizedRollup to the collection data"""
        try:
            import pandas as pd
            
            # Read CSV data from collection
            with open(collection.path, 'r') as f:
                df = pd.read_csv(f)
            
            # Apply anonymized rollup
            anonymized_results = JobHostSummaryAnonymizedRollup.base(df)
            return anonymized_results
            
        except Exception as e:
            logger.error(f'Failed to process job host summary anonymization: {e}')
            return []

    def _process_jobs_anonymization(self, collection):
        """Apply JobsAnonymizedRollups to the collection data"""
        try:
            import pandas as pd
            
            # Read CSV data from collection
            with open(collection.path, 'r') as f:
                df = pd.read_csv(f)
            
            # Apply anonymized rollup
            anonymized_results = JobsAnonymizedRollups.base(df)
            return anonymized_results
            
        except Exception as e:
            logger.error(f'Failed to process jobs anonymization: {e}')
            return []

    def _process_events_anonymization(self, collection):
        """Apply EventModulesAnonymizedRollups to the collection data"""
        try:
            import pandas as pd
            
            # Read CSV data from collection
            with open(collection.path, 'r') as f:
                df = pd.read_csv(f)
            
            # Apply anonymized rollup
            anonymized_results = EventModulesAnonymizedRollups.base(df)
            return anonymized_results
            
        except Exception as e:
            logger.error(f'Failed to process events anonymization: {e}')
            return []

    def _anonymize_json_collection(self, collection):
        """Create anonymized summary for JSON collections"""
        try:
            data = json.loads(collection.data) if isinstance(collection.data, str) else collection.data
            
            # Create anonymized summary of config data
            anonymized = {
                'platform_system': data.get('platform', {}).get('system'),
                'install_type': data.get('platform', {}).get('type'),
                'controller_version': data.get('controller_version'),
                'license_type': data.get('license_type'),
                'has_valid_license': bool(data.get('valid_key')),
                'total_licensed_instances': data.get('total_licensed_instances', 0),
                'current_instances': data.get('current_instances', 0),
                'compliant': data.get('compliant'),
                'metrics_utility_version': data.get('metrics_utility_version'),
            }
            
            # Remove any None values
            return {k: v for k, v in anonymized.items() if v is not None}
            
        except Exception as e:
            logger.warning(f'Failed to anonymize JSON collection: {e}')
            return {'anonymization_error': True}

    # Abstract method implementations (not used for Segment)
    def _get_http_request_headers(self):
        return {}

    def _get_rh_user(self):
        return None

    def _get_rh_password(self):
        return None

    def _get_rh_region(self):
        return None

    def _get_rh_bucket(self):
        return None

    def get_ingress_url(self):
        return self.get_segment_endpoint()

    def get_s3_configured(self):
        return False
