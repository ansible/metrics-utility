#!/usr/bin/env python3
"""
Sample code demonstrating the new StorageSegment interface
as requested in the PR feedback.

This shows how to use the library interface without needing
to search for implementation details.
"""

import metrics_utility.library.storage

# Example 1: Basic usage with a tarball
segment_config = {
    'write_key': 'your_segment_write_key_here',
    'user_id': 'your_install_uuid_here',
    'debug': True,  # Optional: enable debug mode
    'endpoint': 'https://api.segment.io/v1/track'  # Optional: custom endpoint
}

#storage = metrics_utility.library.storage.StorageSegment(**segment_config)

# Upload a tarball artifact
#storage.put('daily-metrics-2024-01-15', '/path/to/metrics-2024-01-15.tar.gz')

# Example 2: Upload a parquet file
#storage.put('job-analytics-batch-1', '/path/to/job_analytics.parquet')

# Example 3: Upload with environment variables
import os

# Configure using environment variables
env_config = {
    'write_key': os.getenv('METRICS_UTILITY_SEGMENT_WRITE_KEY'),
    'user_id': os.getenv('INSTALL_UUID', 'unknown'),
    'debug': os.getenv('METRICS_UTILITY_SEGMENT_DEBUG', 'false').lower() == 'true'
}

print(env_config)

storage_env = metrics_utility.library.storage.StorageSegment(**env_config)
storage_env.put('controller-config-snapshot', filename='example_data.json')

print("Sample usage complete - see example_segment_usage.py for implementation")
