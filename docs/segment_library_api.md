# Segment Library API

This document describes how to use the Segment integration programmatically from Python code using the library API.

## Overview

The `metrics_utility.library.segment` module provides a clean API for sending data to Segment.com without using the CLI command. This is useful for:

- Embedding Segment tracking in your own Python applications
- Building custom automation workflows
- Integrating with other Python libraries and frameworks
- Unit testing with Segment integration

## Installation

The Segment library API is included with metrics-utility. Just ensure the package is installed:

```bash
pip install metrics-utility
```

## Quick Start

### Simple Function API

The easiest way to send data:

```python
from metrics_utility.library.segment import send_data

# Send list of dictionaries
result = send_data(
    data=[
        {"hostname": "server1", "status": "active"},
        {"hostname": "server2", "status": "inactive"},
    ],
    app="ansible-automation-platform",
    write_key="your_segment_write_key",
)

print(result)
# {'success': True, 'event_name': 'ansible-automation-platform_data_upload', ...}
```

### Class-Based API

For more control and reusability:

```python
from metrics_utility.library.segment import SegmentSender

# Initialize sender once
sender = SegmentSender(write_key="your_segment_write_key")

# Send multiple times
sender.send(
    data=[{"metric": "cpu", "value": 75}],
    app="awx",
    user_id="admin@example.com",
)

sender.send(
    data=[{"metric": "memory", "value": 82}],
    app="awx",
    user_id="admin@example.com",
)
```

## API Reference

### Functions

#### `send_data()`

Send data to Segment in a single function call.

**Parameters:**

- `data` (list|dict|str|Path): Data to send. Can be:
  - List of dictionaries
  - Single dictionary
  - Path to CSV file
  - Path to JSON file
- `app` (str): Application identifier (used in event name and context)
- `write_key` (str): Segment write key for authentication
- `user_id` (str, optional): User ID for the event. Default: `'system'`
- `additional_properties` (dict, optional): Additional properties to include
- `debug` (bool, optional): Enable debug logging. Default: `False`

**Returns:**

Dictionary with:

- `success` (bool): Whether send was successful
- `event_name` (str): Name of the event sent
- `row_count` (int): Number of data items sent
- `message` (str): Success or error message

**Raises:**

- `SegmentConfigurationError`: If configuration is invalid
- `SegmentDataError`: If data cannot be processed or sent

**Example:**

```python
result = send_data(
    data=[{"key": "value"}],
    app="my-app",
    write_key="sk_test_...",
    user_id="user123",
    additional_properties={"source": "api", "version": "1.0"},
)
```

#### `send_csv_file()`

Convenience function for sending CSV files.

**Parameters:**

- `file_path` (str|Path): Path to CSV file
- `app` (str): Application identifier
- `write_key` (str): Segment write key
- `user_id` (str, optional): User ID. Default: `'system'`
- `debug` (bool, optional): Enable debug logging. Default: `False`

**Example:**

```python
from metrics_utility.library.segment import send_csv_file

result = send_csv_file(
    file_path="metrics.csv",
    app="ansible-automation-platform",
    write_key="your_key",
)
```

#### `send_json_file()`

Convenience function for sending JSON files.

**Parameters:**

- `file_path` (str|Path): Path to JSON file
- `app` (str): Application identifier
- `write_key` (str): Segment write key
- `user_id` (str, optional): User ID. Default: `'system'`
- `debug` (bool, optional): Enable debug logging. Default: `False`

**Example:**

```python
from metrics_utility.library.segment import send_json_file

result = send_json_file(
    file_path="report.json",
    app="awx",
    write_key="your_key",
)
```

### Classes

#### `SegmentSender`

Class-based API for sending data to Segment.

**Constructor:**

```python
sender = SegmentSender(write_key: str, debug: bool = False)
```

**Parameters:**

- `write_key` (str): Segment write key for authentication
- `debug` (bool, optional): Enable debug logging. Default: `False`

**Methods:**

##### `send()`

Send data to Segment as a custom event.

```python
sender.send(
    data: Union[List[Dict], Dict, str, Path],
    app: str,
    user_id: str = 'system',
    additional_properties: Optional[Dict] = None,
) -> Dict
```

**Parameters:**

- `data`: Data to send (list of dicts, dict, or file path)
- `app`: Application identifier
- `user_id`: User ID for the event. Default: `'system'`
- `additional_properties`: Additional properties to include

**Returns:**

Dictionary with send results.

**Example:**

```python
sender = SegmentSender(write_key="your_key")

result = sender.send(
    data=[{"metric": "cpu", "value": 75}],
    app="awx",
    user_id="admin",
)
```

### Exceptions

#### `SegmentError`

Base exception for all Segment-related errors.

#### `SegmentConfigurationError`

Raised when Segment configuration is invalid (e.g., missing write key).

#### `SegmentDataError`

Raised when data cannot be processed or sent to Segment.

## Usage Examples

### Example 1: Sending Metrics from a Python Application

```python
from metrics_utility.library.segment import send_data

def track_metrics(metrics, app_name, segment_key):
    """Send application metrics to Segment."""
    try:
        result = send_data(
            data=metrics,
            app=app_name,
            write_key=segment_key,
            user_id="system",
        )

        if result['success']:
            print(f"Sent {result['row_count']} metrics to Segment")
        return result

    except Exception as e:
        print(f"Error sending to Segment: {e}")
        return None

# Usage
metrics = [
    {"server": "web-1", "cpu": 75, "memory": 82},
    {"server": "web-2", "cpu": 68, "memory": 79},
]

track_metrics(
    metrics=metrics,
    app_name="ansible-automation-platform",
    segment_key="your_key",
)
```

### Example 2: Integration with Pandas

```python
import pandas as pd
from metrics_utility.library.segment import send_data

# Load data from pandas DataFrame
df = pd.read_csv("metrics.csv")

# Convert to list of dictionaries
data = df.to_dict('records')

# Send to Segment
result = send_data(
    data=data,
    app="awx",
    write_key="your_key",
    additional_properties={
        "source": "pandas",
        "total_rows": len(df),
    },
)
```

### Example 3: Reusable Sender Class

```python
from metrics_utility.library.segment import SegmentSender, SegmentError

class MetricsTracker:
    def __init__(self, segment_key, app_name):
        self.sender = SegmentSender(write_key=segment_key)
        self.app_name = app_name

    def track_event(self, data, user_id="system"):
        """Track an event with error handling."""
        try:
            result = self.sender.send(
                data=data,
                app=self.app_name,
                user_id=user_id,
            )
            return result
        except SegmentError as e:
            print(f"Segment error: {e}")
            return None

    def track_file(self, file_path, user_id="system"):
        """Track data from a file."""
        return self.track_event(data=file_path, user_id=user_id)

# Usage
tracker = MetricsTracker(
    segment_key="your_key",
    app_name="ansible-automation-platform",
)

tracker.track_event([{"action": "login", "user": "admin"}])
tracker.track_file("daily_metrics.csv")
```

### Example 4: Adding Custom Properties

```python
from metrics_utility.library.segment import send_data
import socket

# Get additional context
hostname = socket.gethostname()

result = send_data(
    data=[{"metric": "deployment", "status": "success"}],
    app="ansible-controller",
    write_key="your_key",
    user_id="deployment-bot",
    additional_properties={
        "hostname": hostname,
        "environment": "production",
        "version": "2.5.0",
        "deployment_id": "deploy-123",
    },
)
```

### Example 5: Batch Processing

```python
from metrics_utility.library.segment import SegmentSender
from pathlib import Path

def process_metrics_directory(directory, app_name, segment_key):
    """Process all CSV files in a directory and send to Segment."""
    sender = SegmentSender(write_key=segment_key)
    results = []

    for csv_file in Path(directory).glob("*.csv"):
        try:
            result = sender.send(
                data=csv_file,
                app=app_name,
                additional_properties={"filename": csv_file.name},
            )
            results.append(result)
            print(f"Processed {csv_file.name}: {result['row_count']} rows")
        except Exception as e:
            print(f"Error processing {csv_file.name}: {e}")

    return results

# Process all metrics files
results = process_metrics_directory(
    directory="/data/metrics",
    app_name="ansible-automation-platform",
    segment_key="your_key",
)

print(f"Processed {len(results)} files")
```

### Example 6: Error Handling

```python
from metrics_utility.library.segment import (
    send_data,
    SegmentConfigurationError,
    SegmentDataError,
)

def safe_send_to_segment(data, app, write_key):
    """Send data with comprehensive error handling."""
    try:
        result = send_data(
            data=data,
            app=app,
            write_key=write_key,
        )
        return result

    except SegmentConfigurationError as e:
        print(f"Configuration error: {e}")
        # Log to monitoring system
        return None

    except SegmentDataError as e:
        print(f"Data error: {e}")
        # Maybe retry with different data format
        return None

    except Exception as e:
        print(f"Unexpected error: {e}")
        # Alert ops team
        return None
```

## Environment Variables

While the library API doesn't use environment variables directly, you can integrate with them:

```python
import os
from metrics_utility.library.segment import send_data

segment_key = os.getenv("SEGMENT_WRITE_KEY")
if not segment_key:
    raise ValueError("SEGMENT_WRITE_KEY environment variable not set")

result = send_data(
    data=[{"metric": "value"}],
    app="my-app",
    write_key=segment_key,
)
```

## Comparison with CLI

| Feature        | CLI Command      | Library API      |
| -------------- | ---------------- | ---------------- |
| Usage          | Command line     | Python code      |
| File types     | CSV, JSON, text  | Same + list/dict |
| Configuration  | Environment vars | Function params  |
| Error handling | Exit codes       | Exceptions       |
| Reusability    | One-off          | Reusable in code |
| Integration    | Shell scripts    | Python apps      |

## Best Practices

1. **Store write keys securely**: Use environment variables or secret management systems
2. **Handle errors gracefully**: Use try-except blocks with specific exception types
3. **Reuse sender instances**: Create once, use multiple times for better performance
4. **Add context**: Use `additional_properties` to add useful metadata
5. **Test first**: Use Segment's test environment before production
6. **Monitor results**: Check the returned result dictionary for success
7. **Batch when possible**: Send multiple items in one call rather than individual calls

## See Also

- [CLI Command Documentation](send_to_segment.md)
- [Quick Start Guide](send_to_segment_quickstart.md)
- [Segment API Documentation](https://segment.com/docs/connections/sources/catalog/libraries/server/python/)
