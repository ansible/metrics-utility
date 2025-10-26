# Send to Segment Command

## Overview

The `send_to_segment` command allows you to send data from CSV, JSON, or text files to Segment.com as custom tracking events. This is useful for uploading metrics, analytics data, or other structured information to your Segment workspace.

## Features

- 📊 **Multiple File Formats**: Supports CSV, JSON, and plain text files
- 🔒 **Secure Authentication**: Uses Segment Write Key for API authentication
- 📱 **Application Context**: Tags events with application identifiers
- 🎯 **Single Event Upload**: Sends entire file as one custom event (not row-by-row)
- ✅ **Error Handling**: Comprehensive validation and error messages
- 🐛 **Debug Mode**: Verbose logging for troubleshooting

## Installation

### 1. Install Dependencies

The Segment SDK is included in the project dependencies. Install it with:

```bash
# If using pip
pip install -e .

# If using uv (recommended)
uv pip install -e .
```

### 2. Get Your Segment Write Key

1. Log in to your Segment account at [app.segment.com](https://app.segment.com)
2. Navigate to **Connections** → **Sources**
3. Select your source or create a new one
4. Copy the **Write Key** from the Settings → API Keys section

### 3. Set Environment Variable

```bash
export SEGMENT_WRITE_KEY="your_write_key_here"
```

Or add it to your `.env` file:

```
SEGMENT_WRITE_KEY=your_write_key_here
```

## Usage

### Basic Syntax

```bash
python manage.py send_to_segment --file <file_path> --app <app_name> [options]
```

### Required Arguments

| Argument | Description                                             | Example                             |
| -------- | ------------------------------------------------------- | ----------------------------------- |
| `--file` | Path to the data file (CSV, JSON, or text)              | `--file data.csv`                   |
| `--app`  | Application identifier (used in event name and context) | `--app ansible-automation-platform` |

### Optional Arguments

| Argument    | Description                   | Default  | Example             |
| ----------- | ----------------------------- | -------- | ------------------- |
| `--user-id` | User ID for the Segment event | `system` | `--user-id user123` |
| `--verbose` | Enable debug logging          | `False`  | `--verbose`         |

### Environment Variables

| Variable            | Required | Description                               |
| ------------------- | -------- | ----------------------------------------- |
| `SEGMENT_WRITE_KEY` | ✅ Yes   | Your Segment write key for authentication |

## Examples

### Example 1: Send a CSV File

```bash
python manage.py send_to_segment \
  --file /path/to/metrics.csv \
  --app ansible-automation-platform
```

**CSV File Example** (`metrics.csv`):

```csv
hostname,managed,timestamp
server1,true,2025-10-26T10:00:00Z
server2,true,2025-10-26T10:01:00Z
server3,false,2025-10-26T10:02:00Z
```

### Example 2: Send with Custom User ID

```bash
python manage.py send_to_segment \
  --file billing_data.csv \
  --app awx \
  --user-id admin@example.com
```

### Example 3: Send JSON Data

```bash
python manage.py send_to_segment \
  --file report.json \
  --app ansible-controller
```

**JSON File Example** (`report.json`):

```json
[
  {"metric": "cpu_usage", "value": 75.2, "timestamp": "2025-10-26T10:00:00Z"},
  {"metric": "memory_usage", "value": 82.5, "timestamp": "2025-10-26T10:01:00Z"}
]
```

### Example 4: Debug Mode with Verbose Logging

```bash
python manage.py send_to_segment \
  --file data.csv \
  --app ansible-platform \
  --verbose
```

### Example 5: Using with Environment Variables

```bash
# Set the Segment write key
export SEGMENT_WRITE_KEY="sk_test_abc123xyz789"

# Run the command
python manage.py send_to_segment \
  --file monthly_metrics.csv \
  --app automation-platform
```

## File Format Support

### CSV Files (`.csv`)

- Automatically parsed into list of dictionaries
- Header row becomes dictionary keys
- Each subsequent row becomes a dictionary entry

**Input:**

```csv
id,name,status
1,server1,active
2,server2,inactive
```

**Parsed as:**

```json
[
  {"id": "1", "name": "server1", "status": "active"},
  {"id": "2", "name": "server2", "status": "inactive"}
]
```

### JSON Files (`.json`)

- Supports both arrays and single objects
- Arrays are sent as-is
- Single objects are wrapped in an array

**Input (Array):**

```json
[{"key": "value1"}, {"key": "value2"}]
```

**Input (Object):**

```json
{"key": "value"}
```

**Parsed as:**

```json
[{"key": "value"}]
```

### Text Files (`.txt`, other)

- Files without `.csv` or `.json` extensions
- Attempts CSV parsing first
- Falls back to plain text if CSV parsing fails
- Content stored with line-by-line breakdown

## Segment Event Structure

### Event Name

Events are named using the pattern: `{app}_data_upload`

**Examples:**

- App: `ansible-automation-platform` → Event: `ansible-automation-platform_data_upload`
- App: `awx` → Event: `awx_data_upload`

### Event Properties

The following properties are automatically included:

| Property    | Type         | Description                                |
| ----------- | ------------ | ------------------------------------------ |
| `filename`  | string       | Name of the uploaded file                  |
| `file_path` | string       | Full path to the file                      |
| `data`      | array/object | Parsed file content                        |
| `row_count` | integer      | Number of rows/items in the data           |
| `file_type` | string       | Detected file type (`csv`, `json`, `text`) |
| `timestamp` | string       | ISO 8601 timestamp of the upload           |

### Event Context

The `app.name` context is automatically set:

```json
{
  "app": {
    "name": "ansible-automation-platform"
  }
}
```

### Complete Event Example

```json
{
  "userId": "system",
  "event": "ansible-automation-platform_data_upload",
  "properties": {
    "filename": "metrics.csv",
    "file_path": "/data/metrics.csv",
    "data": [
      {"hostname": "server1", "managed": "true"},
      {"hostname": "server2", "managed": "false"}
    ],
    "row_count": 2,
    "file_type": "csv",
    "timestamp": "2025-10-26T21:43:04.494644+00:00"
  },
  "context": {
    "app": {
      "name": "ansible-automation-platform"
    }
  }
}
```

## Error Handling

### Common Errors and Solutions

#### Missing Segment Write Key

**Error:**

```
Missing required env variable SEGMENT_WRITE_KEY.
```

**Solution:**

```bash
export SEGMENT_WRITE_KEY="your_write_key_here"
```

#### File Not Found

**Error:**

```
File not found: /path/to/data.csv
```

**Solution:**

- Check the file path is correct
- Ensure the file exists
- Use absolute paths or verify current directory

#### Invalid CSV Format

**Error:**

```
Error reading file: [parsing error details]
```

**Solution:**

- Verify CSV has proper header row
- Check for malformed rows
- Ensure consistent number of columns
- Try saving with UTF-8 encoding

#### Segment API Error

**Error:**

```
Error sending data to Segment: [API error details]
```

**Solution:**

- Verify your Segment Write Key is valid
- Check network connectivity
- Ensure your Segment source is active
- Review Segment dashboard for rate limits

## Best Practices

### 1. Data Privacy

- Never include personally identifiable information (PII) without proper consent
- Sanitize sensitive data before uploading
- Review your organization's data policies

### 2. File Size Considerations

- Keep file sizes reasonable (< 10MB recommended)
- For large datasets, consider splitting into multiple files
- Segment has payload size limits (~32KB per event)

### 3. Event Naming

- Use consistent, descriptive app names
- Follow your team's naming conventions
- Examples: `ansible-controller`, `awx-billing`, `automation-platform`

### 4. User ID Strategy

- Use `--user-id` for user-specific data
- Keep default `system` for automated/system data
- Be consistent across your tracking

### 5. Testing

- Test with small files first
- Use `--verbose` flag during initial setup
- Verify events in Segment debugger before production use

## Verification

### Check Events in Segment

1. Log in to [app.segment.com](https://app.segment.com)
2. Go to **Connections** → **Sources** → Your Source
3. Click **Debugger** tab
4. Look for events with your custom event name (e.g., `ansible-automation-platform_data_upload`)
5. Inspect event properties to verify data was sent correctly

### Test Command Output

Successful upload shows:

```
Reading file: data.csv
Sending data to Segment: event=ansible-automation-platform_data_upload, user_id=system, rows=3
Successfully sent data to Segment: 3 rows from data.csv as event "ansible-automation-platform_data_upload"
```

## Troubleshooting

### Enable Debug Mode

Add `--verbose` flag to see detailed logging:

```bash
python manage.py send_to_segment \
  --file data.csv \
  --app my-app \
  --verbose
```

This shows:

- Segment SDK debug output
- API request/response details
- Event queueing and flushing
- Consumer thread activity

### Check File Permissions

Ensure the command can read your file:

```bash
ls -la /path/to/your/file.csv
```

### Verify Python Environment

Ensure the Segment SDK is installed:

```bash
pip list | grep segment
# Should show: segment-analytics-python
```

## Integration with Existing Commands

This command works alongside other metrics-utility commands:

```bash
# 1. Gather data
python manage.py gather_automation_controller_billing_data --since 2025-10-01

# 2. Build report
python manage.py build_report --month 2025-10

# 3. Send to Segment
python manage.py send_to_segment \
  --file out/reports/2025/10/CCSPv2-2025-10.csv \
  --app ansible-automation-platform
```

## API Reference

### Command Class

**Location:** `metrics_utility/management/commands/send_to_segment.py`

**Key Methods:**

- `handle()` - Main command entry point
- `_read_file()` - Auto-detect and parse file format
- `_read_csv()` - Parse CSV files
- `_read_json()` - Parse JSON files
- `_read_text()` - Handle plain text files

## Support

For issues or questions:

1. Check the error message and this documentation
2. Enable `--verbose` mode for detailed logging
3. Review Segment documentation at [segment.com/docs](https://segment.com/docs)
4. Open an issue in the metrics-utility repository

## Changelog

### Version 1.0.0 (2025-10-26)

- Initial release
- Support for CSV, JSON, and text files
- Segment SDK integration
- Application context tagging
- Comprehensive error handling
