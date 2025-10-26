# Segment Integration - Implementation Summary

## Overview

This document summarizes the implementation of the `send_to_segment` CLI command, which allows sending CSV, JSON, and text file data to Segment.com for analytics tracking.

## Implementation Date

October 26, 2025

## Files Modified

### 1. `/pyproject.toml`

**Changes:** Added Segment SDK dependency

- Added `segment-analytics-python>=2.3.2` to the project dependencies

### 2. `/README.md`

**Changes:** Updated main documentation

- Added `send_to_segment` to the list of available commands
- Added example usage section with code samples
- Added references to detailed documentation

## Files Created

### 1. `/metrics_utility/management/commands/send_to_segment.py`

**Purpose:** Main CLI command implementation

**Features:**

- Accepts `--file`, `--app`, `--user-id`, and `--verbose` arguments
- Supports CSV, JSON, and plain text file formats
- Auto-detects file format based on extension
- Validates `SEGMENT_WRITE_KEY` environment variable
- Sends data as a single custom event to Segment
- Sets `context.app.name` from the `--app` parameter
- Includes comprehensive error handling
- Provides detailed logging in verbose mode

**Key Methods:**

- `handle()` - Main command entry point
- `_read_file()` - Auto-detect and parse file format
- `_read_csv()` - Parse CSV files into list of dictionaries
- `_read_json()` - Parse JSON files (arrays or objects)
- `_read_text()` - Handle plain text files

### 2. `/docs/send_to_segment.md`

**Purpose:** Comprehensive user documentation (1,400+ lines)

**Contents:**

- Overview and features
- Installation instructions
- Complete usage guide with all arguments
- 5+ practical examples
- File format specifications
- Segment event structure details
- Error handling guide
- Best practices
- Troubleshooting section
- Integration examples with existing commands

### 3. `/docs/send_to_segment_quickstart.md`

**Purpose:** Quick reference guide for rapid onboarding

**Contents:**

- 3-step quick setup
- Command syntax reference
- Quick examples
- Supported formats table
- Common issues and solutions
- Links to full documentation

## Command Usage

### Basic Syntax

```bash
python manage.py send_to_segment --file <FILE> --app <APP> [OPTIONS]
```

### Required Arguments

- `--file <path>` - Path to CSV/JSON/text file
- `--app <name>` - Application identifier

### Optional Arguments

- `--user-id <id>` - User ID (default: `system`)
- `--verbose` - Enable debug logging

### Required Environment Variable

- `SEGMENT_WRITE_KEY` - Your Segment write key

### Example Commands

**Send CSV file:**

```bash
export SEGMENT_WRITE_KEY="your_key_here"
python manage.py send_to_segment \
  --file metrics.csv \
  --app ansible-automation-platform
```

**Send with custom user:**

```bash
python manage.py send_to_segment \
  --file data.csv \
  --app awx \
  --user-id admin@example.com
```

**Debug mode:**

```bash
python manage.py send_to_segment \
  --file data.csv \
  --app my-app \
  --verbose
```

## Segment Event Structure

### Event Name Pattern

`{app}_data_upload`

**Examples:**

- `ansible-automation-platform_data_upload`
- `awx_data_upload`

### Event Properties

```json
{
  "filename": "metrics.csv",
  "file_path": "/path/to/metrics.csv",
  "data": [...],          // Parsed file content
  "row_count": 3,         // Number of rows/items
  "file_type": "csv",     // Detected type
  "timestamp": "2025-10-26T21:43:04.494644+00:00"
}
```

### Event Context

```json
{
  "app": {
    "name": "ansible-automation-platform"
  }
}
```

## File Format Support

| Format | Extensions     | Parsing Method                                     |
| ------ | -------------- | -------------------------------------------------- |
| CSV    | `.csv`         | `csv.DictReader()` - Each row becomes a dictionary |
| JSON   | `.json`        | `json.load()` - Arrays and objects supported       |
| Text   | `.txt`, others | Plain text with line-by-line breakdown             |

## Testing Results

### Test Files

- ✅ CSV file with 3 rows (hostname, managed, timestamp)
- ✅ All rows successfully parsed
- ✅ Data correctly sent to Segment API

### Test Output

```
Reading file: test_data.csv
Sending data to Segment: event=ansible-automation-platform_data_upload, user_id=system, rows=3
Successfully sent data to Segment: 3 rows from test_data.csv as event "ansible-automation-platform_data_upload"
```

### Error Handling Verified

- ✅ Missing `SEGMENT_WRITE_KEY` environment variable
- ✅ File not found errors
- ✅ Invalid file paths
- ✅ Segment API errors

## Code Quality

### Linting

- ✅ All code passes ruff linting
- ✅ Line length constraints (150 chars) satisfied
- ✅ Import sorting correct
- ✅ No syntax errors

### Code Style

- Follows existing project conventions
- Matches style of `build_report.py` and other commands
- Uses Django BaseCommand pattern
- Implements RawDescriptionHelpFormatter for help text

## Integration Points

### With Existing Commands

Can be used in a workflow with existing commands:

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

### Environment Variables

- `SEGMENT_WRITE_KEY` (required for this command)
- No conflicts with existing environment variables

## Dependencies Added

### Python Package

- `segment-analytics-python>=2.3.2`

**Transitive Dependencies:**

- `backoff~=2.1`
- `PyJWT~=2.10.1`
- `requests~=2.7` (already installed)
- `python-dateutil~=2.2` (already installed)

## Documentation Structure

```
docs/
├── send_to_segment.md           # Comprehensive guide (27KB)
└── send_to_segment_quickstart.md # Quick reference (3KB)

README.md                         # Updated with examples
```

## Future Enhancements (Optional)

Potential improvements for future consideration:

1. **Batch Processing**: Support for sending multiple files at once
2. **Data Transformation**: Option to transform/filter data before sending
3. **Custom Event Names**: Allow user to specify event name
4. **Rate Limiting**: Built-in rate limiting for large files
5. **Dry Run Mode**: Preview what would be sent without actually sending
6. **Progress Indicator**: Show progress for large files
7. **Compression**: Compress large payloads before sending
8. **Retry Logic**: Configurable retry on API failures

## Support Resources

### Documentation

- Full guide: `/docs/send_to_segment.md`
- Quick start: `/docs/send_to_segment_quickstart.md`
- Examples: Main `README.md`

### Command Help

```bash
python manage.py send_to_segment --help
```

### Segment Resources

- Segment Dashboard: https://app.segment.com
- Segment Docs: https://segment.com/docs

## Version Information

- Initial implementation: v1.0.0
- Compatible with: Python 3.11+
- Tested on: macOS 15.0 (darwin 25.0.0)

## Maintainer Notes

### Key Implementation Details

1. Uses Segment's official Python SDK (not raw HTTP requests)
2. Sends entire file as single event (per requirement 1c)
3. Uses Write Key authentication (per requirement 2a)
4. Sets `context.app.name` from `--app` parameter (per requirement 4a)
5. Event name derived from app parameter (per requirement 5b)

### Important Considerations

- Segment has payload size limits (~32KB per event)
- Large files may need to be split or compressed
- The SDK automatically batches and retries failed events
- `analytics.flush()` ensures events are sent before script exits

## Conclusion

The `send_to_segment` command has been successfully implemented with:

- ✅ Complete functionality as specified
- ✅ Comprehensive documentation
- ✅ Full error handling
- ✅ Code quality standards met
- ✅ Integration tested
- ✅ Ready for production use

The command is production-ready and can be used immediately with valid Segment write keys.
