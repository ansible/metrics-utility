# Send to Segment - Quick Start Guide

## 🚀 Quick Setup

1. **Install dependencies:**

   ```bash
   pip install -e .
   ```

2. **Set your Segment Write Key:**

   ```bash
   export SEGMENT_WRITE_KEY="your_write_key_here"
   ```

3. **Run the command:**
   ```bash
   python manage.py send_to_segment --file data.csv --app your-app-name
   ```

## 📋 Command Syntax

```bash
python manage.py send_to_segment --file <FILE> --app <APP> [OPTIONS]
```

### Required

- `--file <path>` - Path to CSV/JSON/text file
- `--app <name>` - App identifier (e.g., `ansible-automation-platform`)

### Optional

- `--user-id <id>` - User ID (default: `system`)
- `--verbose` - Enable debug logging

## 💡 Quick Examples

### CSV File

```bash
python manage.py send_to_segment \
  --file metrics.csv \
  --app ansible-automation-platform
```

### JSON File

```bash
python manage.py send_to_segment \
  --file report.json \
  --app awx
```

### With Custom User

```bash
python manage.py send_to_segment \
  --file data.csv \
  --app ansible-controller \
  --user-id admin@example.com
```

### Debug Mode

```bash
python manage.py send_to_segment \
  --file data.csv \
  --app my-app \
  --verbose
```

## 📊 Supported File Formats

| Format | Extension      | Auto-detected |
| ------ | -------------- | ------------- |
| CSV    | `.csv`         | ✅ Yes        |
| JSON   | `.json`        | ✅ Yes        |
| Text   | `.txt`, others | ✅ Yes        |

## 🎯 What Gets Sent

**Event Name:** `{app}_data_upload`

**Properties:**

- `filename` - File name
- `data` - Parsed file content
- `row_count` - Number of rows
- `file_type` - Detected type
- `timestamp` - Upload time

**Context:**

- `app.name` - Your app identifier

## ⚠️ Common Issues

| Error                     | Solution                         |
| ------------------------- | -------------------------------- |
| Missing SEGMENT_WRITE_KEY | `export SEGMENT_WRITE_KEY="..."` |
| File not found            | Check file path                  |
| CSV parse error           | Verify CSV format and encoding   |

## 📖 Full Documentation

See [send_to_segment.md](send_to_segment.md) for complete documentation.

## ✅ Verify in Segment

1. Go to [app.segment.com](https://app.segment.com)
2. Navigate to Debugger
3. Look for `{app}_data_upload` events
