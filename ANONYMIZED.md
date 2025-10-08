# Anonymized Metrics Architecture

This document describes the architecture and handling of anonymized metrics in the metrics-utility project for Ansible Automation Platform (AAP) Controller instances.

## Overview

The metrics-utility provides **dual collection modes** for AAP Controller data:

1. **Detailed Collection**: Full operational data including hostnames, organizations, and job details
2. **Anonymized Collection**: Statistical aggregations and rollups that remove individual host identification

The system combines **pseudonymization** (UUID-based identification) with **true anonymization** (statistical aggregation) to support both comprehensive analysis and privacy-preserving analytics.

## Architecture

### Data Collection Flow

```
Controller Database → Collectors → [Anonymization Layer] → Collections → Packages → Shipping Destinations
```

1. **Collectors** (`metrics_utility/automation_controller_billing/collectors.py`) extract raw data using `@register` decorated functions
2. **Anonymization Layer** (`metrics_utility/anonymized_rollups/`) processes data into statistical aggregations
3. **Collections** package data into JSON/CSV formats with tarball compression
4. **Packages** handle shipping to configured destinations with UUID-based identification
5. **Shipping** sends data to Red Hat Cloud Console, S3, or local directories

### Shipping Destinations

The system supports three shipping targets via `METRICS_UTILITY_SHIP_TARGET`:

#### 1. Red Hat Cloud Console (`crc`)
- **Endpoint**: `https://console.redhat.com/api/ingress/v1/upload`
- **Authentication**: Red Hat SSO via `https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token`
- **Content Type**: `application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz`
- **Implementation**: `metrics_utility/automation_controller_billing/package/package_crc.py`

#### 2. S3 Storage (`s3`)
- **Structure**: `data/{year}/{month}/{day}/`
- **Implementation**: `metrics_utility/automation_controller_billing/package/package_s3.py`

#### 3. Local Directory (`directory`)
- **Structure**: `data/{year}/{month}/{day}/`
- **Implementation**: `metrics_utility/automation_controller_billing/package/package_directory.py`

## Anonymization Mechanisms

The system provides multiple levels of anonymization from pseudonymization to statistical aggregation.

### 1. Anonymized Rollups (True Anonymization)

The `metrics_utility/anonymized_rollups/` module provides statistical aggregation that removes individual identification:

#### Job Host Summary Anonymization (`jobhostsummary_anonymized_rollup.py:8-36`)
```python
@staticmethod
def base(dataframe):
    # Groups by job_template_name instead of individual hosts
    aggregated = (
        dataframe.groupby('job_template_name')
        .agg(
            jobs_total=('job_id', 'nunique'),
            dark_total=('dark', 'sum'),
            failures_total=('failures', 'sum'),
            ok_total=('ok', 'sum'),
            hosts_total=('host_name', 'nunique'),  # Count only, no individual names
        )
    )
```

**Key Features**:
- Aggregates task execution statistics by job template
- Removes individual host identification
- Provides success rates and performance metrics
- Groups data temporally and functionally

#### Jobs Anonymization (`jobs_anonymized_rollup.py:10-50`)
- Job duration statistics by template (min/max/average)
- Job success/failure rates
- Waiting time analysis
- Template usage patterns

#### Event Modules Anonymization (`events_modules_anonymized_rollup.py:31-50`)
- Module usage statistics by collection type
- Task execution patterns
- Performance metrics by automation type
- Collection source analysis

#### Available Anonymized Collectors (`metrics_utility/library/collectors.py:27-30`)
```python
@collector
def anonymous(db=None, since=None, until=None, custom_params=None):
    return {'fake': 'anonymous_data'}
```

### 2. UUID-Based Pseudonymization

For non-anonymized data, the system uses UUIDs instead of customer-identifying information:

**Installation Identification** (`metrics_utility/automation_controller_billing/collectors.py:112-113`):
```python
'install_uuid': settings.INSTALL_UUID,
'instance_uuid': settings.SYSTEM_UUID,
```

**File Naming**:
- Red Hat Cloud Console: `{SYSTEM_UUID}-{timestamp}.tar.gz`
- S3/Directory: `{INSTALL_UUID}-{since}-{until}.tar.gz`

### Configuration Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `METRICS_UTILITY_SHIP_TARGET` | Destination (crc/s3/directory) | - |
| `METRICS_UTILITY_SHIP_PATH` | Target path for s3/directory | - |
| `METRICS_UTILITY_CRC_SSO_URL` | Red Hat SSO endpoint | `https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token` |
| `METRICS_UTILITY_CRC_INGRESS_URL` | Red Hat ingress endpoint | `https://console.redhat.com/api/ingress/v1/upload` |
| `METRICS_UTILITY_SERVICE_ACCOUNT_ID` | Service account for Red Hat auth | - |
| `METRICS_UTILITY_SERVICE_ACCOUNT_SECRET` | Service account secret | - |

## Data Collection Details

## Data Collection Modes

### Detailed Collection Mode

**Host Information Handling** (`metrics_utility/automation_controller_billing/dataframe_engine/dataframe_jobhost_summary_usage.py:36-44`):
```python
# Store the original host name for mapping purposes
billing_data['original_host_name'] = billing_data['host_name']

if 'ansible_host_variable' in billing_data.columns:
    # Replace missing ansible_host_variable with host name
    billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
    # Use ansible_host_variable instead of host_name
    billing_data['host_name'] = billing_data['ansible_host_variable']
```

**Organization Data** (`dataframe_jobhost_summary_usage.py:33`):
```python
billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
```

**Key Point**: In detailed mode, real hostnames and organization names are preserved and shipped.

### Anonymized Collection Mode

**Statistical Aggregation**: The anonymized rollups process detailed data to create:
- Aggregated counts and statistics by job template
- Success/failure rates without individual host identification  
- Performance metrics grouped by automation patterns
- Usage statistics by module/collection type

**Test Data Examples** (`test_anonymized_rollups/test_jobhostsummary_anonymized_rollups.py:6-30`):
- Individual host records: `['h1', 1001, 'T1']`, `['h2', 1001, 'T1']`
- Become aggregated statistics: `jobs_total`, `hosts_total` (counts only)
- No individual host names in final output

### Collected Data Types

**Config Data** (`metrics_utility/automation_controller_billing/collectors.py:102-147`):
- Platform information (OS, distribution, release)
- License details (subscription name, SKU, account number)
- Controller version and URL base
- Authentication backends
- Logging configuration

**Usage Data** (`@register` functions in `collectors.py`):
- Job execution metrics
- Host summaries with real hostnames
- Template usage statistics
- User activity data

## Anonymization Coverage

### What IS Anonymized (Rollup Mode)

**Individual Identification Removed**:
1. **Host Names**: Aggregated into counts (`hosts_total`) without individual names
2. **Task Details**: Statistical summaries (success rates, counts) instead of individual task records
3. **Job Instances**: Template-level statistics instead of individual job records
4. **Event Details**: Module usage patterns instead of specific event logs

**Preserved for Analysis**:
- Job template names (functional categorization)
- Time-based patterns and trends
- Performance and success metrics
- Module and collection usage statistics

### What Is NOT Anonymized (Detailed Mode)

**Detailed mode preserves actual identifying information**:
1. **Host Names**: Real hostnames from inventory
2. **Organization Names**: Actual AAP organization names  
3. **Job Template Names**: Real automation job template names
4. **Controller URLs**: Actual controller base URLs
5. **License Information**: Subscription names, SKUs, account numbers
6. **Individual Job Records**: Complete job execution details
7. **Event Logs**: Individual task and module execution records

## Data Aggregation and Deduplication

### Deduplication Logic

**Unique Index** (`dataframe_jobhost_summary_usage.py:138-139`):
```python
@staticmethod
def unique_index_columns():
    return ['organization_name', 'job_template_name', 'host_name', 'original_host_name', 'install_uuid', 'job_remote_id']
```

**Experimental Deduplication** (`dataframe_jobhost_summary_usage.py:47-52`):
- Enabled via `deduplicator=ccsp-experimental` parameter
- Tracks hostname changes for deduplication impact analysis
- Enriches direct managed nodes with canonical facts

### Data Aggregation

**Aggregation Fields** (`dataframe_jobhost_summary_usage.py:104-118`):
- Task run counts
- Host run counts
- First/last automation timestamps
- Job creation times
- Managed node types
- Event data
- Canonical facts

## Security Considerations

### Authentication to Red Hat Services

**Service Account Authentication** (`package_crc.py:34-38`):
```python
def _get_rh_user(self):
    return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID')

def _get_rh_password(self):
    return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET')
```

### Advisory Locking

**Database Locking** (`collector.py:52-60`):
```python
key = 'gather_automation_controller_billing_lock'
suffix = os.getenv('METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX')
if suffix:
    key = f'gather_automation_controller_billing_{suffix}_lock'

with self._pg_advisory_lock(key, wait=False) as acquired:
```

Prevents concurrent collection runs and conflicts with existing analytics systems.

## File Structure and Packaging

### Tarball Contents

Each shipped tarball contains:
- **config.json**: Installation and license metadata
- **job_host_summary_table.csv**: Host usage data
- **manifest.json**: Collection metadata
- **data_collection_status.json**: Collection status information

### File Naming Convention

- **Red Hat Cloud**: `{SYSTEM_UUID}-{timestamp}.tar.gz`
- **S3/Directory**: `{INSTALL_UUID}-{since_timestamp}-{until_timestamp}.tar.gz`

## Collection Mode Selection

### Configuration Options

The system supports different collection modes through:

1. **Collector Selection**: Choose between detailed collectors (`collectors.py`) and library collectors (`library/collectors.py`)
2. **Rollup Processing**: Apply anonymized rollups to detailed data before shipping
3. **Ship Target Configuration**: Direct anonymized vs detailed data to appropriate destinations

### Recommended Usage

**For Privacy-Sensitive Environments**:
- Use anonymized rollups (`anonymized_rollups/` modules)  
- Configure library collectors with `anonymous` collector
- Ship statistical aggregations instead of detailed records

**For Comprehensive Analysis**:
- Use detailed collectors with full data collection
- Apply UUID-based pseudonymization
- Ship to secure, authorized destinations only

## Conclusion

The metrics-utility now provides **flexible anonymization options**:

1. **True Anonymization**: Statistical aggregation through rollup modules removes individual identification while preserving analytical value
2. **Pseudonymization**: UUID-based installation identification for detailed data analysis
3. **Hybrid Approach**: Dual collection modes allow organizations to choose appropriate privacy levels

**Key Improvement**: The addition of `anonymized_rollups/` modules enables genuine anonymization through statistical aggregation, addressing privacy concerns while maintaining analytical utility for performance monitoring and usage analysis.

Organizations can now implement privacy-preserving analytics without sacrificing insights into automation patterns, success rates, and performance trends.