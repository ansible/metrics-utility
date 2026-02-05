# Metrics Utility Collectors: Database Tables and Partition Analysis

**Last Updated**: February 2026

## Overview

This document provides comprehensive documentation on all collectors in the metrics-utility library, including:
- Which database tables each collector queries
- Whether tables are partitioned
- Partition key columns and structure
- How collectors leverage partition pruning
- Frequency of partition access

## Table of Contents

1. [Partitioned Tables Overview](#partitioned-tables-overview)
2. [Collector Documentation](#collector-documentation)
3. [Partition Pruning Strategies](#partition-pruning-strategies)
4. [Performance Considerations](#performance-considerations)

---

## Partitioned Tables Overview

### Tables with Hourly Partitions

The following tables in Ansible Automation Platform Controller are partitioned by **hourly ranges** on the `job_created` column:

| Table Name | Partition Key | Partition Type | Partition Naming Pattern |
|------------|---------------|----------------|-------------------------|
| `main_jobevent` | `job_created` (TIMESTAMPTZ) | RANGE (hourly) | `main_jobevent_YYYYMMDD_HH` |

**Note**: While other event tables (`main_adhoccommandevent`, `main_inventoryupdateevent`, `main_projectupdateevent`, `main_systemjobevent`) are also partitioned in the Controller database, **metrics-utility collectors do not currently access these tables**, so they are not documented here.

### Partition Structure

Each partition covers a **one-hour time range**:
- **Format**: `YYYY-MM-DD HH:00:00+00` to `YYYY-MM-DD HH+1:00:00+00`
- **Example**: `main_jobevent_20241219_17` covers `2024-12-19 17:00:00+00` to `2024-12-19 18:00:00+00`

### Non-Partitioned Tables

The following tables are **NOT partitioned**:
- `main_executionenvironment`
- `main_unifiedjob`
- `main_jobhostsummary`
- `main_host`
- `main_inventory`
- `main_organization`
- `main_unifiedjobtemplate`
- `main_indirectmanagednodeaudit`
- `conf_setting`
- `main_instance`

---

## Collector Documentation

### 1. `execution_environments`

**File**: `metrics_utility/library/collectors/controller/execution_environments.py`

**Purpose**: Collects execution environment configuration data.

**Tables Accessed**:
- `main_executionenvironment` (READ)

**Partition Information**:
- ❌ **Not partitioned**
- No partition pruning needed

**Query Pattern**:
```sql
SELECT id, created, modified, description, image, managed, 
       created_by_id, credential_id, modified_by_id, organization_id, name, pull
FROM main_executionenvironment
```

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters
- Collects all execution environments

**Frequency**:
- Typically run once per collection cycle (daily)

---

### 2. `job_host_summary_service`

**File**: `metrics_utility/library/collectors/controller/job_host_summary_service.py`

**Purpose**: Collects job host summary data for jobs that finished within a time window.

**Tables Accessed**:
- `main_unifiedjob` (READ) - Filtered by `finished` timestamp
- `main_jobhostsummary` (READ) - Joined via `job_id`
- `main_host` (READ) - LEFT JOIN for host variables
- `main_job` (READ) - LEFT JOIN for inventory/project relationships
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjobtemplate` (READ) - LEFT JOIN for project info

**Partition Information**:
- ❌ **Not partitioned** (all tables are non-partitioned)
- Uses index on `main_unifiedjob.finished` for filtering
- Uses index on `main_jobhostsummary.job_id` for joining

**Query Pattern**:
```sql
WITH filtered_jobs AS (
    SELECT mu.id
    FROM main_unifiedjob mu
    WHERE mu.finished >= 'since' AND mu.finished < 'until'
      AND mu.finished IS NOT NULL
),
filtered_hosts AS (
    SELECT DISTINCT mjs.host_id
    FROM main_jobhostsummary mjs
    JOIN filtered_jobs fj ON fj.id = mjs.job_id
)
SELECT mjs.*, mu.*, mi.*, mo.*, mup.*
FROM filtered_jobs fj
JOIN main_jobhostsummary mjs ON mjs.job_id = fj.id
LEFT JOIN main_unifiedjob mu ON mu.id = mjs.job_id
-- ... additional LEFT JOINs
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_unifiedjob.finished` timestamp

**Optimization Strategy**:
1. First filters jobs by `finished` timestamp (uses index)
2. Then filters host summaries by `job_id` (uses index)
3. Reduces data volume before expensive joins

**Frequency**:
- Run per collection window (typically daily)
- Accesses only jobs finished in the specified time range

---

### 3. `main_jobevent_service`

**File**: `metrics_utility/library/collectors/controller/main_jobevent_service.py`

**Purpose**: Collects job events for jobs that finished within a time window. **This is the most partition-aware collector.**

**Tables Accessed**:
- `main_unifiedjob` (READ) - To get job IDs and `job_created` timestamps
- `main_jobevent` (READ) - **PARTITIONED TABLE** - Filtered by `job_created` and `job_id`

**Partition Information**:
- ✅ **`main_jobevent` is PARTITIONED** by `job_created` (hourly partitions)
- **Partition Key**: `job_created` (TIMESTAMPTZ)
- **Partition Type**: RANGE partitioning (hourly)

**Query Pattern**:
```sql
-- Step 1: Get jobs finished in time window
SELECT uj.id AS job_id, uj.created AS job_created
FROM main_unifiedjob uj
WHERE uj.finished >= %(since)s AND uj.finished < %(until)s

-- Step 2: Extract unique hour boundaries from job_created
-- Step 3: Build partition-pruning WHERE clause with literal timestamps
SELECT e.*
FROM main_jobevent e
WHERE (e.job_created >= '2024-12-19 17:00:00+00' AND e.job_created < '2024-12-19 18:00:00+00')
   OR (e.job_created >= '2024-12-19 18:00:00+00' AND e.job_created < '2024-12-19 19:00:00+00')
   -- ... more hour ranges
  AND e.job_id IN (1, 2, 3, ...)
  AND e.event IN ('runner_on_ok', 'runner_on_failed', ...)
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_unifiedjob.finished` to find relevant jobs
- Then filters `main_jobevent` by `job_created` (partition key) and `job_id`

**Partition Pruning Strategy**:
1. **Fetches jobs** finished in the time window (gets `job_id` and `job_created`)
2. **Extracts unique hour boundaries** from `job_created` timestamps
3. **Groups consecutive hours** into ranges to reduce OR clauses
4. **Builds WHERE clause** with literal timestamp ranges for partition pruning
5. **PostgreSQL can prune partitions** because it sees literal timestamp values

**Example Partition Access**:
- If jobs finished between `2024-12-19 17:30:00` and `2024-12-19 19:15:00`
- Collector accesses partitions:
  - `main_jobevent_20241219_17` (if any jobs created in hour 17)
  - `main_jobevent_20241219_18` (if any jobs created in hour 18)
  - `main_jobevent_20241219_19` (if any jobs created in hour 19)

**Frequency**:
- Run per collection window (typically daily)
- Accesses **only the hourly partitions** that contain events for jobs finished in the time window
- **Partition pruning significantly reduces scan cost** compared to scanning all partitions

**Performance Notes**:
- The collector explicitly optimizes for partition pruning by:
  - Using literal timestamp values (not joins) in WHERE clause
  - Grouping consecutive hours into ranges
  - Filtering by both `job_created` (partition key) and `job_id`

---

### 4. `unified_jobs`

**File**: `metrics_utility/library/collectors/controller/unified_jobs.py`

**Purpose**: Collects unified job data (jobs created or finished within a time window).

**Tables Accessed**:
- `main_unifiedjob` (READ) - Filtered by `created` OR `finished` timestamp
- `main_unifiedjobtemplate` (READ) - LEFT JOIN
- `django_content_type` (READ) - LEFT JOIN for polymorphic type
- `main_job` (READ) - LEFT JOIN
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_executionenvironment` (READ) - LEFT JOIN

**Partition Information**:
- ❌ **Not partitioned** (all tables are non-partitioned)
- Uses indexes on `main_unifiedjob.created` and `main_unifiedjob.finished`

**Query Pattern**:
```sql
SELECT main_unifiedjob.*, ...
FROM main_unifiedjob
WHERE (main_unifiedjob.created >= 'since' AND main_unifiedjob.created < 'until')
   OR (main_unifiedjob.finished >= 'since' AND main_unifiedjob.finished < 'until')
  AND main_unifiedjob.launch_type != 'sync'
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `created` OR `finished` timestamp (OR condition)

**Frequency**:
- Run per collection window (typically daily)
- Accesses jobs that were either created or finished in the time window

---

### 5. `main_jobevent` (Legacy)

**File**: `metrics_utility/library/collectors/controller/main_jobevent.py`

**Purpose**: Legacy collector that collects job events filtered by `main_jobhostsummary.modified` timestamp.

**Tables Accessed**:
- `main_jobhostsummary` (READ) - Filtered by `modified` timestamp
- `main_unifiedjob` (READ) - JOIN for `job_created`
- `main_jobevent` (READ) - **PARTITIONED TABLE** - Joined via `job_created` and `job_id`

**Partition Information**:
- ✅ **`main_jobevent` is PARTITIONED** by `job_created` (hourly partitions)
- **Partition Key**: `job_created` (TIMESTAMPTZ)

**Query Pattern**:
```sql
WITH job_scope AS (
    SELECT mjs.id, mjs.job_id, mjs.host_name, mu.created AS job_created
    FROM main_jobhostsummary mjs
    JOIN main_unifiedjob mu ON mu.id = mjs.job_id
    WHERE mjs.modified >= 'since' AND mjs.modified < 'until'
)
SELECT e.*
FROM main_jobevent e
JOIN job_scope ON job_scope.job_created = e.job_created
              AND job_scope.job_id = e.job_id
              AND job_scope.host_name = e.host_name
WHERE e.event IN ('runner_on_ok', 'runner_on_failed', ...)
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_jobhostsummary.modified` timestamp

**Partition Pruning**:
- ⚠️ **Limited partition pruning** - Uses JOIN on `job_created` which may not enable optimal partition pruning
- PostgreSQL may need to scan multiple partitions if the JOIN condition doesn't match partition boundaries exactly

**Frequency**:
- Run per collection window (typically daily)
- **Note**: This collector is less efficient than `main_jobevent_service` for partition pruning

**Recommendation**: Prefer `main_jobevent_service` over this collector for better partition pruning.

---

### 6. `job_host_summary` (Legacy)

**File**: `metrics_utility/library/collectors/controller/job_host_summary.py`

**Purpose**: Legacy collector that collects job host summaries filtered by `main_jobhostsummary.modified` timestamp.

**Tables Accessed**:
- `main_jobhostsummary` (READ) - Filtered by `modified` timestamp
- `main_host` (READ) - LEFT JOIN for host variables
- `main_job` (READ) - LEFT JOIN
- `main_unifiedjob` (READ) - LEFT JOIN
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjobtemplate` (READ) - LEFT JOIN

**Partition Information**:
- ❌ **Not partitioned** (all tables are non-partitioned)

**Query Pattern**:
```sql
SELECT mjs.*, mu.*, mi.*, mo.*, ...
FROM main_jobhostsummary mjs
WHERE mjs.modified >= 'since' AND mjs.modified < 'until'
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_jobhostsummary.modified` timestamp

**Frequency**:
- Run per collection window (typically daily)

**Recommendation**: Prefer `job_host_summary_service` which filters by job `finished` timestamp for better alignment with job completion times.

---

### 7. `config`

**File**: `metrics_utility/library/collectors/controller/config.py`

**Purpose**: Collects Controller configuration settings and license information.

**Tables Accessed**:
- `conf_setting` (READ) - Filtered by `key` IN (...)
- `main_instance` (READ) - To get Controller version

**Partition Information**:
- ❌ **Not partitioned** (both tables are non-partitioned)

**Query Pattern**:
```sql
SELECT key, value FROM conf_setting WHERE key IN ('AUTHENTICATION_BACKENDS', 'INSTALL_UUID', ...)

SELECT version FROM main_instance WHERE enabled = true AND version IS NOT NULL ORDER BY last_seen DESC LIMIT 1
```

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters
- Collects current configuration state

**Frequency**:
- Typically run once per collection cycle (daily)

---

### 8. `main_host`

**File**: `metrics_utility/library/collectors/controller/main_host.py`

**Purpose**: Collects all enabled hosts.

**Tables Accessed**:
- `main_host` (READ) - Filtered by `enabled='t'`
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjob` (READ) - LEFT JOIN for `last_job_id`

**Partition Information**:
- ❌ **Not partitioned** (all tables are non-partitioned)

**Query Pattern**:
```sql
SELECT main_host.*, main_inventory.*, main_organization.*, ...
FROM main_host
WHERE enabled='t'
```

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters
- Collects all enabled hosts

**Frequency**:
- Typically run once per collection cycle (daily)

---

### 9. `main_host_daily`

**File**: `metrics_utility/library/collectors/controller/main_host.py`

**Purpose**: Collects hosts created or modified within a time window.

**Tables Accessed**:
- `main_host` (READ) - Filtered by `created` OR `modified` timestamp
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjob` (READ) - LEFT JOIN

**Partition Information**:
- ❌ **Not partitioned** (all tables are non-partitioned)

**Query Pattern**:
```sql
SELECT main_host.*, ...
FROM main_host
WHERE enabled='t'
  AND (main_host.created >= 'since' AND main_host.created < 'until'
    OR main_host.modified >= 'since' AND main_host.modified < 'until')
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `created` OR `modified` timestamp

**Frequency**:
- Run per collection window (typically daily)

---

### 10. `main_indirectmanagednodeaudit`

**File**: `metrics_utility/library/collectors/controller/main_indirectmanagednodeaudit.py`

**Purpose**: Collects indirect managed node audit data.

**Tables Accessed**:
- `main_indirectmanagednodeaudit` (READ) - Filtered by `created` timestamp
- `main_job` (READ) - LEFT JOIN
- `main_unifiedjob` (READ) - LEFT JOIN
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjobtemplate` (READ) - LEFT JOIN

**Partition Information**:
- ❌ **Not partitioned** (all tables are non-partitioned)

**Query Pattern**:
```sql
SELECT main_indirectmanagednodeaudit.*, ...
FROM main_indirectmanagednodeaudit
WHERE main_indirectmanagednodeaudit.created >= 'since'
  AND main_indirectmanagednodeaudit.created < 'until'
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `created` timestamp

**Frequency**:
- Run per collection window (typically daily)

---

## Partition Pruning Strategies

### How Partition Pruning Works

PostgreSQL can **prune partitions** (skip scanning irrelevant partitions) when:
1. The WHERE clause contains **literal values** (not joins or functions) on the partition key
2. The partition key column is used in range conditions (`>=`, `<`, `BETWEEN`)
3. The query planner can statically determine which partitions to scan

### Collector Partition Pruning Comparison

| Collector | Partitioned Table | Partition Pruning Strategy | Efficiency |
|-----------|-------------------|---------------------------|-------------|
| `main_jobevent_service` | `main_jobevent` | ✅ **Optimal** - Uses literal timestamp ranges in WHERE clause | **High** - Accesses only relevant hourly partitions |
| `main_jobevent` (legacy) | `main_jobevent` | ⚠️ **Limited** - Uses JOIN on `job_created` | **Medium** - May scan more partitions than necessary |

### Best Practices for Partition-Aware Collectors

1. **Use literal timestamp values** in WHERE clauses (not joins)
2. **Group consecutive hours** into ranges to reduce OR clauses
3. **Filter by partition key** (`job_created`) AND other filters (`job_id`, `event`)
4. **Prefer `main_jobevent_service`** over `main_jobevent` for better partition pruning

---

## Performance Considerations

### Partition Access Frequency

For a typical daily collection window (e.g., last 24 hours):

| Collector | Partitioned Tables | Estimated Partitions Accessed |
|-----------|-------------------|------------------------------|
| `main_jobevent_service` | `main_jobevent` | **24 partitions** (one per hour) |
| `main_jobevent` (legacy) | `main_jobevent` | **24-48 partitions** (may scan more due to JOIN) |

### Index Usage

All collectors leverage indexes where available:
- `main_unifiedjob.finished` - Used by `job_host_summary_service` and `main_jobevent_service`
- `main_unifiedjob.created` - Used by `unified_jobs` and `main_jobevent_service`
- `main_jobhostsummary.job_id` - Used by `job_host_summary_service`
- `main_jobhostsummary.modified` - Used by legacy collectors

### Query Optimization Tips

1. **Use service collectors** (`*_service.py`) over legacy collectors for better partition pruning
2. **Narrow time windows** when possible to reduce partition scans
3. **Monitor partition pruning** using `EXPLAIN ANALYZE` to verify only relevant partitions are scanned
4. **Consider partition maintenance** - Ensure partitions exist for the time range being queried

---

## Summary Table

| Collector | Tables | Partitioned? | Partition Key | Time Range Support | Recommended Usage |
|-----------|--------|---------------|---------------|-------------------|-------------------|
| `execution_environments` | `main_executionenvironment` | ❌ | N/A | ❌ | Daily snapshot |
| `job_host_summary_service` | `main_unifiedjob`, `main_jobhostsummary`, ... | ❌ | N/A | ✅ | ✅ **Preferred** |
| `main_jobevent_service` | `main_unifiedjob`, `main_jobevent` | ✅ | `job_created` (hourly) | ✅ | ✅ **Preferred** |
| `unified_jobs` | `main_unifiedjob`, ... | ❌ | N/A | ✅ | ✅ **Preferred** |
| `main_jobevent` (legacy) | `main_jobhostsummary`, `main_jobevent` | ✅ | `job_created` (hourly) | ✅ | ⚠️ Legacy |
| `job_host_summary` (legacy) | `main_jobhostsummary`, ... | ❌ | N/A | ✅ | ⚠️ Legacy |
| `config` | `conf_setting`, `main_instance` | ❌ | N/A | ❌ | Daily snapshot |
| `main_host` | `main_host`, ... | ❌ | N/A | ❌ | Daily snapshot |
| `main_host_daily` | `main_host`, ... | ❌ | N/A | ✅ | Incremental |
| `main_indirectmanagednodeaudit` | `main_indirectmanagednodeaudit`, ... | ❌ | N/A | ✅ | Incremental |

---

## References

- PostgreSQL Partition Pruning: https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITION-PRUNING
- Collector Source Code: `metrics_utility/library/collectors/controller/`
- Partition Schema: `tools/docker/latest.sql`
