# Metrics Utility Collectors

**Last Updated**: May 2026

All collectors in the metrics-utility library, including which database tables each queries and whether they support time range filtering.

See also [partitions.md](./partitions.md) for partition pruning analysis on `main_jobevent`.


## Collector Documentation

### 1. `execution_environments`

**File**: `metrics_utility/library/collectors/controller/execution_environments.py`

**Purpose**: Collects execution environment configuration data.

**Tables Accessed**:
- `main_executionenvironment` (READ)

**Query Pattern**:
```sql
SELECT id, created, modified, description, image, managed, 
       created_by_id, credential_id, modified_by_id, organization_id, name, pull
FROM main_executionenvironment
```

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters
- Collects all execution environments

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

---

### 3. `main_jobevent_service`

**File**: `metrics_utility/library/collectors/controller/main_jobevent_service.py`

**Purpose**: Collects job events for jobs that finished within a time window. **This is the most partition-aware collector** — see [partitions.md](./partitions.md) for details.

**Tables Accessed**:
- `main_unifiedjob` (READ) - To get job IDs and `job_created` timestamps
- `main_jobevent` (READ) - **PARTITIONED TABLE** - Filtered by `job_created` and `job_id`

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
  AND e.job_id IN (1, 2, 3, ...)
  AND e.event IN ('runner_on_ok', 'runner_on_failed', ...)
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_unifiedjob.finished` to find relevant jobs
- Then filters `main_jobevent` by `job_created` (partition key) and `job_id`

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

---

### 5. `main_jobevent` (Legacy)

**File**: `metrics_utility/library/collectors/controller/main_jobevent.py`

**Purpose**: Legacy collector that collects job events filtered by `main_jobhostsummary.modified` timestamp.

**Tables Accessed**:
- `main_jobhostsummary` (READ) - Filtered by `modified` timestamp
- `main_unifiedjob` (READ) - JOIN for `job_created`
- `main_jobevent` (READ) - **PARTITIONED TABLE** - Joined via `job_created` and `job_id`

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

**Query Pattern**:
```sql
SELECT mjs.*, mu.*, mi.*, mo.*, ...
FROM main_jobhostsummary mjs
WHERE mjs.modified >= 'since' AND mjs.modified < 'until'
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_jobhostsummary.modified` timestamp

**Recommendation**: Prefer `job_host_summary_service` which filters by job `finished` timestamp for better alignment with job completion times.

---

### 7. `config`

**File**: `metrics_utility/library/collectors/controller/config.py`

**Purpose**: Collects Controller configuration settings and license information.

**Tables Accessed**:
- `conf_setting` (READ) - Filtered by `key` IN (...)
- `main_instance` (READ) - To get Controller version

**Query Pattern**:
```sql
SELECT key, value FROM conf_setting WHERE key IN ('AUTHENTICATION_BACKENDS', 'INSTALL_UUID', ...)

SELECT version FROM main_instance WHERE enabled = true AND version IS NOT NULL ORDER BY last_seen DESC LIMIT 1
```

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters
- Collects current configuration state

---

### 8. `main_host`

**File**: `metrics_utility/library/collectors/controller/main_host.py`

**Purpose**: Collects all enabled hosts.

**Tables Accessed**:
- `main_host` (READ) - Filtered by `enabled='t'`
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjob` (READ) - LEFT JOIN for `last_job_id`

**Query Pattern**:
```sql
SELECT main_host.*, main_inventory.*, main_organization.*, ...
FROM main_host
WHERE enabled='t'
```

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters
- Collects all enabled hosts

---

### 9. `main_host_daily`

**File**: `metrics_utility/library/collectors/controller/main_host.py`

**Purpose**: Collects hosts created or modified within a time window.

**Tables Accessed**:
- `main_host` (READ) - Filtered by `created` OR `modified` timestamp
- `main_inventory` (READ) - LEFT JOIN
- `main_organization` (READ) - LEFT JOIN
- `main_unifiedjob` (READ) - LEFT JOIN

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

---

### 11. `config_django`

**File**: `metrics_utility/library/collectors/controller/config_django.py`

**Purpose**: Collects Controller configuration via AWX Django APIs (settings, license info, versions, platform details).

**Tables Accessed**:
- None (uses AWX Django APIs directly)

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters

---

### 12. `controller_version_service`

**File**: `metrics_utility/library/collectors/controller/controller_version_service.py`

**Purpose**: Collects distinct controller versions from enabled instances with control/hybrid node types.

**Tables Accessed**:
- `main_instance` (READ)

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters

---

### 13. `credentials_service`

**File**: `metrics_utility/library/collectors/controller/credentials_service.py`

**Purpose**: Collects distinct managed credential type names used in jobs within a time window.

**Tables Accessed**:
- `main_unifiedjob_credentials` (READ)
- `main_unifiedjob` (READ) - Filtered by `finished` timestamp
- `main_credential` (READ)
- `main_credentialtype` (READ)

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_unifiedjob.finished` timestamp

---

### 14. `feature_flags_service`

**File**: `metrics_utility/library/collectors/controller/feature_flags_service.py`

**Purpose**: Collects enabled feature flags from the controller.

**Tables Accessed**:
- `dab_feature_flags_aapflag` (READ)

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters

---

### 15. `table_metadata`

**File**: `metrics_utility/library/collectors/controller/table_metadata.py`

**Purpose**: Collects row count and size information for partitioned and regular tables.

**Tables Accessed**:
- PostgreSQL system tables (`pg_class`, `pg_inherits`, etc.) for metadata about `main_jobevent`, `main_unifiedjob`, `main_jobhostsummary`

**Time Range Support**:
- ❌ Does not support `since`/`until` parameters

---

### 16. `task_executions_service`

**File**: `metrics_utility/library/collectors/service/task_executions_service.py`

**Purpose**: Collects task execution statistics from the metrics-service database for internal observability.

**Tables Accessed**:
- `tasks_taskexecution` (READ, from metrics-service database)

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `started_at` timestamp

---

### 17. `dashboard_jobs`

**File**: `metrics_utility/library/collectors/dashboard/collectors.py`

**Purpose**: Collects job data for the dashboard including job details, labels, and host summaries.

**Tables Accessed**:
- `main_unifiedjob` (READ) - Filtered by `modified` timestamp
- `main_job` (READ)
- `main_unifiedjobtemplate` (READ)
- `auth_user` (READ)
- `main_project` (READ)
- `main_unifiedjob_labels` (READ)
- `main_jobhostsummary` (READ)

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_unifiedjob.modified` timestamp

---

### 18. `total_workers_vcpu`

**File**: `metrics_utility/library/collectors/others/total_workers_vcpu.py`

**Purpose**: Collects total worker vCPU count from Prometheus for the previous hour.

**Tables Accessed**:
- None (queries Prometheus HTTP API, not database)

**Time Range Support**:
- ❌ Automatically queries previous hour

---

### 19. `main_hostmetric`

**File**: `metrics_utility/library/collectors/controller/main_hostmetric.py`

**Purpose**: Collects host metric data (automation history, deletion status) joined with host facts. Used by the Renewal Guidance report.

**Tables Accessed**:
- `main_hostmetric` (READ) - Filtered by `last_automation` timestamp
- `main_host` (READ) - LEFT JOIN for host variables and ansible facts

**Query Pattern**:
```sql
SELECT main_hostmetric.hostname, COALESCE(main_host.id, 0) AS host_id,
       main_hostmetric.first_automation, main_hostmetric.last_automation,
       main_hostmetric.automated_counter, main_hostmetric.deleted_counter,
       main_hostmetric.last_deleted, main_hostmetric.deleted,
       main_host.ansible_facts->>'ansible_product_serial', ...
FROM main_hostmetric
LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname
WHERE main_hostmetric.last_automation >= 'since' AND main_hostmetric.last_automation < 'until'
ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
```

**Time Range Support**:
- ✅ **Supports `since`/`until` parameters**
- Filters by `main_hostmetric.last_automation` timestamp

---

## Summary Table

| Collector | Tables | Partitioned? | Time Range | Usage |
|-----------|--------|:---:|:---:|---|
| `execution_environments` | `main_executionenvironment` | | | Daily snapshot |
| `job_host_summary_service` | `main_unifiedjob`, `main_jobhostsummary`, `main_host`, `main_job`, `main_unifiedjobtemplate`, `main_inventory`, `main_organization` | | ✅ | **Preferred** |
| `main_jobevent_service` | `main_unifiedjob`, `main_jobevent` | ✅ | ✅ | **Preferred** |
| `unified_jobs` | `main_unifiedjob`, `main_unifiedjobtemplate`, `django_content_type`, `main_job`, `main_inventory`, `main_organization`, `main_executionenvironment` | | ✅ | **Preferred** |
| `main_jobevent` (legacy) | `main_jobhostsummary`, `main_jobevent` | ✅ | ✅ | Legacy |
| `job_host_summary` (legacy) | `main_jobhostsummary`, `main_host`, `main_job`, `main_unifiedjobtemplate`, `main_inventory`, `main_organization`, `main_unifiedjob` | | ✅ | Legacy |
| `config` | `conf_setting`, `main_instance` | | | Daily snapshot |
| `main_host` | `main_host`, `main_inventory`, `main_organization`, `main_unifiedjob` | | | Daily snapshot |
| `main_host_daily` | `main_host`, `main_inventory`, `main_organization`, `main_unifiedjob` | | ✅ | Incremental |
| `main_hostmetric` | `main_hostmetric`, `main_host` | | ✅ | Renewal Guidance |
| `main_indirectmanagednodeaudit` | `main_indirectmanagednodeaudit`, `main_job`, `main_unifiedjob`, `main_inventory`, `main_organization`, `main_unifiedjobtemplate` | | ✅ | Incremental |
| `config_django` | (AWX Django APIs) | | | Daily snapshot |
| `controller_version_service` | `main_instance` | | | Daily snapshot |
| `credentials_service` | `main_unifiedjob_credentials`, `main_unifiedjob`, `main_credential`, `main_credentialtype` | | ✅ | Incremental |
| `feature_flags_service` | `dab_feature_flags_aapflag` | | | Daily snapshot |
| `table_metadata` | (PostgreSQL system tables) | | | Daily snapshot |
| `task_executions_service` | `tasks_taskexecution` (metrics-service DB) | | ✅ | Incremental |
| `dashboard_jobs` | `main_unifiedjob`, `main_job`, `main_unifiedjobtemplate`, `auth_user`, `main_project`, `main_unifiedjob_labels`, `main_jobhostsummary` | | ✅ | Incremental |
| `total_workers_vcpu` | (Prometheus API) | | | Daily snapshot |
