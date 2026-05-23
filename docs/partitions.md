# Partition Analysis

**Last Updated**: May 2026

How `main_jobevent` partitioning affects collector performance, and how collectors leverage partition pruning.

See [collectors.md](./collectors.md) for the full list of collectors and their table usage.


## Partitioned Tables

### Tables with Hourly Partitions

| Table Name | Partition Key | Partition Type | Partition Naming Pattern |
|------------|---------------|----------------|-------------------------|
| `main_jobevent` | `job_created` (TIMESTAMPTZ) | RANGE (hourly) | `main_jobevent_YYYYMMDD_HH` |

**Note**: While other event tables (`main_adhoccommandevent`, `main_inventoryupdateevent`, `main_projectupdateevent`, `main_systemjobevent`) are also partitioned in the Controller database, **metrics-utility collectors do not currently access these tables**.

### Partition Structure

Each partition covers a **one-hour time range**:
- **Format**: `YYYY-MM-DD HH:00:00+00` to `YYYY-MM-DD HH+1:00:00+00`
- **Example**: `main_jobevent_20241219_17` covers `2024-12-19 17:00:00+00` to `2024-12-19 18:00:00+00`

---

## Collectors Accessing Partitioned Tables

Only two collectors access `main_jobevent`:

### `main_jobevent_service` (Preferred)

**Partition Pruning**: Optimal - uses literal timestamp ranges in WHERE clause.

**Strategy**:
1. Fetches jobs finished in the time window (gets `job_id` and `job_created`)
2. Extracts unique hour boundaries from `job_created` timestamps
3. Groups consecutive hours into ranges to reduce OR clauses
4. Builds WHERE clause with literal timestamp ranges for partition pruning
5. PostgreSQL prunes partitions because it sees literal timestamp values

**Example**: If jobs finished between `2024-12-19 17:30:00` and `2024-12-19 19:15:00`, accesses only:
- `main_jobevent_20241219_17`
- `main_jobevent_20241219_18`
- `main_jobevent_20241219_19`

### `main_jobevent` (Legacy)

**Partition Pruning**: Limited - uses JOIN on `job_created` which may not enable optimal partition pruning. PostgreSQL may scan more partitions than necessary.

**Recommendation**: Prefer `main_jobevent_service`.

---

## How Partition Pruning Works

PostgreSQL can **prune partitions** (skip scanning irrelevant partitions) when:
1. The WHERE clause contains **literal values** (not joins or functions) on the partition key
2. The partition key column is used in range conditions (`>=`, `<`, `BETWEEN`)
3. The query planner can statically determine which partitions to scan

### Comparison

| Collector | Pruning Strategy | Efficiency |
|-----------|-----------------|------------|
| `main_jobevent_service` | Literal timestamp ranges in WHERE clause | **High** - only relevant hourly partitions |
| `main_jobevent` (legacy) | JOIN on `job_created` | **Medium** - may scan extra partitions |

### Best Practices for Partition-Aware Collectors

1. **Use literal timestamp values** in WHERE clauses (not joins)
2. **Group consecutive hours** into ranges to reduce OR clauses
3. **Filter by partition key** (`job_created`) AND other filters (`job_id`, `event`)
4. **Prefer `main_jobevent_service`** over `main_jobevent` for better partition pruning

---

## Performance Considerations

### Partition Access Frequency

For a typical daily collection window (last 24 hours):

| Collector | Estimated Partitions Accessed |
|-----------|------------------------------|
| `main_jobevent_service` | **24 partitions** (one per hour) |
| `main_jobevent` (legacy) | **24-48 partitions** (may scan more due to JOIN) |

### Index Usage

All collectors leverage indexes where available:
- `main_unifiedjob.finished` - used by `job_host_summary_service` and `main_jobevent_service`
- `main_unifiedjob.created` - used by `unified_jobs` and `main_jobevent_service`
- `main_jobhostsummary.job_id` - used by `job_host_summary_service`
- `main_jobhostsummary.modified` - used by legacy collectors

### Query Optimization Tips

1. **Use service collectors** (`*_service.py`) over legacy collectors for better partition pruning
2. **Narrow time windows** when possible to reduce partition scans
3. **Monitor partition pruning** using `EXPLAIN ANALYZE` to verify only relevant partitions are scanned
4. **Consider partition maintenance** - ensure partitions exist for the time range being queried

---

## References

- PostgreSQL Partition Pruning: https://www.postgresql.org/docs/current/ddl-partitioning.html#DDL-PARTITION-PRUNING
- Collector Source Code: `metrics_utility/library/collectors/controller/`
- Partition Schema: `tools/docker/latest.sql`
