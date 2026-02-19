from ..util import collector, copy_table


@collector
def table_metadata(*, db=None):
    """
    Collect table metadata (size and estimated row counts) for key tables.
    
    This collector queries PostgreSQL system catalogs to get:
    - Table size in bytes (exact, very fast)
    - Total size including indexes and TOAST (exact, very fast)
    - Estimated row count from statistics (approximate, very fast)
    
    Accuracy:
    - Size queries are EXACT and very fast (just read metadata)
    - Row count estimates are based on PostgreSQL statistics (pg_class.reltuples)
      which are updated by ANALYZE/VACUUM. They're usually accurate within a few
      percent but can be off if statistics are stale. For exact counts, you'd
      need to run COUNT(*) which scans the entire table (slow).
    
    Note on sizes:
    - Sizes reflect actual disk usage including indexes, TOAST, and overhead
    - PostgreSQL allocates space in 8KB pages, so even small tables have minimum sizes
    - For partitioned tables (like main_jobevent), sizes are summed from all partitions
    - Index sizes can be significant, especially for tables with many indexes or large indexes
    
    Tables collected:
    - main_jobevent (events table, partitioned by job_created hour)
    - main_unifiedjob (jobs table, regular table)
    - main_jobhostsummary (job host summary table, regular table)
    """
    query = """
        WITH table_list AS (
            SELECT 'public'::text AS schemaname, 'main_jobevent'::text AS tablename
            UNION ALL
            SELECT 'public'::text AS schemaname, 'main_unifiedjob'::text AS tablename
            UNION ALL
            SELECT 'public'::text AS schemaname, 'main_jobhostsummary'::text AS tablename
        ),
        table_info AS (
            SELECT
                t.schemaname,
                t.tablename,
                c.oid AS table_oid,
                c.relkind,
                c.reltuples
            FROM table_list t
            JOIN pg_class c ON c.relname = t.tablename
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
            WHERE c.relkind IN ('r', 'p')  -- regular tables and partitioned tables
        ),
        partition_sizes AS (
            -- For partitioned tables, sum sizes from all child partitions
            -- Use pg_inherits to find all partitions (range partitions are regular tables, not partitioned)
            -- Partitions in PostgreSQL are regular tables (relkind='r') that inherit from the parent
            SELECT
                ti.schemaname,
                ti.tablename,
                COALESCE(SUM(pg_total_relation_size(p.oid)), 0)::BIGINT AS total_size_bytes,
                COALESCE(SUM(pg_relation_size(p.oid)), 0)::BIGINT AS table_size_bytes,
                COALESCE(SUM(pg_indexes_size(p.oid)), 0)::BIGINT AS indexes_size_bytes,
                COALESCE(SUM(p.reltuples)::BIGINT, 0) AS estimated_row_count
            FROM table_info ti
            INNER JOIN pg_inherits inh ON inh.inhparent = ti.table_oid
            INNER JOIN pg_class p ON p.oid = inh.inhrelid AND p.relkind = 'r'  -- partitions are regular tables
            WHERE ti.relkind = 'p'  -- only for partitioned tables
            GROUP BY ti.schemaname, ti.tablename
        )
        SELECT
            ti.schemaname,
            ti.tablename,
            -- For partitioned tables, use sum from partitions; for regular tables, use direct size
            (CASE
                WHEN ti.relkind = 'p' THEN COALESCE(ps.total_size_bytes, 0)
                ELSE pg_total_relation_size(ti.table_oid)
            END)::BIGINT AS total_size_bytes,
            (CASE
                WHEN ti.relkind = 'p' THEN COALESCE(ps.table_size_bytes, 0)
                ELSE pg_relation_size(ti.table_oid)
            END)::BIGINT AS table_size_bytes,
            (CASE
                WHEN ti.relkind = 'p' THEN COALESCE(ps.indexes_size_bytes, 0)
                ELSE pg_indexes_size(ti.table_oid)
            END)::BIGINT AS indexes_size_bytes,
            -- Estimated row count: for partitioned tables, sum from partitions; for regular, use reltuples
            (CASE
                WHEN ti.relkind = 'p' THEN COALESCE(ps.estimated_row_count, 0)
                ELSE COALESCE(ti.reltuples::BIGINT, 0)
            END)::BIGINT AS estimated_row_count,
            -- Last statistics update time (helps assess accuracy of estimates)
            s.last_analyze,
            s.last_vacuum
        FROM table_info ti
        LEFT JOIN partition_sizes ps ON ps.schemaname = ti.schemaname AND ps.tablename = ti.tablename
        LEFT JOIN pg_stat_user_tables s ON s.schemaname = ti.schemaname AND s.relname = ti.tablename
        ORDER BY ti.tablename
    """
    
    return copy_table(db=db, query=query)
