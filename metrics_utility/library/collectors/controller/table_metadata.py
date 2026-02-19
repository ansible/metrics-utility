from ..util import collector, copy_table


@collector
def table_metadata(*, db=None):
    """
    Collect row count and size information for main_jobevent (partitioned), main_unifiedjob (regular), and main_jobhostsummary (regular) tables.
    """
    query = """
        -- Partitioned table: sum from all partitions
        SELECT
            'public'::text AS schemaname,
            'main_jobevent'::text AS tablename,
            COALESCE(SUM(p.reltuples)::BIGINT, 0) AS estimated_row_count,
            COALESCE(SUM(pg_total_relation_size(p.oid)), 0)::BIGINT AS total_size_bytes,
            COALESCE(SUM(pg_relation_size(p.oid)), 0)::BIGINT AS table_size_bytes,
            COALESCE(SUM(pg_indexes_size(p.oid)), 0)::BIGINT AS indexes_size_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        LEFT JOIN pg_inherits inh ON inh.inhparent = c.oid
        LEFT JOIN pg_class p ON p.oid = inh.inhrelid AND p.relkind = 'r'
        WHERE c.relname = 'main_jobevent'
          AND c.relkind = 'p'
        GROUP BY c.oid, c.relname
        
        UNION ALL
        
        -- Regular table: direct reltuples and sizes
        SELECT
            'public'::text AS schemaname,
            'main_unifiedjob'::text AS tablename,
            COALESCE(c.reltuples::BIGINT, 0) AS estimated_row_count,
            pg_total_relation_size(c.oid)::BIGINT AS total_size_bytes,
            pg_relation_size(c.oid)::BIGINT AS table_size_bytes,
            pg_indexes_size(c.oid)::BIGINT AS indexes_size_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        WHERE c.relname = 'main_unifiedjob'
          AND c.relkind = 'r'
        
        UNION ALL
        
        -- Regular table: direct reltuples and sizes
        SELECT
            'public'::text AS schemaname,
            'main_jobhostsummary'::text AS tablename,
            COALESCE(c.reltuples::BIGINT, 0) AS estimated_row_count,
            pg_total_relation_size(c.oid)::BIGINT AS total_size_bytes,
            pg_relation_size(c.oid)::BIGINT AS table_size_bytes,
            pg_indexes_size(c.oid)::BIGINT AS indexes_size_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        WHERE c.relname = 'main_jobhostsummary'
          AND c.relkind = 'r'
    """
    
    return copy_table(db=db, query=query)
