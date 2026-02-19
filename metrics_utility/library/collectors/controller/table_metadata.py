from ..util import collector, copy_table


@collector
def table_metadata(*, db=None):
    """
    Collect row count for main_jobevent table, summing across all partitions.
    """
    query = """
        SELECT
            'public'::text AS schemaname,
            'main_jobevent'::text AS tablename,
            COALESCE(SUM(p.reltuples)::BIGINT, 0) AS estimated_row_count
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        LEFT JOIN pg_inherits inh ON inh.inhparent = c.oid
        LEFT JOIN pg_class p ON p.oid = inh.inhrelid AND p.relkind = 'r'
        WHERE c.relname = 'main_jobevent'
          AND c.relkind = 'p'
        GROUP BY c.oid, c.relname
    """
    
    return copy_table(db=db, query=query)
