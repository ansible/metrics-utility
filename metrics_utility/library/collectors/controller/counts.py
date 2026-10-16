"""Collector for object counts across the Controller database."""

from ..util import DictOutput, collector


_MODEL_TABLES = [
    ('organization', 'main_organization'),
    ('team', 'main_team'),
    ('user', 'auth_user'),
    ('inventory', 'main_inventory'),
    ('credential', 'main_credential'),
    ('project', 'main_project'),
    ('job_template', 'main_jobtemplate'),
    ('workflow_job_template', 'main_workflowjobtemplate'),
    ('host', 'main_host'),
    ('schedule', 'main_schedule'),
    ('notification_template', 'main_notificationtemplate'),
]


@collector
def counts(*, db=None, output=DictOutput()):
    """Return object counts mirroring AWX's ``counts`` collector."""
    counts = {}

    with db.cursor() as cursor:
        # Simple model counts
        for key, table in _MODEL_TABLES:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            counts[key] = cursor.fetchone()[0]

        # Inventory breakdown by kind
        cursor.execute("""
            SELECT COALESCE(NULLIF(kind, ''), 'normal') AS kind, COUNT(*)
            FROM main_inventory
            GROUP BY kind
        """)
        inv_counts = dict(cursor.fetchall())
        inv_counts.setdefault('normal', 0)
        inv_counts.setdefault('smart', 0)
        counts['inventories'] = inv_counts

        # Unified jobs (exclude implicit project_updates)
        cursor.execute("""
            SELECT COUNT(*) FROM main_unifiedjob
            WHERE launch_type != 'sync'
        """)
        counts['unified_job'] = cursor.fetchone()[0]

        # Active host count — replicates Django ORM:
        #   .exclude(inventory_sources__source='controller')
        #   .exclude(inventory__kind='constructed')
        #   .values(name_lower=Lower('name')).distinct().count()
        cursor.execute("""
            SELECT COUNT(DISTINCT LOWER(main_host.name))
            FROM main_host
            JOIN main_inventory ON main_inventory.id = main_host.inventory_id
            WHERE main_inventory.kind != 'constructed'
                AND NOT EXISTS (
                    SELECT 1
                    FROM main_inventorysource
                    JOIN main_host_inventory_sources
                        ON main_host_inventory_sources.inventorysource_id
                            = main_inventorysource.unifiedjobtemplate_ptr_id
                    WHERE main_host_inventory_sources.host_id = main_host.id
                        AND main_inventorysource.source = 'controller'
                )
        """)
        counts['active_host_count'] = cursor.fetchone()[0]

        # Active sessions
        cursor.execute("""
            SELECT COUNT(*) FROM django_session
            WHERE expire_date >= NOW()
        """)
        active_sessions = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*)
            FROM main_usersessionmembership
            JOIN django_session ON django_session.session_key = main_usersessionmembership.session_id
            WHERE django_session.expire_date >= NOW()
        """)
        active_user_sessions = cursor.fetchone()[0]

        counts['active_sessions'] = active_sessions
        counts['active_user_sessions'] = active_user_sessions
        counts['active_anonymous_sessions'] = active_sessions - active_user_sessions

        # Running and pending jobs
        cursor.execute("""
            SELECT COUNT(*) FROM main_unifiedjob
            WHERE launch_type != 'sync'
                AND status IN ('running', 'waiting')
        """)
        counts['running_jobs'] = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM main_unifiedjob
            WHERE launch_type != 'sync'
                AND status = 'pending'
        """)
        counts['pending_jobs'] = cursor.fetchone()[0]

        # Database connections
        cursor.execute("""
            SELECT COUNT(*) FROM pg_stat_activity
            WHERE datname = current_database()
        """)
        counts['database_connections'] = cursor.fetchone()[0]

    return output.dict(counts)
