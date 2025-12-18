import json


_configured_connection = None


def configure_db(db_json=None):
    """Configure the database connection. Call this early in command handle()."""
    global _configured_connection

    if db_json is None:
        # Use default Django connection
        from django.db import connection as django_connection

        _configured_connection = django_connection
        return

    # Parse JSON with defaults
    config = json.loads(db_json)
    config.setdefault('ENGINE', 'django.db.backends.postgresql')
    config.setdefault('HOST', 'localhost')
    config.setdefault('PORT', '5432')

    # Create custom connection
    from django.db.utils import ConnectionHandler

    _configured_connection = ConnectionHandler({'default': config})['default']


def get_connection():
    """Get the configured connection, or default Django connection if not configured."""
    if _configured_connection is None:
        from django.db import connection as django_connection

        return django_connection
    return _configured_connection
