"""Collector output helpers, SQL execution utilities, and the ``@collector`` decorator."""

from datetime import datetime


# default in db collectors
# outputs a pandas DataFrame for SQL
class DataframeOutput:
    """Output adapter used by DB-backed library collectors to return a pandas DataFrame."""

    def sql(self, db, query):
        """Execute *query* and return a pandas DataFrame.

        Args:
            db: Django database connection.
            query: SQL query string.

        Returns:
            pandas DataFrame with the query results.
        """
        return _copy_table_pandas(db, query)


# default in dict collectors
# outputs a dict
class DictOutput:
    """Output adapter for collectors that return a plain Python dict."""

    def dict(self, data):
        """Validate and return a dict, or None.

        Args:
            data: Must be a dict or None.

        Returns:
            The dict unchanged, or None.

        Raises:
            Exception: If *data* is neither a dict nor None.
        """
        if data is None:
            return None

        if type(data) is not dict:
            raise Exception('data must be a dict, or None')

        return data


def date_where(field, since, until):
    """Build a SQL WHERE clause fragment that filters *field* to the [since, until) window.

    Args:
        field: Column name (should be a hardcoded literal, not user input).
        since: Optional timezone-aware datetime for the inclusive lower bound.
        until: Optional timezone-aware datetime for the exclusive upper bound.

    Returns:
        SQL fragment string (e.g. ``"( created >= '…' AND created < '…' )"``),
        or ``'true'`` if neither bound is specified.

    Raises:
        TypeError: If *since* or *until* is not None and not a datetime.
        ValueError: If *since* or *until* is timezone-naive.
    """
    for name, value in [('since', since), ('until', until)]:
        if value is not None and not isinstance(value, datetime):
            raise TypeError(f'date_where: {name} must be a datetime, got {type(value).__name__}')
        if value is not None and value.tzinfo is None:
            raise ValueError(f'date_where: {name} must be timezone-aware')

    if since and until:
        return f"( {field} >= '{since.isoformat()}' AND {field} < '{until.isoformat()}' )"

    if since:
        return f"( {field} >= '{since.isoformat()}' )"

    if until:
        return f"( {field} < '{until.isoformat()}' )"

    return 'true'


def collector(func):
    """Decorator that creates a collector class and returns a constructor function."""

    class CollectorClass:
        fn = staticmethod(func)
        key = func.__name__

        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def gather(self, **kwargs):
            return self.fn(**self.kwargs, **kwargs)

    def constructor(**kwargs):
        return CollectorClass(**kwargs)

    return constructor


def ensure_functions(db):
    """Create or replace the custom PostgreSQL helper functions used by collectors.

    Installs ``metrics_utility_parse_yaml_field`` and
    ``metrics_utility_is_valid_json`` into the active database connection.

    Args:
        db: Django database connection.
    """
    # Execute prepend_query if needed (custom PostgreSQL functions)
    with db.cursor() as cursor:
        cursor.execute(_yaml_json_functions())


def _copy_table_pandas(db, query):
    import pandas as pd

    # Execute query and create DataFrame from results
    # Using cursor approach since pd.read_sql doesn't work well with psycopg3
    with db.cursor() as cursor:
        cursor.execute(query)

        # Get column names from cursor description
        columns = [desc[0] for desc in cursor.description]

        # Fetch all rows
        rows = cursor.fetchall()

        # Create DataFrame
        df = pd.DataFrame(rows, columns=columns)

    return df


def _yaml_json_functions():
    return """
        -- Define function for parsing field out of yaml encoded as text
        CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field(
            str text,
            field text
        )
        RETURNS text AS
        $$
        DECLARE
            line_re text;
            field_re text;
        BEGIN
            field_re := ' *[:=] *(.+?) *$';
            line_re := '(?n)^' || field || field_re;
            RETURN trim(both '"' from substring(str from line_re) );
        END;
        $$
        LANGUAGE plpgsql;

        -- Define function to check if field is a valid json
        CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json(p_json text)
            returns boolean
        AS
        $$
        BEGIN
            RETURN (p_json::json is not null);
        EXCEPTION
            WHEN others
            THEN RETURN false;
        END;
        $$
        LANGUAGE plpgsql;
    """
