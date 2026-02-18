# FIXME: psycopg.sql
def date_where(field, since, until):
    if since and until:
        return f'( "{field}" >= \'{since.isoformat()}\' AND "{field}" < \'{until.isoformat()}\' )'

    if since:
        return f'( "{field}" >= \'{since.isoformat()}\' )'

    if until:
        return f'( "{field}" < \'{until.isoformat()}\' )'

    return 'true'


def collector(func):
    """Decorator that creates a collector class and returns a constructor function."""

    class CollectorClass:
        fn = staticmethod(func)
        key = func.__name__

        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def gather(self):
            return self.fn(**self.kwargs)

    def constructor(**kwargs):
        return CollectorClass(**kwargs)

    return constructor


def copy_table(db, query, params=None, prepend_query=False):
    """
    Execute SQL query and return data as pandas DataFrame.

    Args:
        db: psycopg db connection
        query: SQL query to execute
        params: (Optional) query params
        prepend_query: (bool) Use when parse_yaml_field or is_valid_json are required by the query

    Returns:
        pandas DataFrame with query results
    """
    import pandas as pd

    # Execute prepend_query if needed (custom PostgreSQL functions)
    if prepend_query:
        with db.cursor() as cursor:
            cursor.execute(_yaml_json_functions())

    # Execute query and create DataFrame from results
    # Using cursor approach since pd.read_sql doesn't work well with psycopg3
    with db.cursor() as cursor:
        cursor.execute(query, params)

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
