"""Collector output helpers, SQL execution utilities, and the ``@collector`` decorator."""

import tempfile

from datetime import datetime

from ..csv_file_splitter import CsvFileSplitter


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


# passed from cli to collectors
# outputs a list of CSV filenames for SQL, keeps dict intact
# .as_* functions take a whole collector, pass self as output
class CollectionOutput(DictOutput):
    """Output adapter passed from the CLI to collectors.

    CSV collectors write files to ``full_path`` and return a list of file paths.
    JSON collectors return a dict (handled by the :class:`DictOutput` base class).
    """

    def __init__(self, full_path):
        """Initialise with the directory where CSV output files will be written.

        Args:
            full_path: Absolute path to the staging directory.
        """
        self.full_path = full_path

    # takes a list of filenames, returns the same
    def files(self, filenames):
        """Validate and return a list of CSV file paths.

        Args:
            filenames: Must be a list of path strings or None.

        Returns:
            The list unchanged, or None.

        Raises:
            Exception: If *filenames* is neither a list nor None.
        """
        if filenames is None:
            return None

        if type(filenames) is not list:
            raise Exception('filenames must be a list, or None')

        return filenames

    # takes a collector, returns a dict
    def as_dict(self, collector):
        """Gather from *collector* and return the result as a dict.

        Args:
            collector: A collector object with a ``gather(output=…)`` method.

        Returns:
            The gathered dict, or None.
        """
        return self.dict(collector.gather(output=self))

    # takes a collector, returns a list of filenames
    def as_files(self, collector):
        """Gather from *collector* and return the result as a list of file paths.

        Args:
            collector: A collector object with a ``gather(output=…)`` method.

        Returns:
            List of CSV file paths, or None.
        """
        return self.files(collector.gather(output=self))

    def sql(self, db, query):
        filespec = tempfile.mktemp(dir=self.full_path)  # NOT mkstemp - this is a prefix, can't have it get created
        return _copy_table_files(db, query, filespec)


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


def _copy_table_files(db, query, filespec):
    with CsvFileSplitter(filespec=filespec) as file:
        with db.cursor() as cursor:
            copy_query = f'COPY ({query}) TO STDOUT WITH CSV HEADER'

            with cursor.copy(copy_query) as copy:
                while data := copy.read():
                    byte_data = bytes(data)
                    file.write(byte_data.decode())

        return file.file_list(keep_empty=True)


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
