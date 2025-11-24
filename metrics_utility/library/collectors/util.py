import os
import pathlib
import tempfile

from ..csv_file_splitter import CsvFileSplitter


def date_where(column, since, until):
    """Generate a SQL WHERE clause for date range filtering.

    Args:
        column: The column name to filter on
        since: Start datetime (inclusive), or None for no lower bound
        until: End datetime (exclusive), or None for no upper bound

    Returns:
        A SQL WHERE clause string
    """
    conditions = []

    if since is not None:
        conditions.append(f"{column} >= '{since.isoformat()}'")

    if until is not None:
        conditions.append(f"{column} < '{until.isoformat()}'")

    if not conditions:
        return 'TRUE'

    return ' AND '.join(conditions)


def collector(func):
    """Decorator that creates a collector class and returns a constructor
    function."""

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


# FIXME: cleanup
def init_tmp_dir():
    tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix='awx_analytics-'))
    gather_dir = tmp_dir.joinpath('stage')
    gather_dir.mkdir(mode=0o700)
    return gather_dir


def copy_table(
    db,
    table,
    query,
    params=None,
    prepend_query=False,
    output_file=None,
    output_dir=None,
    format='csv',
):
    """Copy table data to file in specified format (csv or json)."""
    if format.lower() == 'json':
        return copy_table_to_json(db, table, query, params, prepend_query, output_dir)

    # Original CSV implementation
    file = output_file
    if not output_file:
        path = output_dir or init_tmp_dir()
        file_path = os.path.join(path, table + '_table.csv')
        file = CsvFileSplitter(filespec=file_path)

    with db.cursor() as cursor:
        if prepend_query:
            cursor.execute(_yaml_json_functions())

        copy_query = f'COPY ({query}) TO STDOUT WITH CSV HEADER'

        # FIXME: remove once 2.4 is no longer supported
        if hasattr(cursor, 'copy_expert') and callable(cursor.copy_expert):
            _copy_table_aap_2_4_and_below(cursor, copy_query, params, file)
        else:
            _copy_table_aap_2_5_and_above(cursor, copy_query, params, file)

    if output_file:
        return [output_file.name]
    return file.file_list(keep_empty=True)


def copy_table_to_json(db, table, query, params=None, prepend_query=False, output_dir=None):
    """Copy table data directly to JSON format.

    Always returns a list of file paths for consistency with CSV
    implementation.
    """
    import json

    with db.cursor() as cursor:
        if prepend_query:
            cursor.execute(_yaml_json_functions())

        # Execute the query and fetch results
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Get column names from cursor description
        columns = [desc[0] for desc in cursor.description]

        # Fetch all rows and convert to list of dictionaries
        rows = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            # Convert any non-JSON serializable types to strings
            for key, value in row_dict.items():
                if hasattr(value, 'isoformat'):  # datetime objects
                    row_dict[key] = value.isoformat()
                elif hasattr(value, '__str__') and not isinstance(value, (str, int, float, bool, type(None))):
                    row_dict[key] = str(value)
            rows.append(row_dict)

        # Create structured JSON response
        json_data = {
            table: {
                'data': rows,
                'count': len(rows),
                'format': 'json',
                'table_name': table,
            }
        }

        # Save to file, using temp dir if output_dir not specified
        path = output_dir or init_tmp_dir()
        json_file_path = os.path.join(path, table + '_table.json')
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2)

        return [json_file_path]


def _copy_table_aap_2_4_and_below(cursor, query, params, file):
    if params:
        # copy_expert doesn't support params, make do (but no escaping)
        for p in params:
            if f'%({p})s' in query:
                query = query.replace(f'%({p})s', f'"{params[p]}"')
            if f'%({p})d' in query:
                query = query.replace(f'%({p})d', str(int(params[p])))

    # Automation Controller 4.4 and below use psycopg2 with
    # .copy_expert() method
    cursor.copy_expert(query, file)


def _copy_table_aap_2_5_and_above(cursor, query, params, file):
    # Automation Controller 4.5 and above use psycopg3 with
    # .copy() method
    with cursor.copy(query, params) as copy:
        while data := copy.read():
            byte_data = bytes(data)
            file.write(byte_data.decode())


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
