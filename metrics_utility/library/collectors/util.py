import tempfile

from ..csv_file_splitter import CsvFileSplitter


# default in db collectors
# outputs a pandas DataFrame for SQL
class DataframeOutput:
    def sql(self, db, query):
        return _copy_table_pandas(db, query)


# default in dict collectors
# outputs a dict
class DictOutput:
    def dict(self, data):
        if data is None:
            return None

        if type(data) is not dict:
            raise Exception('data must be a dict, or None')

        return data


# passed from cli to collectors
# outputs a list of CSV filenames for SQL, keeps dict intact
# .as_* functions take a whole collector, pass self as output
class CollectionOutput(DictOutput):
    def __init__(self, full_path):
        self.full_path = full_path

    # takes a list of filenames, returns the same
    def files(self, filenames):
        if filenames is None:
            return None

        if type(filenames) is not list:
            raise Exception('filenames must be a list, or None')

        return filenames

    # takes a collector, returns a dict
    def as_dict(self, collector):
        return self.dict(collector.gather(self))

    # takes a collector, returns a list of filenames
    def as_files(self, collector):
        return self.files(collector.gather(self))

    def sql(self, db, query):
        filespec = tempfile.mktemp(dir=self.full_path)  # NOT mkstemp - this is a prefix, can't have it get created
        return _copy_table_files(db, query, filespec)


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

        def gather(self, **kwargs):
            return self.fn(**self.kwargs, **kwargs)

    def constructor(**kwargs):
        return CollectorClass(**kwargs)

    return constructor


def ensure_functions(db):
    # Execute prepend_query if needed (custom PostgreSQL functions)
    with db.cursor() as cursor:
        cursor.execute(_yaml_json_functions())


def _copy_table_files(db, query, filespec):
    file = CsvFileSplitter(filespec=filespec)

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
