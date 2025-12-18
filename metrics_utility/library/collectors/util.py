import os
import pathlib
import tempfile

from ..csv_file_splitter import CsvFileSplitter


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


# FIXME: cleanup
def init_tmp_dir():
    tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix='awx_analytics-'))
    gather_dir = tmp_dir.joinpath('stage')
    gather_dir.mkdir(mode=0o700)
    return gather_dir


def copy_table(db, table, query, params=None, prepend_query=False, output_file=None, output_dir='.'):
    file = output_file
    if not output_file:
        path = output_dir or init_tmp_dir()
        file_path = os.path.join(path, table + '_table.csv')
        file = CsvFileSplitter(filespec=file_path)

    with db.cursor() as cursor:
        if prepend_query:
            cursor.execute(_yaml_json_functions())

        copy_query = f'COPY ({query}) TO STDOUT WITH CSV HEADER'

        # Use psycopg (v3) cursor.copy() method
        with cursor.copy(copy_query, params) as copy:
            while data := copy.read():
                byte_data = bytes(data)
                file.write(byte_data.decode())

    if output_file:
        return [output_file.name]
    return file.file_list(keep_empty=True)


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
