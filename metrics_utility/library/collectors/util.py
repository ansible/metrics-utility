import os
import tempfile

from datetime import datetime

from metrics_utility.logger import logger

from ..csv_file_splitter import CsvFileSplitter


def get_batch_size():
    """
    Get the row-count batch size for collectors that support ID-range batching.
    Defaults to 0 (disabled). Set to e.g. 100000 to limit each COPY query to
    approximately that many rows using keyset pagination on the primary key.
    Returns 0 (disabled) on any invalid or negative value.
    """
    try:
        value = int(os.getenv('METRICS_UTILITY_GATHER_BATCH_SIZE', '0'))
    except (ValueError, TypeError):
        logger.error('METRICS_UTILITY_GATHER_BATCH_SIZE cannot be converted to an integer, disabling batching')
        return 0
    if value < 0:
        logger.error('METRICS_UTILITY_GATHER_BATCH_SIZE must be >= 0, got %d, disabling batching', value)
        return 0
    return value


# default in db collectors
# outputs a pandas DataFrame for SQL
class DataframeOutput:
    def sql(self, db, query):
        return _copy_table_pandas(db, query)

    def batch_sql(self, db, query_fn, min_max_query, batch_size):
        """ID-range batching for library usage. Returns a single concatenated DataFrame."""
        import pandas as pd

        with db.cursor() as cursor:
            cursor.execute(min_max_query)
            row = cursor.fetchone()

        if not row or row[0] is None:
            return pd.DataFrame()

        min_id, max_id = int(row[0]), int(row[1])
        dfs = []
        batch_start = min_id

        while batch_start <= max_id:
            batch_end = batch_start + batch_size
            dfs.append(_copy_table_pandas(db, query_fn(batch_start, batch_end)))
            batch_start = batch_end

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


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
        return self.dict(collector.gather(output=self))

    # takes a collector, returns a list of filenames
    def as_files(self, collector):
        return self.files(collector.gather(output=self))

    def _make_filespec(self):
        """Return a secure filespec prefix for CsvFileSplitter inside a fresh subdirectory.

        Uses mkdtemp (atomic, race-free) so split files become
        <tmpdir>/data_split0, <tmpdir>/data_split1, …
        """
        tmpdir = tempfile.mkdtemp(dir=self.full_path)
        return os.path.join(tmpdir, 'data')

    def sql(self, db, query):
        return _copy_table_files(db, query, self._make_filespec())

    def batch_sql(self, db, query_fn, min_max_query, batch_size):
        """Run query_fn in row-count batches using ID-range keyset pagination.

        query_fn(batch_start, batch_end) must return the full SQL query with the
        ID-range filter already embedded in its WHERE clause (and any CTE that
        scans the same table), so each batch pays only for its own rows.
        min_max_query must return (MIN(id), MAX(id)) for the relevant time window.
        """
        return _batch_copy_table_files(db, query_fn, min_max_query, batch_size, self._make_filespec())


# NOTE: `field` should be a hardcoded column name
def date_where(field, since, until):
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


def _batch_copy_table_files(db, query_fn, min_max_query, batch_size, filespec):
    """Execute query_fn in ID-range batches, writing all output to one CsvFileSplitter.

    The first batch uses COPY … WITH CSV HEADER so the splitter captures the
    header row; subsequent batches use COPY … WITH CSV (no header) and their
    rows are appended directly. If the splitter cycles to a new file it replays
    the stored header automatically.
    """
    if batch_size <= 0:
        raise ValueError(f'batch_size must be > 0, got {batch_size}')

    file = CsvFileSplitter(filespec=filespec)

    with db.cursor() as cursor:
        cursor.execute(min_max_query)
        row = cursor.fetchone()

    if not row or row[0] is None:
        return file.file_list(keep_empty=True)

    min_id, max_id = int(row[0]), int(row[1])
    first_batch = True
    batch_start = min_id

    while batch_start <= max_id:
        batch_end = batch_start + batch_size
        query = query_fn(batch_start, batch_end)
        copy_suffix = 'WITH CSV HEADER' if first_batch else 'WITH CSV'

        with db.cursor() as cursor:
            with cursor.copy(f'COPY ({query}) TO STDOUT {copy_suffix}') as copy:
                while data := copy.read():
                    file.write(bytes(data).decode())

        first_batch = False
        batch_start = batch_end

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
