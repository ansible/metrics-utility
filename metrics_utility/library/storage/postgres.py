import datetime
import fnmatch
import json

from contextlib import contextmanager

from psycopg import sql

from .helpers import load_csv, load_json
from .util import dict_to_json_file


def _sql_identifier(name):
    return sql.Identifier(*name.split('.'))


class StoragePostgres:
    """
    StoragePostgres(db=connection, table='daily', [key_field='key', value_field='value', timestamp_field=None])
    stores & reads data as a json value in a key-value-style table
    supports dict, csv files, json files
    """

    def __init__(self, **settings):
        self.db = settings.get('db')
        self.table = settings.get('table')
        self.key_field = settings.get('key_field', 'key')
        self.value_field = settings.get('value_field', 'value')
        self.timestamp_field = settings.get('timestamp_field', None)

        if not self.db:
            raise Exception('StoragePostgres: db connection is required')

        if not self.table:
            raise Exception('StoragePostgres: table is required')

    @contextmanager
    def get(self, key):
        """
        Get value from the database as a temporary JSON file.

        Args:
            key: The key to look up

        Yields:
            Path to a temporary JSON file containing the data

        Raises:
            KeyError: If the key doesn't exist in the database
        """
        data = self.get_data(key)
        if data is None:
            raise KeyError(f'Key not found in storage: {key}')

        # Use the util function to create a temporary JSON file
        with dict_to_json_file(data) as filename:
            yield filename

    def get_data(self, key):
        """
        Get value from the database by key and return it parsed.

        Args:
            key: The key to look up

        Returns:
            The parsed JSON value (dict or list) or None if not found
        """
        with self.db.cursor() as cursor:
            query = sql.SQL('SELECT {value_field} FROM {table} WHERE {key_field} = %s').format(
                value_field=_sql_identifier(self.value_field),
                table=_sql_identifier(self.table),
                key_field=_sql_identifier(self.key_field),
            )
            cursor.execute(query, (key,))

            row = cursor.fetchone()
            if row is None:
                return None

            # The value is stored as JSON/JSONB in PostgreSQL
            value = row[0]

            # If it's already a dict/list (JSONB type), return it
            if isinstance(value, (dict, list)):
                return value

            # Otherwise parse it as JSON string
            return json.loads(value) if value else None

    def put(self, key, *, filename=None, fileobj=None, dict=None, update_timestamp=True):
        """
        Store value in the database.

        Args:
            key: The key to store under
            filename: Path to a CSV or JSON file to load
            fileobj: File-like object containing CSV or JSON data
            dict: Dictionary or list to store directly
            update_timestamp: If True (default), update timestamp to now. If False, don't update timestamp field.

        Note:
            Exactly one of filename, fileobj, or dict must be provided.
            For CSV files, the loaded list of dicts will be stored.
            For JSON files, the loaded data (dict or list) will be stored.
        """
        if sum([filename is not None, fileobj is not None, dict is not None]) != 1:
            raise ValueError('Exactly one of filename, fileobj, or dict must be provided')

        # Determine the value to store
        value = None

        if dict is not None:
            value = dict
        elif filename is not None:
            # Determine file type from extension
            if filename.endswith('.csv'):
                value = load_csv(filename)
            elif filename.endswith('.json'):
                value = load_json(filename)
            else:
                raise ValueError(f'Unsupported file type for filename: {filename}. Only .csv and .json are supported.')
        elif fileobj is not None:
            # Try to detect file type from fileobj name attribute if available
            fileobj_name = getattr(fileobj, 'name', '')
            if fileobj_name.endswith('.csv') or 'csv' in fileobj_name.lower():
                value = load_csv(fileobj)
            elif fileobj_name.endswith('.json') or 'json' in fileobj_name.lower():
                value = load_json(fileobj)
            else:
                # Default to JSON for file-like objects without clear extension
                value = load_json(fileobj)

        # Convert value to JSON string
        json_value = json.dumps(value)

        # Build and execute the upsert query
        with self.db.cursor() as cursor:
            if self.timestamp_field and update_timestamp:
                # Include timestamp in the upsert
                query = sql.SQL(
                    """INSERT INTO {table} ({key_field}, {value_field}, {timestamp_field})
                       VALUES (%s, %s, %s)
                       ON CONFLICT ({key_field})
                       DO UPDATE SET {value_field} = EXCLUDED.{value_field},
                                     {timestamp_field} = EXCLUDED.{timestamp_field}"""
                ).format(
                    table=_sql_identifier(self.table),
                    key_field=_sql_identifier(self.key_field),
                    value_field=_sql_identifier(self.value_field),
                    timestamp_field=_sql_identifier(self.timestamp_field),
                )
                cursor.execute(query, (key, json_value, datetime.datetime.now(datetime.timezone.utc)))
            else:
                # No timestamp or don't update it
                query = sql.SQL(
                    """INSERT INTO {table} ({key_field}, {value_field})
                       VALUES (%s, %s)
                       ON CONFLICT ({key_field})
                       DO UPDATE SET {value_field} = EXCLUDED.{value_field}"""
                ).format(
                    table=_sql_identifier(self.table),
                    key_field=_sql_identifier(self.key_field),
                    value_field=_sql_identifier(self.value_field),
                )
                cursor.execute(query, (key, json_value))

        # Commit the transaction
        self.db.commit()

    def glob(self, pattern, since=None, until=None):
        """
        List keys matching a glob pattern, optionally filtered by timestamp.

        Args:
            pattern: Glob pattern to match against keys (e.g., "data-*")
            since: Optional datetime - include only records with timestamp >= since
            until: Optional datetime - include only records with timestamp < until

        Returns:
            List of matching keys
        """
        with self.db.cursor() as cursor:
            # Build the base query
            if self.timestamp_field and (since or until):
                query = sql.SQL('SELECT {key_field}, {timestamp_field} FROM {table}').format(
                    key_field=_sql_identifier(self.key_field),
                    timestamp_field=_sql_identifier(self.timestamp_field),
                    table=_sql_identifier(self.table),
                )
            else:
                query = sql.SQL('SELECT {key_field} FROM {table}').format(
                    key_field=_sql_identifier(self.key_field),
                    table=_sql_identifier(self.table),
                )
            cursor.execute(query)

            rows = cursor.fetchall()

        # Filter by pattern and timestamp
        matching_keys = []
        for row in rows:
            key = row[0]

            # Check glob pattern
            if not fnmatch.fnmatch(key, pattern):
                continue

            # Check timestamp if applicable
            if self.timestamp_field and (since or until):
                timestamp = row[1]
                if since and timestamp < since:
                    continue
                if until and timestamp >= until:
                    continue

            matching_keys.append(key)

        return matching_keys

    def exists(self, key):
        """
        Check if a key exists in the database.

        Args:
            key: The key to check

        Returns:
            True if the key exists, False otherwise
        """
        with self.db.cursor() as cursor:
            query = sql.SQL('SELECT 1 FROM {table} WHERE {key_field} = %s').format(
                table=_sql_identifier(self.table),
                key_field=_sql_identifier(self.key_field),
            )
            cursor.execute(query, (key,))

            return cursor.fetchone() is not None

    def remove(self, key):
        """
        Remove a key from the database.

        Args:
            key: The key to remove
        """
        with self.db.cursor() as cursor:
            query = sql.SQL('DELETE FROM {table} WHERE {key_field} = %s').format(
                table=_sql_identifier(self.table),
                key_field=_sql_identifier(self.key_field),
            )
            cursor.execute(query, (key,))

        self.db.commit()


def create_storage_table(
    db,
    table='storage',
    key_field='key',
    value_field='value',
    timestamp_field='updated_at',
):
    """
    Create a storage table if it doesn't exist.

    Args:
        db: Database connection
        table: Table name
        key_field: Name of the key field (will be PRIMARY KEY)
        value_field: Name of the value field (will be JSONB)
        timestamp_field: Name of the timestamp field (will auto-update, can be None to skip)

    Example:
        create_storage_table(db, table='my_storage')
        storage = StoragePostgres(db=db, table='my_storage')
    """
    with db.cursor() as cursor:
        # Build the CREATE TABLE statement
        if timestamp_field:
            query = sql.SQL(
                """CREATE TABLE IF NOT EXISTS {table} (
                    {key_field} TEXT PRIMARY KEY,
                    {value_field} JSONB NOT NULL,
                    {timestamp_field} TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )"""
            ).format(
                table=_sql_identifier(table),
                key_field=_sql_identifier(key_field),
                value_field=_sql_identifier(value_field),
                timestamp_field=_sql_identifier(timestamp_field),
            )
        else:
            query = sql.SQL(
                """CREATE TABLE IF NOT EXISTS {table} (
                    {key_field} TEXT PRIMARY KEY,
                    {value_field} JSONB NOT NULL
                )"""
            ).format(
                table=_sql_identifier(table),
                key_field=_sql_identifier(key_field),
                value_field=_sql_identifier(value_field),
            )
        cursor.execute(query)

        # Create an index on the timestamp field if it exists
        if timestamp_field:
            index_name = f'{table}_{timestamp_field}_idx'
            query = sql.SQL('CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({timestamp_field})').format(
                index_name=_sql_identifier(index_name),
                table=_sql_identifier(table),
                timestamp_field=_sql_identifier(timestamp_field),
            )
            cursor.execute(query)

    db.commit()
