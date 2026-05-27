"""Extractor that reads host_metric data directly from the Controller database."""

import datetime

import pandas as pd

from django.db import connection


class ExtractorControllerDB:
    """Extracts host_metric data from the AWX/Controller PostgreSQL database.

    Uses marker-based keyset pagination to stream large result sets in batches
    of :attr:`limit` rows.
    """

    def __init__(self, extra_params):
        """Initialise the DB extractor.

        Args:
            extra_params: Dict containing at least ``'opt_since'`` (datetime).
        """
        super().__init__()

        self.extra_params = extra_params

    def iter_batches(self):
        """Yield host_metric batches from the Controller database.

        Uses keyset pagination on ``hostname + host_id`` to iterate large result
        sets without repeated full scans.

        Yields:
            Dict ``{'host_metric': pandas.DataFrame}`` for each non-empty batch.
        """
        with connection.cursor() as cursor:
            cursor.execute(self.pg_functions())

            since = self.extra_params['opt_since']
            if since.tzinfo is None:
                since = since.replace(tzinfo=datetime.UTC)

            marker = None
            while True:
                query, params = self.host_metric_query(since, marker)
                cursor.execute(query, params)
                host_metric = self.dict_fetchall(cursor)

                # Marker based pagination
                if len(host_metric) <= 0:
                    break

                last = list(host_metric)[-1]
                marker = f"{last['hostname']}___{last['host_id']}"

                host_metric = pd.DataFrame(host_metric)

                yield {'host_metric': host_metric}

    def dict_fetchall(self, cursor):
        """
        Return all rows from a cursor as a dict.
        Assume the column names are unique.
        """
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def pg_functions(self):
        """Return SQL that creates or replaces the custom PostgreSQL helper functions.

        Returns:
            SQL string defining ``metrics_utility_parse_yaml_field`` and
            ``metrics_utility_is_valid_json``.
        """
        query = """
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
                WHEN others THEN
                    RETURN false;
            END;
            $$
            LANGUAGE plpgsql;
        """
        return query

    def host_metric_query(self, since, marker=None):
        """Build the host_metric SQL query with an optional keyset-pagination marker.

        Uses parameterized queries to prevent SQL injection.

        Args:
            since: Inclusive lower bound timestamp (timezone-aware datetime).
            marker: Optional opaque cursor string (``'hostname___host_id'``) from the
                last row of the previous page.  When provided, results are restricted
                to rows whose concatenated ``hostname___host_id`` key sorts strictly
                after this value.  Defaults to ``None``, which returns the first page.

        Returns:
            Tuple of ``(query_string, params_tuple)`` suitable for passing directly
            to ``cursor.execute()``.
        """
        if marker is not None:
            marker_sql = 'AND CONCAT(main_hostmetric.hostname , \'___\', COALESCE(main_host.id, 0)) > %s'
            params = (since, marker)
        else:
            marker_sql = ''
            params = (since,)

        query = f"""
            SELECT main_hostmetric.hostname,
                   COALESCE(main_host.id, 0) AS host_id,
                   main_hostmetric.first_automation,
                   main_hostmetric.last_automation,
                   main_hostmetric.automated_counter,
                   main_hostmetric.deleted_counter,
                   main_hostmetric.last_deleted,
                   main_hostmetric.deleted,
                   main_host.ansible_facts->>'ansible_product_serial'::TEXT AS ansible_product_serial,
                   main_host.ansible_facts->>'ansible_machine_id'::TEXT AS ansible_machine_id,
                   CASE
                       WHEN (metrics_utility_is_valid_json(main_host.variables))
                           THEN main_host.variables::jsonb->>'ansible_host'
                       ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                   END AS ansible_host_variable,
                   CASE
                       WHEN (metrics_utility_is_valid_json(main_host.variables))
                           THEN main_host.variables::jsonb->>'ansible_connection'
                       ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                   END AS ansible_connection_variable

            FROM main_hostmetric
            LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname
            WHERE (main_hostmetric.last_automation >= %s {marker_sql})
            ORDER BY CONCAT(main_hostmetric.hostname , '___', COALESCE(main_host.id, 0)) ASC
            -- ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
            LIMIT {self.limit()}
        """

        return query, params

    @staticmethod
    def limit():
        """Return the maximum number of rows fetched per page.

        Returns:
            int row-count limit.
        """
        return 10000
