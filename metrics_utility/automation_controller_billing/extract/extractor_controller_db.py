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

                # Advance the keyset marker to the last row of this batch. Values are
                # carried as query parameters (never interpolated) on the next iteration.
                last_row = host_metric[-1]
                marker = (last_row['hostname'], last_row['host_id'])

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
        """Build the host_metric SQL query and bind parameters, with optional keyset pagination.

        All caller-influenced values (``since``, the pagination marker, and the row
        limit) are passed as query parameters rather than interpolated into the SQL
        text, so the returned query is safe to hand to ``cursor.execute(query, params)``.

        Args:
            since: Inclusive lower bound timestamp (timezone-aware datetime).
            marker: Optional ``(hostname, host_id)`` tuple from the last row of the
                previous batch. When provided, only rows ordered strictly after the
                marker are returned (keyset pagination). ``None`` returns from the start.

        Returns:
            Tuple ``(query, params)`` suitable for ``cursor.execute``.
        """
        params = [since]

        marker_cond = ''
        if marker is not None:
            # Multi-column keyset comparison matching the ORDER BY below. Expanded
            # explicitly (rather than a row-value comparison) because the second
            # sort key is COALESCE(main_host.id, 0), not a bare column.
            marker_cond = """
                AND (main_hostmetric.hostname > %s
                     OR (main_hostmetric.hostname = %s
                         AND COALESCE(main_host.id, 0) > %s))
            """
            last_hostname, last_host_id = marker
            params.extend([last_hostname, last_hostname, last_host_id])

        # marker_cond contains only static SQL with %s placeholders (no interpolated
        # values); the surrounding f-string only injects that fixed fragment.
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
            WHERE (main_hostmetric.last_automation >= %s {marker_cond})
            ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
            LIMIT %s
        """
        params.append(self.limit())

        return query, params

    @staticmethod
    def limit():
        """Return the maximum number of rows fetched per page.

        Returns:
            int row-count limit.
        """
        return 10000
