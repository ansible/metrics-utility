import json
import uuid

from datetime import UTC, datetime

import pandas as pd
import pytest

from django.db import connection
from django.db.utils import OperationalError

from metrics_utility.library.collectors.controller.events_table import events_table
from metrics_utility.library.collectors.controller.main_jobevent_service import main_jobevent_service


SINCE = datetime(2025, 6, 12, tzinfo=UTC)
UNTIL = datetime(2025, 6, 14, tzinfo=UTC)
JOB_ID = 1
JOB_CREATED = datetime(2025, 6, 13, 10, tzinfo=UTC)


@pytest.fixture
def collection_field_events_db():
    """Add collection-field cases to the compose PostgreSQL fixture."""
    try:
        connection.ensure_connection()
    except OperationalError as error:
        pytest.skip(f'compose PostgreSQL fixture unavailable: {error}')

    event_ids = {
        label: f'collection-fields-{label}-{uuid.uuid4()}'
        for label in (
            'valid',
            'missing_version',
            'unknown_collection',
            'one_part',
            'two_part',
            'malformed',
            'absent',
        )
    }
    cases = {
        'valid': {
            'task_action': 'a10.acos_axapi.a10_slb_virtual_server',
            'resolved_action': 'a10.acos_axapi.a10_slb_virtual_server',
        },
        'missing_version': {
            'task_action': 'versionless.collection.plugin',
            'resolved_action': 'versionless.collection.plugin',
        },
        'unknown_collection': {
            'task_action': 'community.general.foo',
            'resolved_action': 'community.general.foo',
        },
        'one_part': {'task_action': 'copy', 'resolved_action': 'copy'},
        'two_part': {'task_action': 'a10.acos_axapi', 'resolved_action': 'a10.acos_axapi'},
        'malformed': {
            'task_action': 'a10.acos_axapi.bad-plugin',
            'resolved_action': 'a10.acos_axapi.bad-plugin',
        },
        'absent': {'task_action': 'ansible.builtin.yum'},
    }

    with connection.cursor() as cursor:
        cursor.execute('SELECT installed_collections FROM main_unifiedjob WHERE id = %s', [JOB_ID])
        original = cursor.fetchone()
        if original is None:
            pytest.skip('compose PostgreSQL fixture has no main_unifiedjob id=1')
        original_installed_collections = original[0]
        updated_installed_collections = {
            'a10.acos_axapi': {'version': '1.0.0'},
            'ansible.builtin': {'version': '2.9.10'},
            'versionless.collection': {},
        }
        cursor.execute(
            'UPDATE main_unifiedjob SET installed_collections = %s::jsonb WHERE id = %s',
            [json.dumps(updated_installed_collections), JOB_ID],
        )

        for counter, (label, action_data) in enumerate(cases.items(), start=9000):
            event_data = json.dumps({'task_uuid': event_ids[label], **action_data})
            cursor.execute(
                """
                INSERT INTO main_jobevent (
                    created, modified, event, event_data, failed, changed,
                    host_name, play, role, task, counter, host_id, job_id,
                    uuid, parent_uuid, end_line, playbook, start_line,
                    stdout, verbosity, job_created
                ) VALUES (
                    %s, %s, 'runner_on_ok', %s, false, false,
                    'default_host_1_2025-06-13', 'default_play', 'default_role',
                    'collection field test', %s, 31, %s, %s, '', %s,
                    'default_playbook.yml', %s, '', 0, %s
                )
                """,
                [JOB_CREATED, JOB_CREATED, event_data, counter, JOB_ID, event_ids[label], counter, counter, JOB_CREATED],
            )

    try:
        yield event_ids
    finally:
        with connection.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(event_ids))
            cursor.execute(f'DELETE FROM main_jobevent WHERE uuid IN ({placeholders})', list(event_ids.values()))
            original_json = original_installed_collections
            if not isinstance(original_json, str):
                original_json = json.dumps(original_json)
            cursor.execute(
                'UPDATE main_unifiedjob SET installed_collections = %s::jsonb WHERE id = %s',
                [original_json, JOB_ID],
            )


def _assert_nullable_equal(actual, expected):
    if expected is None:
        assert pd.isna(actual)
    else:
        assert actual == expected


@pytest.mark.parametrize('collector', [main_jobevent_service, events_table], ids=['main_jobevent_service', 'events_table'])
def test_collectors_return_collection_fields_from_database(collector, collection_field_events_db):
    """Both event collectors resolve collection fields from live JSONB rows."""
    dataframe = collector(db=connection, since=SINCE, until=UNTIL).gather()
    rows = dataframe.set_index('uuid')

    expected = {
        'valid': ('a10.acos_axapi', '1.0.0'),
        'missing_version': ('versionless.collection', None),
        'unknown_collection': ('community.general', None),
        'one_part': (None, None),
        'two_part': (None, None),
        'malformed': (None, None),
        'absent': (None, None),
    }
    for label, (expected_name, expected_version) in expected.items():
        row = rows.loc[collection_field_events_db[label]]
        _assert_nullable_equal(row['collection_name'], expected_name)
        _assert_nullable_equal(row['collection_version'], expected_version)
