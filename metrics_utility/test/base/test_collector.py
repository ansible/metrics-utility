from datetime import timedelta

import pytest

from metrics_utility.base import CollectionCSV, Collector, register


@pytest.mark.parametrize(
    ('start_days_ago', 'end_days_ago', 'expected_end_days_ago', 'truncated'),
    [
        (8, None, 5, False),  # A historical open interval ends at the configured limit.
        (2, None, 0, False),  # A recent open interval ends at now.
        (8, 4, 5, True),
        (8, 5, 5, False),  # Exactly at the limit: no truncation warning.
        (8, 7, 7, False),
        (None, None, 0, False),
        (None, 2, 2, False),
    ],
)
def test_collection_interval(fixed_now, monkeypatch, mocker, caplog, start_days_ago, end_days_ago, expected_end_days_ago, truncated):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '3')
    mocker.patch('metrics_utility.base.collector.now', return_value=fixed_now)
    since = fixed_now - timedelta(days=start_days_ago) if start_days_ago is not None else None
    until = fixed_now - timedelta(days=end_days_ago) if end_days_ago is not None else None
    expected_end = fixed_now - timedelta(days=expected_end_days_ago)
    collector = Collector()

    collector._calculate_collection_interval(since, until)

    assert collector.gather_since == since
    assert collector.gather_until == expected_end
    assert collector.last_gather == expected_end - timedelta(days=3)
    assert (f'End of the collection interval is greater than 3 days from start, setting end to {expected_end}.' in caplog.messages) is truncated


@pytest.mark.parametrize(
    ('disable_summary', 'optional', 'expected'),
    [
        ('false', '', 'Now gathering job_host_summary'),
        ('true', '', 'Skipping job_host_summary because it is not enabled.'),
        ('true', 'job_host_summary', 'Now gathering job_host_summary'),
    ],
)
def test_csv_progress_logged_once_per_key(monkeypatch, mocker, caplog, disable_summary, optional, expected):
    monkeypatch.setenv('METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR', disable_summary)
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', optional)
    slices = [mocker.Mock(key='job_host_summary', **{'is_empty.return_value': True}) for _ in range(2)]
    collector = Collector()
    collector.collections['csv'] = slices

    collector._gather_csv_collections()

    assert caplog.messages == [f'Progress info: {expected}']
    for part in slices:
        part.gather.assert_called_once_with()


def test_csv_gather_packages_each_output_file(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent,main_host')
    collector = Collector()
    collector.gather_dir = tmp_path
    collector.last_gathered_entries = {}

    @register('main_jobevent', '1', format='csv')
    def events(**kwargs):
        return [tmp_path / 'events-1.csv', tmp_path / 'events-2.csv']

    @register('main_host', '1', format='csv')
    def hosts(**kwargs):
        return [tmp_path / 'hosts.csv']

    @register('unused', '1', format='csv')
    def unused(**kwargs):
        return []

    for name in ('events-1.csv', 'events-2.csv', 'hosts.csv'):
        (tmp_path / name).write_text('id\n1\n')
    collector.collections['csv'] = [CollectionCSV(collector, fn) for fn in (events, hosts, unused)]

    collector._gather_csv_collections()

    assert [[part.target().name for part in package.collections] for package in collector.packages['default']] == [
        ['events-1.csv', 'hosts.csv'],
        ['events-2.csv'],
    ]
    assert caplog.messages == [
        'Progress info: Now gathering main_jobevent',
        'Progress info: Now gathering main_host',
        'Progress info: Skipping unused because it is not enabled.',
    ]
