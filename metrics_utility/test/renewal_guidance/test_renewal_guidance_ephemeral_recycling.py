"""Regression tests for renewal guidance ephemeral classification (AAP-88193).

These cover four defects in how ephemeral hosts are counted after
deduplication:

* Defect 1 - recycled immutable-infra fleets collapse to a single HostMetric
  record whose first/last automation span the whole year, so they were
  mis-classified as long-lived *standard* (billed) hosts instead of ephemeral.
* Defect 4 - soft-deleted ephemeral hosts were absent from the "Ephemeral
  automated hosts total" (which excluded deleted rows) even though they were
  counted in the high-water mark, so the two figures disagreed.
* Defects 2 & 3 - distinct hosts sharing a single machine_id (shared execution
  environment / connection:local) are falsely collapsed by deduplication; the
  report now flags such merge-suspect records so the under-count is visible.
"""

import datetime as dt_actual

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from openpyxl import Workbook

from metrics_utility.automation_controller_billing.report.report_renewal_guidance import ReportRenewalGuidance


EPHEMERAL_DAYS = 30

# Base column set produced downstream of deduplication and consumed by the
# classification / query methods under test.
_COLUMNS = [
    'hostname',
    'first_automation',
    'last_automation',
    'days_automated',
    'deleted',
    'deleted_counter',
    'hostnames',
    'ansible_product_serials',
    'ansible_machine_ids',
]


@pytest.fixture
def report_instance(fixed_now):
    """A ReportRenewalGuidance with datetime.now() pinned to fixed_now.

    Mirrors the patching used by the existing query-method tests so the
    ephemeral threshold (now - (ephemeral_days - 1)) is deterministic:
    for fixed_now = 2025-06-03 the threshold date is 2025-05-05.
    """
    patch_target = 'metrics_utility.automation_controller_billing.report.report_renewal_guidance.datetime'
    with patch(patch_target) as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = fixed_now
        mock_datetime_module.timedelta = dt_actual.timedelta
        mock_datetime_module.timezone = MagicMock(spec=dt_actual.timezone)
        mock_datetime_module.timezone.utc = dt_actual.UTC

        extra_params = {
            'ephemeral_days': EPHEMERAL_DAYS,
            'price_per_node': 0.1,
            'report_period': '2025-01-01,2025-06-03',
            'since_date': '2025-01-01',
            'until_date': '2025-06-03',
        }
        yield ReportRenewalGuidance(dataframes={'host_metric': _empty_frame()}, extra_params=extra_params)


def _empty_frame():
    return pd.DataFrame(columns=_COLUMNS)


def _row(hostname, first, last, deleted=False, deleted_counter=0, hostnames=None, serials='', mids=''):
    """Build one deduped-style record. `first`/`last` are naive datetimes."""
    days = max(0, (last - first).days)
    return {
        'hostname': hostname,
        'first_automation': first,
        'last_automation': last,
        'days_automated': days,
        'deleted': deleted,
        'deleted_counter': deleted_counter,
        'hostnames': hostnames if hostnames is not None else hostname,
        'ansible_product_serials': serials,
        'ansible_machine_ids': mids,
    }


def _frame(rows):
    df = pd.DataFrame(rows, columns=_COLUMNS)
    return df.astype(
        {
            'deleted': bool,
            'deleted_counter': 'int64',
            'days_automated': 'int64',
            'first_automation': 'datetime64[ns]',
            'last_automation': 'datetime64[ns]',
        }
    )


# fixed_now = 2025-06-03; anything on/before 2025-05-05 satisfies "old enough".
_OLD = dt_actual.datetime(2024, 6, 18)  # ~350 days before fixed_now


def test_recycled_fleet_classified_ephemeral(report_instance):
    """Defect 1: a recycled fleet (long span, many deletions) is ephemeral, not standard."""
    df = _frame(
        [
            # One HostMetric record spanning ~348 days but recycled 49 times
            # while currently live -> 50 incarnations -> ~7 days each.
            _row('recycled-web', _OLD, dt_actual.datetime(2025, 6, 1), deleted=False, deleted_counter=49),
            # Genuinely long-lived host: one incarnation over ~100 days.
            _row('stable-prod', dt_actual.datetime(2025, 1, 1), dt_actual.datetime(2025, 4, 11), deleted=False, deleted_counter=0),
        ]
    )

    ephemeral = report_instance.df_managed_nodes_query(df, ephemeral=True)
    standard = report_instance.df_managed_nodes_query(df, ephemeral=False)

    assert 'recycled-web' in ephemeral['hostname'].values, 'recycled fleet must be ephemeral (defect 1)'
    assert 'recycled-web' not in standard['hostname'].values, 'recycled fleet must not be billed as standard'
    assert 'stable-prod' in standard['hostname'].values
    assert 'stable-prod' not in ephemeral['hostname'].values


def test_short_span_single_incarnation_still_ephemeral(report_instance):
    """A short-lived, never-recycled host stays ephemeral (no regression from defect-1 logic)."""
    df = _frame(
        [
            _row('short-dev', _OLD, _OLD + dt_actual.timedelta(days=5), deleted=False, deleted_counter=0),
        ]
    )
    ephemeral = report_instance.df_managed_nodes_query(df, ephemeral=True)
    assert 'short-dev' in ephemeral['hostname'].values


def test_ephemeral_and_standard_are_exact_complements(report_instance):
    """ephemeral=True and ephemeral=False partition the non-deleted set exactly."""
    df = _frame(
        [
            _row('recycled-web', _OLD, dt_actual.datetime(2025, 6, 1), deleted_counter=49),
            _row('stable-prod', dt_actual.datetime(2025, 1, 1), dt_actual.datetime(2025, 4, 11)),
            _row('short-dev', _OLD, _OLD + dt_actual.timedelta(days=5)),
        ]
    )
    total = report_instance.df_managed_nodes_query(df, ephemeral=None)
    ephemeral = report_instance.df_managed_nodes_query(df, ephemeral=True)
    standard = report_instance.df_managed_nodes_query(df, ephemeral=False)

    assert len(ephemeral) + len(standard) == len(total)
    assert set(ephemeral['hostname']).isdisjoint(set(standard['hostname']))
    assert set(ephemeral['hostname']) | set(standard['hostname']) == set(total['hostname'])


def test_deleted_ephemeral_counted_only_with_deleted(report_instance):
    """Defect 4: soft-deleted ephemerals appear only when with_deleted=True."""
    df = _frame(
        [
            _row('live-ephemeral', _OLD, _OLD + dt_actual.timedelta(days=3), deleted=False, deleted_counter=0),
            _row('gone-ephemeral', _OLD, _OLD + dt_actual.timedelta(days=3), deleted=True, deleted_counter=1),
        ]
    )

    without_deleted = report_instance.df_managed_nodes_query(df, ephemeral=True)
    with_deleted = report_instance.df_managed_nodes_query(df, ephemeral=True, with_deleted=True)

    assert 'gone-ephemeral' not in without_deleted['hostname'].values
    assert 'live-ephemeral' in without_deleted['hostname'].values
    assert set(with_deleted['hostname'].values) == {'live-ephemeral', 'gone-ephemeral'}


def test_flag_suspect_merges_identifies_false_collapse(report_instance):
    """Defects 2 & 3: only records collapsing distinct hostnames on a shared
    machine_id with no serial are flagged as merge-suspect."""
    df = _frame(
        [
            # False collapse: 3 distinct hostnames, no serial, one shared mid.
            _row(
                'ee-collapsed',
                _OLD,
                dt_actual.datetime(2025, 6, 1),
                deleted=True,
                deleted_counter=6,
                hostnames='a, b, c',
                serials='',
                mids='ee-shared-mid',
            ),
            # Legitimate merge: distinct serials justify the collapse.
            _row('serial-merge', _OLD, dt_actual.datetime(2025, 6, 1), hostnames='d, e', serials='SER1, SER2', mids=''),
            # Single identity: nothing merged.
            _row('single-host', _OLD, dt_actual.datetime(2025, 6, 1), hostnames='f', serials='', mids='m1'),
            # Multiple machine_ids: not the connection:local false-collapse pattern.
            _row('multi-mid', _OLD, dt_actual.datetime(2025, 6, 1), hostnames='g, h', serials='', mids='m1, m2'),
        ]
    )

    mask = report_instance._flag_suspect_merges(df)

    assert int(mask.sum()) == 1
    assert df[mask]['hostname'].tolist() == ['ee-collapsed']


def test_flag_suspect_merges_defensive_without_plural_columns(report_instance):
    """The flag degrades to all-False (no error) when aggregate columns are absent."""
    df = pd.DataFrame(
        {
            'hostname': ['h1', 'h2'],
            'deleted': [False, False],
            'deleted_counter': [0, 0],
            'days_automated': [1, 2],
            'first_automation': pd.to_datetime(['2025-01-01', '2025-01-02']),
            'last_automation': pd.to_datetime(['2025-01-02', '2025-01-04']),
        }
    )
    mask = report_instance._flag_suspect_merges(df)
    assert not mask.any()
    assert len(mask) == len(df)


def test_flag_suspect_merges_tolerates_non_string_aggregate_values(report_instance):
    """Non-string aggregate cells (e.g. NaN) count as zero rather than raising."""
    df = _frame(
        [
            # A genuine false-collapse whose serial cell is NaN (not ''): the
            # non-string branch of the counter must treat it as zero serials so
            # the record is still flagged.
            _row('nan-serial-collapse', _OLD, dt_actual.datetime(2025, 6, 1), hostnames='a, b', serials='', mids='shared'),
        ]
    )
    df.loc[df['hostname'] == 'nan-serial-collapse', 'ansible_product_serials'] = float('nan')

    mask = report_instance._flag_suspect_merges(df)

    assert df[mask]['hostname'].tolist() == ['nan-serial-collapse']


def test_build_data_section_emits_merge_suspect_row(report_instance):
    """Defects 2-4: the CCSP summary section writes a merge-suspect row (and the
    deleted-inclusive ephemeral total) into the worksheet when ephemeral_days is set."""
    df = _frame(
        [
            _row('stable-prod', dt_actual.datetime(2025, 1, 1), dt_actual.datetime(2025, 4, 11)),
            _row('live-ephemeral', _OLD, _OLD + dt_actual.timedelta(days=3)),
            _row('gone-ephemeral', _OLD, _OLD + dt_actual.timedelta(days=3), deleted=True, deleted_counter=1),
            # False collapse -> one merge-suspect record.
            _row(
                'ee-collapsed', _OLD, dt_actual.datetime(2025, 6, 1), deleted=True, deleted_counter=6, hostnames='a, b, c', serials='', mids='shared'
            ),
        ]
    )
    ephemeral_usage = pd.DataFrame({'ephemeral_hosts': [1, 2]})
    ws = Workbook().active

    next_row = report_instance._build_data_section(1, ws, df, ephemeral_usage)

    # The section returns the next free row and renders the new summary lines.
    assert next_row > 1
    descriptions = {row[0].value for row in ws.iter_rows(min_col=1, max_col=1) if row[0].value}
    assert any('Merge-suspect host records' in d for d in descriptions)
    assert 'Ephemeral automated hosts total' in descriptions
