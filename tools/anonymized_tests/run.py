#!/usr/bin/env python
"""
Local pipeline mirroring the metrics-service production flow.

Phase 1 – Hourly collect + prepare (one hour at a time):
  For each hour in [--since, --until):
    For each hourly collector (unified_jobs, job_host_summary, credentials, main_jobevent):
      collect()              → print rows + elapsed
      rollup.prepare()       → store hourly rollup JSON + elapsed

Phase 2 – Snapshot/daily collectors (run once):
  execution_environments, table_metadata, controller_version, feature_flags
  task_executions (metrics-service DB, optional)

Phase 3 – Merge rollups (mirrors daily_metrics_rollup):
  For each collector: rollup.merge() all hourly rollups + rollup.base()

Phase 4 – Anonymize (mirrors daily_anonymize_and_prepare):
  anonymize_rollups() → flattened + anonymized JSON

Phase 5 – Output:
  ./out/anonymized_rollup.json
  ./out/segment/chunk_*.json

Usage:
  python run.py [--since YYYY-MM-DD] [--until YYYY-MM-DD] [--no-events] [--event-collector {finished,created}]

Examples:
  python run.py
  python run.py --since "2024-01-01" --until "2024-01-02"
  python run.py --since "2024-01-01 01:00:00" --until "2024-01-01 04:00:00"
  python run.py --no-events
  python run.py --event-date-diff 30mins
  python run.py --event-date-diff 2hours
  python run.py --event-date-diff 1days
"""

import argparse
import json
import os
import shutil
import sys
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

current_dir = Path(__file__).resolve().parent
metrics_utility_path = current_dir.parent.parent
sys.path.insert(0, str(metrics_utility_path))

from metrics_utility import prepare  # noqa: E402


prepare()

from django.db import connection, connections  # noqa: E402

from metrics_utility.anonymized_rollups import (  # noqa: E402
    CredentialsAnonymizedRollup,
    EventModulesAnonymizedRollup,
    JobHostSummaryAnonymizedRollup,
    JobsAnonymizedRollup,
)
from metrics_utility.anonymized_rollups.anonymized_rollups import anonymize_rollups  # noqa: E402
from metrics_utility.anonymized_rollups.controller_version_anonymized_rollup import ControllerVersionAnonymizedRollup  # noqa: E402
from metrics_utility.anonymized_rollups.execution_environments_anonymized_rollup import ExecutionEnvironmentsAnonymizedRollup  # noqa: E402
from metrics_utility.anonymized_rollups.feature_flags_anonymized_rollup import FeatureFlagsAnonymizedRollup  # noqa: E402
from metrics_utility.anonymized_rollups.helpers import sanitize_json  # noqa: E402
from metrics_utility.anonymized_rollups.table_metadata_anonymized_rollup import TableMetadataAnonymizedRollup  # noqa: E402
from metrics_utility.anonymized_rollups.task_executions_anonymized_rollup import TaskExecutionsAnonymizedRollup  # noqa: E402
from metrics_utility.library.collectors.controller import (  # noqa: E402
    controller_version_service,
    credentials_service,
    execution_environments,
    feature_flags_service,
    job_host_summary_service,
    main_jobevent_service,
    main_jobevent_service_partition,
    table_metadata,
    unified_jobs_dashboard,
)
from metrics_utility.library.collectors.service import task_executions_service  # noqa: E402
from metrics_utility.library.storage.segment import StorageSegment  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Defaults mirror METRICS_SERVICE_JOBEVENT_ROW_LIMIT / _JOBS_PER_BATCH in metrics-service / collector.
DEFAULT_JOBEVENT_ROW_LIMIT = 200_000
DEFAULT_JOBEVENT_JOB_LIMIT = 1_000

DEFAULT_SINCE = datetime(2025, 6, 13, 0, 0, 0, tzinfo=timezone.utc)
DEFAULT_UNTIL = datetime(2025, 6, 14, 0, 0, 0, tzinfo=timezone.utc)

# Hourly collector registry – mirrors collect_hourly_metrics.py
# Order matches production schedule (:05, :10, :15, :20).
HOURLY_COLLECTORS: Dict[str, Dict[str, Any]] = {
    'job_host_summary_service': {
        'collector': job_host_summary_service,
        'rollup': JobHostSummaryAnonymizedRollup,
    },
    'unified_jobs': {
        'collector': unified_jobs_dashboard,
        'rollup': JobsAnonymizedRollup,
    },
    'credentials_service': {
        'collector': credentials_service,
        'rollup': CredentialsAnonymizedRollup,
    },
    'main_jobevent_service': {
        'collector': main_jobevent_service,
        'rollup': EventModulesAnonymizedRollup,
    },
    'main_jobevent_service_partition': {
        'collector': main_jobevent_service_partition,
        'rollup': EventModulesAnonymizedRollup,
    },
}

# Snapshot/daily collector registry – mirrors collect_snapshot_metrics.py
SNAPSHOT_COLLECTORS: Dict[str, Dict[str, Any]] = {
    'execution_environments': {
        'collector': execution_environments,
        'rollup': ExecutionEnvironmentsAnonymizedRollup,
        'snapshot': True,
    },
    'table_metadata': {
        'collector': table_metadata,
        'rollup': TableMetadataAnonymizedRollup,
        'snapshot': True,
    },
    'controller_version_service': {
        'collector': controller_version_service,
        'rollup': ControllerVersionAnonymizedRollup,
        'snapshot': True,
    },
    'feature_flags_service': {
        'collector': feature_flags_service,
        'rollup': FeatureFlagsAnonymizedRollup,
        'snapshot': True,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_datetime(dt_str: str) -> datetime:
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f'Cannot parse datetime: {dt_str!r}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS')


def parse_event_date_diff(value: str) -> timedelta:
    """Parse --event-date-diff value into a timedelta.

    Accepted formats: {N}mins, {N}hours, {N}days  (N is a positive integer).
    """
    import re

    m = re.fullmatch(r'(\d+)(mins|hours|days)', value.strip())
    if not m:
        raise ValueError(f'Cannot parse --event-date-diff {value!r}. Use {{N}}mins, {{N}}hours, or {{N}}days (e.g. 30mins, 2hours, 1days).')
    n = int(m.group(1))
    unit = m.group(2)
    if unit == 'mins':
        return timedelta(minutes=n)
    if unit == 'hours':
        return timedelta(hours=n)
    return timedelta(days=n)


def hourly_intervals(since: datetime, until: datetime) -> List[Tuple[datetime, datetime]]:
    """Split [since, until) into 1-hour windows."""
    intervals = []
    current = since
    while current < until:
        next_hour = min(current + timedelta(hours=1), until)
        intervals.append((current, next_hour))
        current = next_hour
    return intervals


def fmt_time(seconds: float) -> str:
    if seconds >= 3600:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = seconds % 60
        return f'{h}h {m}m {s:.2f}s'
    if seconds >= 60:
        m = int(seconds // 60)
        s = seconds % 60
        return f'{m}m {s:.2f}s'
    return f'{seconds:.2f}s'


def df_rows(df: Optional[pd.DataFrame]) -> int:
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return 0
    return len(df)


def _json_roundtrip(data: Any) -> Any:
    """Serialize + deserialize — mirrors what metrics-service does when persisting to DB."""
    return json.loads(json.dumps(data))


def _section(title: str) -> None:
    print()
    print('=' * 70)
    print(title)
    print('=' * 70)


# ---------------------------------------------------------------------------
# Pipeline phases
# ---------------------------------------------------------------------------


def phase1_hourly(
    intervals: List[Tuple[datetime, datetime]],
    db,
    collect_events: bool,
    row_limit: int = DEFAULT_JOBEVENT_ROW_LIMIT,
    job_limit: int = DEFAULT_JOBEVENT_JOB_LIMIT,
    event_date_diff: Optional[timedelta] = None,
) -> Tuple[Dict[str, List[Any]], Dict[str, Dict[str, float]]]:
    """
    For each hour: collect → prepare → store hourly rollup JSON.

    event_date_diff, when set, shifts the since/until window backwards for
    main_jobevent_service_partition only (other collectors are unaffected).

    Returns:
        hourly_rollups  – {collector_name: [prepared_json, ...]}  one entry per hour
        timing          – {collector_name: {'collect': float, 'prepare': float}}
                          (summed across all hours)
    """
    hourly_rollups: Dict[str, List[Any]] = {name: [] for name in HOURLY_COLLECTORS}
    timing: Dict[str, Dict[str, float]] = {name: {'collect': 0.0, 'prepare': 0.0} for name in HOURLY_COLLECTORS}

    COL_NAME = 35
    COL_ROWS = 14
    COL_TIME = 10

    for hour_idx, (hour_since, hour_until) in enumerate(intervals):
        _section(f'Hour {hour_idx + 1}/{len(intervals)}  {hour_since.strftime("%Y-%m-%d %H:%M")} – {hour_until.strftime("%H:%M")} UTC')

        # Print table header
        print(f'  {"Collector":<{COL_NAME}}  {"rows":>{COL_ROWS}}  {"collect":>{COL_TIME}}  {"prepare":>{COL_TIME}}  note')
        print(f'  {"-" * COL_NAME}  {"-" * COL_ROWS}  {"-" * COL_TIME}  {"-" * COL_TIME}  ----')

        for collector_name, cfg in HOURLY_COLLECTORS.items():
            _is_events = collector_name in ('main_jobevent_service', 'main_jobevent_service_partition')
            if _is_events and not collect_events:
                print(f'  {collector_name:<{COL_NAME}}  {"–":>{COL_ROWS}}  {"–":>{COL_TIME}}  {"–":>{COL_TIME}}  skipped (--no-events)')
                hourly_rollups[collector_name].append(None)
                continue

            # --- collect ---
            if collector_name == 'main_jobevent_service':
                extra = {'row_limit': row_limit, 'job_limit': job_limit}
                c_since, c_until = hour_since, hour_until
            elif collector_name == 'main_jobevent_service_partition':
                extra = {'row_limit': row_limit}
                if event_date_diff is not None:
                    c_since = hour_since - event_date_diff
                    c_until = hour_until - event_date_diff
                else:
                    c_since, c_until = hour_since, hour_until
            else:
                extra = {}
                c_since, c_until = hour_since, hour_until
            t0 = time.time()
            try:
                df = cfg['collector'](db=db, since=c_since, until=c_until, **extra).gather()
            except Exception as exc:
                print(f'  {collector_name:<{COL_NAME}}  {"ERROR":>{COL_ROWS}}  {fmt_time(time.time() - t0):>{COL_TIME}}  {"–":>{COL_TIME}}  {exc}')
                hourly_rollups[collector_name].append(None)
                continue
            collect_elapsed = time.time() - t0
            timing[collector_name]['collect'] += collect_elapsed
            rows = df_rows(df)

            # --- prepare (hourly rollup) ---
            t0 = time.time()
            try:
                prepared = cfg['rollup']().prepare(df)
                prepared = _json_roundtrip(prepared)
            except Exception as exc:
                print(f'  {collector_name:<{COL_NAME}}  {rows:>{COL_ROWS},}  {fmt_time(collect_elapsed):>{COL_TIME}}  {"ERROR":>{COL_TIME}}  {exc}')
                hourly_rollups[collector_name].append(None)
                continue
            prepare_elapsed = time.time() - t0
            timing[collector_name]['prepare'] += prepare_elapsed

            if _is_events and row_limit is not None and rows >= row_limit:
                note = f'row limit reached ({row_limit:,})'
            elif collector_name == 'main_jobevent_service_partition' and event_date_diff is not None:
                note = f'window shifted -{event_date_diff}  ({c_since.strftime("%H:%M")}–{c_until.strftime("%H:%M")} UTC)'
            else:
                note = ''
            row = (
                f'  {collector_name:<{COL_NAME}}  {rows:>{COL_ROWS},}'
                f'  {fmt_time(collect_elapsed):>{COL_TIME}}  {fmt_time(prepare_elapsed):>{COL_TIME}}  {note}'
            )
            print(row)

            hourly_rollups[collector_name].append(prepared)

    return hourly_rollups, timing


def phase2_snapshots(
    db,
    service_db,
    since: datetime,
    until: datetime,
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, float]]]:
    """
    Run snapshot + daily collectors once.

    Returns:
        snapshot_rollups – {collector_name: prepared_json}
        timing           – {collector_name: {'collect': float, 'prepare': float}}
    """
    snapshot_rollups: Dict[str, Any] = {}
    timing: Dict[str, Dict[str, float]] = {}

    _section('Phase 2 – Snapshot / Daily Collectors')

    COL_NAME = 35
    COL_ROWS = 14
    COL_TIME = 10
    print(f'  {"Collector":<{COL_NAME}}  {"rows":>{COL_ROWS}}  {"collect":>{COL_TIME}}  {"prepare":>{COL_TIME}}  note')
    print(f'  {"-" * COL_NAME}  {"-" * COL_ROWS}  {"-" * COL_TIME}  {"-" * COL_TIME}  ----')

    for collector_name, cfg in SNAPSHOT_COLLECTORS.items():
        t0 = time.time()
        try:
            df = cfg['collector'](db=db).gather()
        except Exception as exc:
            print(f'  {collector_name:<{COL_NAME}}  {"ERROR":>{COL_ROWS}}  {fmt_time(time.time() - t0):>{COL_TIME}}  {"–":>{COL_TIME}}  {exc}')
            snapshot_rollups[collector_name] = None
            timing[collector_name] = {'collect': time.time() - t0, 'prepare': 0.0}
            continue
        collect_elapsed = time.time() - t0
        rows = df_rows(df)

        t0 = time.time()
        try:
            prepared = cfg['rollup']().prepare(df)
            prepared = _json_roundtrip(prepared)
        except Exception as exc:
            print(f'  {collector_name:<{COL_NAME}}  {rows:>{COL_ROWS},}  {fmt_time(collect_elapsed):>{COL_TIME}}  {"ERROR":>{COL_TIME}}  {exc}')
            prepared = None
            timing[collector_name] = {'collect': collect_elapsed, 'prepare': 0.0}
            snapshot_rollups[collector_name] = None
            continue
        prepare_elapsed = time.time() - t0

        print(
            f'  {collector_name:<{COL_NAME}}  {rows:>{COL_ROWS},}  {fmt_time(collect_elapsed):>{COL_TIME}}  {fmt_time(prepare_elapsed):>{COL_TIME}}'
        )
        snapshot_rollups[collector_name] = prepared
        timing[collector_name] = {'collect': collect_elapsed, 'prepare': prepare_elapsed}

    # task_executions_service lives on the metrics-service DB
    collector_name = 'task_executions_service'
    t0 = time.time()
    try:
        df = task_executions_service(db=service_db, since=since, until=until).gather()
        collect_elapsed = time.time() - t0
        rows = df_rows(df)
        t0 = time.time()
        prepared = TaskExecutionsAnonymizedRollup().prepare(df)
        prepared = _json_roundtrip(prepared)
        prepare_elapsed = time.time() - t0
        print(
            f'  {collector_name:<{COL_NAME}}  {rows:>{COL_ROWS},}  {fmt_time(collect_elapsed):>{COL_TIME}}  {fmt_time(prepare_elapsed):>{COL_TIME}}'
        )
        snapshot_rollups[collector_name] = prepared
        timing[collector_name] = {'collect': collect_elapsed, 'prepare': prepare_elapsed}
    except Exception as exc:
        collect_elapsed = time.time() - t0
        print(f'  {collector_name:<{COL_NAME}}  {"–":>{COL_ROWS}}  {fmt_time(collect_elapsed):>{COL_TIME}}  {"–":>{COL_TIME}}  skipped ({exc})')
        snapshot_rollups[collector_name] = None
        timing[collector_name] = {'collect': collect_elapsed, 'prepare': 0.0}

    return snapshot_rollups, timing


def phase3_merge(
    hourly_rollups: Dict[str, List[Any]],
    snapshot_rollups: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """
    Merge all hourly rollups per collector, then call base().
    Mirrors daily_metrics_rollup task.

    Returns:
        daily_json  – {collector_name: final_json}
        timing      – {collector_name: merge_elapsed}
    """
    _section('Phase 3 – Merge Rollups (daily_metrics_rollup)')

    # Map collector registry name → rollup class
    all_rollup_classes: Dict[str, Any] = {name: cfg['rollup'] for name, cfg in HOURLY_COLLECTORS.items()}
    for name, cfg in SNAPSHOT_COLLECTORS.items():
        all_rollup_classes[name] = cfg['rollup']
    all_rollup_classes['task_executions_service'] = TaskExecutionsAnonymizedRollup

    daily_json: Dict[str, Any] = {}
    timing: Dict[str, float] = {}

    # Hourly collectors: merge list of hourly prepared JSONs
    for collector_name, rollup_cls in {n: c for n, c in all_rollup_classes.items() if n in HOURLY_COLLECTORS}.items():
        t0 = time.time()
        rollup = rollup_cls()
        batches = hourly_rollups.get(collector_name, [])
        merged = None
        valid_batches = 0
        for batch in batches:
            if batch is None:
                continue
            merged = rollup.merge(merged, batch)
            merged = _json_roundtrip(merged)
            valid_batches += 1

        try:
            result = rollup.base(merged)
            daily_json[collector_name] = result.get('json', {})
        except Exception as exc:
            print(f'  {collector_name}: ERROR during base(): {exc}')
            daily_json[collector_name] = {}

        elapsed = time.time() - t0
        timing[collector_name] = elapsed
        print(f'  {collector_name}: merged {valid_batches} hourly batches  ({fmt_time(elapsed)})')

    # Snapshot/daily collectors: single prepared batch
    for collector_name, rollup_cls in {n: c for n, c in all_rollup_classes.items() if n not in HOURLY_COLLECTORS}.items():
        t0 = time.time()
        rollup = rollup_cls()
        batch = snapshot_rollups.get(collector_name)
        if batch is not None:
            merged = rollup.merge(None, batch)
        else:
            merged = None

        try:
            result = rollup.base(merged)
            daily_json[collector_name] = result.get('json', {})
        except Exception as exc:
            print(f'  {collector_name}: ERROR during base(): {exc}')
            daily_json[collector_name] = {}

        elapsed = time.time() - t0
        timing[collector_name] = elapsed
        print(f'  {collector_name}: merged 1 snapshot batch  ({fmt_time(elapsed)})')

    return daily_json, timing


def phase4_anonymize(daily_json: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, float]]:
    """
    Run anonymize_rollups() once per events-collector pipeline.

    Each events collector (main_jobevent_service, main_jobevent_service_partition)
    produces an independent anonymized result. All non-events rollup data is shared.

    Returns:
        results  – {events_collector_name: anonymized_data | None}
        timings  – {events_collector_name: elapsed_seconds}
    """
    _section('Phase 4 – Anonymize (daily_anonymize_and_prepare)')

    shared_kwargs = dict(
        execution_environments_rollup=daily_json.get('execution_environments', {}),
        jobs_rollup=daily_json.get('unified_jobs', {}),
        job_host_summary_rollup=daily_json.get('job_host_summary_service', {}),
        credentials_rollup=daily_json.get('credentials_service', {}),
        table_metadata_rollup=daily_json.get('table_metadata', {}),
        controller_version_rollup=daily_json.get('controller_version_service', {}),
        feature_flags_rollup=daily_json.get('feature_flags_service', {}),
        task_executions_rollup=daily_json.get('task_executions_service', {}),
    )

    results: Dict[str, Any] = {}
    timings: Dict[str, float] = {}

    for events_collector in ('main_jobevent_service', 'main_jobevent_service_partition'):
        t0 = time.time()
        try:
            result = anonymize_rollups(
                events_modules_rollup=daily_json.get(events_collector, {}),
                **shared_kwargs,
            )
            result = sanitize_json(result)
            elapsed = time.time() - t0
            print(f'  [{events_collector}] anonymize_rollups: done  ({fmt_time(elapsed)})')
        except Exception as exc:
            elapsed = time.time() - t0
            print(f'  [{events_collector}] anonymize_rollups: ERROR: {exc}')
            import traceback

            traceback.print_exc()
            result = None
        results[events_collector] = result
        timings[events_collector] = elapsed

    return results, timings


def phase5_output(results: Dict[str, Any]) -> None:
    """Save one anonymized JSON + Segment chunks per events-collector pipeline."""
    _section('Phase 5 – Output')

    storage_segment = StorageSegment()

    for pipeline, json_data in results.items():
        print(f'\n  Pipeline: {pipeline}')
        if json_data is None:
            print('    Skipped (no data)')
            continue

        pipeline_dir = os.path.join('./out', pipeline)
        json_path = os.path.join(pipeline_dir, 'anonymized_rollup.json')
        os.makedirs(pipeline_dir, exist_ok=True)

        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=4)
        print(f'    Final JSON saved to: {json_path}')

        chunks = storage_segment._split_into_chunks(json_data, storage_segment.REGULAR_MESSAGE_LIMIT)
        print(f'    Segment chunks: {len(chunks)}  (limit {storage_segment.REGULAR_MESSAGE_LIMIT / 1024:.0f} KB each)')

        segment_dir = os.path.join(pipeline_dir, 'segment')
        os.makedirs(segment_dir, exist_ok=True)

        for i, chunk in enumerate(chunks, 1):
            chunk_size = storage_segment._calculate_size(chunk)
            chunk_key = next(iter(chunk), 'unknown')
            chunk_path = f'{segment_dir}/chunk_{i:03d}_of_{len(chunks):03d}.json'
            with open(chunk_path, 'w') as f:
                json.dump(chunk, f, indent=4)
            items = f' ({len(chunk[chunk_key])} items)' if isinstance(chunk.get(chunk_key), list) else ''
            print(f'    Chunk {i:3d}/{len(chunks)}: {chunk_key}  {chunk_size:,} bytes{items}  → {chunk_path}')


# ---------------------------------------------------------------------------
# Time summary
# ---------------------------------------------------------------------------


def print_time_summary(
    intervals: List[Tuple[datetime, datetime]],
    hourly_timing: Dict[str, Dict[str, float]],
    snapshot_timing: Dict[str, Dict[str, float]],
    merge_timing: Dict[str, float],
    anon_timings: Dict[str, float],
    total_elapsed: float,
) -> None:
    _section('Time Summary')

    num_hours = max(len(intervals), 1)

    # Hourly collectors – show total + avg per hour
    print(f'  {"Collector":<35}  {"collect":>10}  {"avg/hour":>10}  {"prepare":>10}  {"avg/hour":>10}  {"merge":>10}')
    print(f'  {"-" * 35}  {"-" * 10}  {"-" * 10}  {"-" * 10}  {"-" * 10}  {"-" * 10}')

    for name, t in hourly_timing.items():
        merge = merge_timing.get(name, 0.0)
        avg_collect = t['collect'] / num_hours
        avg_prepare = t['prepare'] / num_hours
        print(
            f'  {name:<35}  {fmt_time(t["collect"]):>10}  {fmt_time(avg_collect):>10}'
            f'  {fmt_time(t["prepare"]):>10}  {fmt_time(avg_prepare):>10}  {fmt_time(merge):>10}'
        )

    # Snapshot/daily collectors – no avg (run once)
    print()
    print(f'  {"Snapshot/daily collector":<35}  {"collect":>10}  {"":>10}  {"prepare":>10}  {"":>10}  {"merge":>10}')
    print(f'  {"-" * 35}  {"-" * 10}  {"-" * 10}  {"-" * 10}  {"-" * 10}  {"-" * 10}')

    for name, t in snapshot_timing.items():
        merge = merge_timing.get(name, 0.0)
        print(f'  {name:<35}  {fmt_time(t["collect"]):>10}  {"(once)":>10}  {fmt_time(t["prepare"]):>10}  {"(once)":>10}  {fmt_time(merge):>10}')

    print()
    for pipeline, elapsed in anon_timings.items():
        label = f'anonymize [{pipeline}]'
        print(f'  {label:<35}  {"":>10}  {"":>10}  {"":>10}  {"":>10}  {fmt_time(elapsed):>10}')
    print()
    print(f'  Hours processed : {num_hours}')
    print(f'  Total elapsed   : {fmt_time(total_elapsed)}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description='Run the full metrics-service pipeline locally against the AWX DB.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--since',
        type=str,
        help='Start datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS, UTC)',
    )
    parser.add_argument(
        '--until',
        type=str,
        help='End datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS, UTC)',
    )
    parser.add_argument(
        '--no-events',
        action='store_true',
        help='Skip both events collectors',
    )
    parser.add_argument(
        '--max-events',
        type=int,
        default=DEFAULT_JOBEVENT_ROW_LIMIT,
        metavar='N',
        help=f'Max event rows fetched per hourly window (default: {DEFAULT_JOBEVENT_ROW_LIMIT:,})',
    )
    parser.add_argument(
        '--max-jobs',
        type=int,
        default=DEFAULT_JOBEVENT_JOB_LIMIT,
        metavar='N',
        help=f'Max finished jobs processed per hourly window (default: {DEFAULT_JOBEVENT_JOB_LIMIT:,})',
    )
    parser.add_argument(
        '--event-date-diff',
        type=str,
        default=None,
        metavar='DIFF',
        dest='event_date_diff',
        help=(
            'Shift the since/until window backwards for main_jobevent_service_partition only. '
            'Format: {N}mins, {N}hours, or {N}days (e.g. 30mins, 2hours, 1days). '
            'All other collectors are unaffected.'
        ),
    )
    args = parser.parse_args()

    total_start = time.time()

    since = parse_datetime(args.since) if args.since else DEFAULT_SINCE
    until = parse_datetime(args.until) if args.until else DEFAULT_UNTIL

    if since >= until:
        raise ValueError(f'--since ({since}) must be before --until ({until})')

    intervals = hourly_intervals(since, until)
    collect_events = not args.no_events
    event_date_diff = parse_event_date_diff(args.event_date_diff) if args.event_date_diff else None

    # Clean output dir
    out_dir = './out'
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir, ignore_errors=True)

    row_limit = args.max_events
    job_limit = args.max_jobs

    print(f'Range           : {since}  →  {until}')
    print(f'Hours           : {len(intervals)}')
    print(f'Events          : {"enabled" if collect_events else "disabled (--no-events)"}')
    if collect_events:
        print(f'Max events      : {row_limit:,} rows per hour  (--max-events)')
        print(f'Max jobs        : {job_limit:,} jobs per hour   (--max-jobs)')
        if event_date_diff is not None:
            print(f'Event date diff : -{event_date_diff}  (partition collector window shifted back)')

    db = connection

    # Try to get metrics-service DB connection for task_executions_service
    try:
        service_db = connections['metrics_service']
    except Exception:
        service_db = None

    # Phase 1 – hourly collect + prepare
    hourly_rollups, hourly_timing = phase1_hourly(intervals, db, collect_events, row_limit, job_limit, event_date_diff)

    # Phase 2 – snapshot/daily collectors
    snapshot_rollups, snapshot_timing = phase2_snapshots(db, service_db, since, until)

    # Phase 3 – merge rollups
    daily_json, merge_timing = phase3_merge(hourly_rollups, snapshot_rollups)

    # Phase 4 – anonymize (one report per events-collector pipeline)
    anonymized, anon_timings = phase4_anonymize(daily_json)

    # Phase 5 – write output (one subdirectory per pipeline)
    phase5_output(anonymized)

    # Time summary
    print_time_summary(
        intervals,
        hourly_timing,
        snapshot_timing,
        merge_timing,
        anon_timings,
        time.time() - total_start,
    )


if __name__ == '__main__':
    main()
