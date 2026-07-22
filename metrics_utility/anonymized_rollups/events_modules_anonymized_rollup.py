"""Anonymized rollup for main_jobevent_service collector data.

Aggregates module, collection, and role usage statistics from job event data,
anonymising custom module/collection/role names before inclusion in reports.

Event counts use a direct 1-to-1 mapping to Ansible event types — one counter
per known event type — with no inferred classification (no "task outcome"
collapsing).  Retries are counted explicitly via ``runner_retry`` events.

For loops the task-level summary event (runner_on_failed/ok) and the
item-level events (runner_item_on_*) are in separate counters so consumers
can use each group independently without double-counting.
"""

import json
import re

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import load_known_collections, sanitize_json
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import DataframeContentUsage


# Regex pattern to match collection names (e.g., namespace.collection.role or namespace.collection.role.task)
# Pattern is safe from reDOS: uses non-capturing groups and non-nested quantifiers
_COLLECTION_RE = re.compile(r'^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$')
_COLLECTION_PATTERN = r'^([A-Za-z0-9_]+\.[A-Za-z0-9_]+)\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$'

# All runner event types tracked per module/collection/role
_RUNNER_EVENTS = frozenset(
    [
        'runner_on_ok',
        'runner_on_async_ok',
        'runner_item_on_ok',
        'runner_on_failed',
        'runner_on_async_failed',
        'runner_item_on_failed',
        'runner_on_unreachable',
        'runner_retry',
    ]
)
# Job-level annotation events counted into top-level warnings_total /
# deprecations_total.  Playbook lifecycle events (playbook_on_*) are not
# collected — high volume, low unique information vs jobs/runner counters.


def _normalize_stats_item(item: dict) -> None:
    """Remove host_ids and rename module_name/collection_name keys for Segment compatibility.

    Segment drops properties whose key contains 'name', so module_name -> module
    and collection_name -> collection before the payload is sent.

    Identity keys are placed first so the report reads as:
    module/role, collection, collection_source, then metrics.
    """
    item.pop('host_ids', None)
    if 'module_name' in item:
        item['module'] = item.pop('module_name')
    if 'collection_name' in item:
        item['collection'] = item.pop('collection_name')

    leading = [key for key in ('module', 'role', 'collection', 'collection_source') if key in item]
    if not leading:
        return

    reordered = {key: item.pop(key) for key in leading}
    reordered.update(item)
    item.clear()
    item.update(reordered)


def extract_collection_name(x: str | None) -> str | None:
    """Extract the ``namespace.collection`` prefix from a fully-qualified module name.

    Args:
        x: A module name such as ``ansible.builtin.copy`` or None/NaN.

    Returns:
        The two-part collection name (e.g. ``ansible.builtin``), or None if the
        input does not match the expected pattern.
    """
    if x is None or pd.isna(x):
        return None
    s = str(x).strip() if not isinstance(x, str) else x.strip()
    if not s or s.lower() == 'nan':
        return None
    m = _COLLECTION_RE.match(s)
    return f'{m.group(1)}.{m.group(2)}' if m else None


def merge_by_name(obj1, obj2, name_key):
    """Merge two lists of dicts by a common key, later values overwriting earlier ones.

    Args:
        obj1: First list of dicts.
        obj2: Second list of dicts.
        name_key: The dict key used to identify matching records.

    Returns:
        A list of merged dicts with one entry per unique key value.
    """
    merged = {}

    for entry in obj1 + obj2:
        key = entry[name_key]
        merged.setdefault(key, {}).update(entry)

    return list(merged.values())


class EventModulesAnonymizedRollup(BaseAnonymizedRollup):
    """
    Event collections rollups operate over main_jobevent_service collector data.

    Important columns in data:
    module_name (task_action) - name of the module that was executed
    job_id - id of the job that was executed
    host_id - id of the host that was automated
    playbook - name of the playbook that was executed
    job_created - timestamp of the job creation
    job_started - timestamp of the job start
    job_finished - timestamps of the job finish
    event - name of the event that was executed

    Event counting strategy
    -----------------------
    Each Ansible event type gets its own counter.  No task-level outcome
    inference is performed, avoiding misclassification due to loops,
    block/rescue, and retries.

    Sync task-level events (runner_on_*):
        runner_on_ok_total, runner_on_failed_total, runner_on_unreachable_total

    Async task-level events (runner_on_async_*):
        runner_on_async_ok_total, runner_on_async_failed_total

    Loop item-level events (runner_item_on_*):
        runner_item_on_ok_total, runner_item_on_failed_total

    Retry events:
        runner_retry_total

    For a loop with partial item failures the task-level runner_on_failed and
    the item-level runner_item_on_failed are in separate counters, so consumers
    can reason about them independently.

    ignore_errors is tracked as a single combined ``ignore_errors_total``
    counter across all failed-variant events (runner_on_failed,
    runner_on_async_failed, runner_item_on_failed), rather than as a per-event
    ``*_ignored`` split. This is a deliberate simplification: ansible-core does
    not expose an ignore_errors signal anywhere on runner_on_async_failed
    events (see main_jobevent_service.py for details), so a per-event split
    would silently under-count ignored async failures as non-ignored. The
    *_total counters above are therefore unconditional (count every event of
    that type regardless of ignore_errors), and ignore_errors_total is a
    best-effort, currently-incomplete signal that undercounts ignored async
    failures. Revisit if/when async ignore_errors can be recovered (e.g. via
    the companion runner_on_failed event ansible-core also emits for a failed
    async task).

    Annotation events (warning / deprecated) are counted into top-level
    ``warnings_total`` / ``deprecations_total`` (promoted to statistics in the
    final anonymized report).
    """

    # Numeric columns summed when merging batches
    _NUMERIC_COLS = [
        'jobs_total',
        'jobs_successful_total',
        'jobs_failed_total',
        'jobs_duration_total_seconds',
        'jobs_waiting_time_total_seconds',
        'jobs_never_started_total',
        'jobs_successful_duration_total_seconds',
        'jobs_failed_duration_total_seconds',
        'runner_on_ok_total',
        'runner_on_failed_total',
        'runner_on_unreachable_total',
        'runner_on_async_ok_total',
        'runner_on_async_failed_total',
        'runner_item_on_ok_total',
        'runner_item_on_failed_total',
        'runner_retry_total',
        'ignore_errors_total',
        'warnings_total',
        'deprecations_total',
        'collected_events_total',
        'event_data_size_total',
    ]
    _LIST_COLS = ['ansible_versions']

    def __init__(self):
        super().__init__('events_modules')

        self.collector_names = ['main_jobevent_service']

        self.collections = load_known_collections()

    # ------------------------------------------------------------------
    # Merge helpers
    # ------------------------------------------------------------------

    def _create_lookup_dict(self, stats_list, groupby_cols):
        """Create a lookup dictionary keyed by grouping columns."""
        lookup = {}
        for item in stats_list:
            key = tuple(item.get(col) for col in groupby_cols)
            lookup[key] = item.copy()
        return lookup

    def _merge_numeric_columns(self, item_all, item_new, merged_item):
        """Sum numeric columns from both items."""
        for col in self._NUMERIC_COLS:
            val_all = item_all.get(col) if item_all.get(col) is not None else 0
            val_new = item_new.get(col) if item_new.get(col) is not None else 0
            merged_item[col] = val_all + val_new

    def _merge_list_columns(self, item_all, item_new, merged_item):
        """Union list columns from both items (sorted for deterministic JSON output)."""
        for col in self._LIST_COLS:
            list_all = item_all.get(col) if item_all.get(col) is not None else []
            list_new = item_new.get(col) if item_new.get(col) is not None else []
            set_all = set(list_all) if isinstance(list_all, list) else set()
            set_new = set(list_new) if isinstance(list_new, list) else set()
            merged_item[col] = sorted(set_all.union(set_new))

    def _merge_single_item(self, item_all, item_new):
        """Merge a single item from all and new data."""
        if item_all:
            merged_item = item_all.copy()
        elif item_new:
            merged_item = item_new.copy()
        else:
            return None

        if item_all and item_new:
            self._merge_numeric_columns(item_all, item_new, merged_item)
            self._merge_list_columns(item_all, item_new, merged_item)

        return merged_item

    def _merge_stats_json(self, stats_all, stats_new, groupby_cols):
        """Merge two stats JSON lists by summing numeric columns and unioning lists."""
        if not stats_all:
            return stats_new if stats_new else []
        if not stats_new:
            return stats_all if stats_all else []

        all_dict = self._create_lookup_dict(stats_all, groupby_cols)
        new_dict = self._create_lookup_dict(stats_new, groupby_cols)

        merged_list = []
        all_keys = set(all_dict.keys()) | set(new_dict.keys())

        for key in all_keys:
            item_all = all_dict.get(key, {})
            item_new = new_dict.get(key, {})
            merged_item = self._merge_single_item(item_all, item_new)
            if merged_item:
                merged_list.append(merged_item)

        return merged_list

    def _merge_unique_modules(self, data_all, data_new):
        """Merge unique_modules lists (union and sort)."""
        unique_modules_all = set(data_all.get('unique_modules', []))
        unique_modules_new = set(data_new.get('unique_modules', []))
        return sorted(list(unique_modules_all.union(unique_modules_new)))

    def _merge_modules_per_playbook(self, data_all, data_new):
        """Merge modules_per_playbook dicts (union lists per playbook)."""
        modules_per_playbook_all = data_all.get('modules_per_playbook', {})
        modules_per_playbook_new = data_new.get('modules_per_playbook', {})
        modules_per_playbook = {}
        all_playbooks = set(modules_per_playbook_all.keys()) | set(modules_per_playbook_new.keys())
        for playbook in all_playbooks:
            list_all = modules_per_playbook_all.get(playbook, []) or []
            list_new = modules_per_playbook_new.get(playbook, []) or []
            set_all = set(list_all) if isinstance(list_all, list) else set()
            set_new = set(list_new) if isinstance(list_new, list) else set()
            modules_per_playbook[playbook] = sorted(list(set_all.union(set_new)))
        return modules_per_playbook

    def merge(self, data_all, data_new):
        """
        Override merge to aggregate module_stats, collection_stats, role_stats from batches.
        Works with JSON structures (lists of dicts), sums numeric columns and unions lists.
        """
        if data_all is None:
            return data_new

        module_stats = self._merge_stats_json(
            data_all.get('module_stats', []),
            data_new.get('module_stats', []),
            ['module_name', 'collection_name', 'collection_source'],
        )
        collection_stats = self._merge_stats_json(
            data_all.get('collection_stats', []),
            data_new.get('collection_stats', []),
            ['collection_name', 'collection_source'],
        )
        role_stats = self._merge_stats_json(
            data_all.get('role_stats', []),
            data_new.get('role_stats', []),
            ['role', 'collection_name', 'collection_source'],
        )

        return {
            'collected_events_total': data_all['collected_events_total'] + data_new['collected_events_total'],
            'warnings_total': data_all.get('warnings_total', 0) + data_new.get('warnings_total', 0),
            'deprecations_total': data_all.get('deprecations_total', 0) + data_new.get('deprecations_total', 0),
            'module_stats': module_stats,
            'collection_stats': collection_stats,
            'role_stats': role_stats,
            'unique_modules': self._merge_unique_modules(data_all, data_new),
            'modules_per_playbook': self._merge_modules_per_playbook(data_all, data_new),
        }

    # ------------------------------------------------------------------
    # Preparation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_event_data_length(dataframe):
        """Ensure event_data_length is a numeric column (bytes); missing values become 0."""
        if dataframe is None or dataframe.empty:
            return dataframe
        if 'event_data_length' not in dataframe.columns:
            return dataframe.assign(event_data_length=0)
        return dataframe.assign(event_data_length=pd.to_numeric(dataframe['event_data_length'], errors='coerce').fillna(0))

    def _count_initial_statistics(self, dataframe):
        """Count all events, warnings, and deprecations before filtering."""
        collected_events_total = len(dataframe) if dataframe is not None and not dataframe.empty else 0

        if dataframe is None or dataframe.empty or 'event' not in dataframe.columns:
            warnings_total = 0
            deprecations_total = 0
        else:
            warnings_total = int((dataframe['event'] == 'warning').sum())
            deprecations_total = int((dataframe['event'] == 'deprecated').sum())

        return collected_events_total, warnings_total, deprecations_total

    def _filter_runner_events(self, dataframe):
        """Filter dataframe to runner event types used for module/collection/role stats."""
        return dataframe[dataframe['event'].isin(_RUNNER_EVENTS)]

    def _prepare_basic_columns(self, dataframe):
        """Prepare basic columns: ignore_errors, datetime, module_name, role, job_failed."""
        if 'ignore_errors' not in dataframe.columns:
            dataframe['ignore_errors'] = False
        dataframe['ignore_errors'] = dataframe['ignore_errors'].fillna(False).astype(bool)

        for col in ['job_created', 'job_started', 'job_finished']:
            if col in dataframe.columns:
                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce', utc=True)

        dataframe['module_name'] = (
            dataframe['resolved_action'].fillna(dataframe['task_action']).where(lambda s: s.notna() & (s.astype(str).str.strip() != ''))
        )

        dataframe['role'] = dataframe['resolved_role'].fillna(dataframe['role']).astype(str)
        raw_role = dataframe['role']
        dataframe['role'] = raw_role.apply(lambda x: DataframeContentUsage.extract_role_name(x))

        # extract_role_name only recognises Galaxy-style dotted names
        # (namespace.role or namespace.collection.role). Most locally-authored
        # roles Ansible reports (task._role._role_name) are a single bare word
        # (e.g. "webserver") with no dot at all, so extract_role_name returns
        # None for them. Keep the raw name instead of silently dropping the
        # role from role_stats; it will correctly fall through to a "Custom"
        # collection_source below since it has no extractable collection.
        bare_role = raw_role.str.strip()
        bare_role_mask = dataframe['role'].isna() & (bare_role != '') & (bare_role.str.lower() != 'nan')
        dataframe.loc[bare_role_mask, 'role'] = bare_role[bare_role_mask]

        dataframe = dataframe.assign(job_failed=dataframe['job_failed'].fillna(False).astype(bool))
        return dataframe

    def _extract_collection_info(self, dataframe):
        """Extract collection_name and collection_source from module_name."""
        dataframe['collection_name'] = dataframe['module_name'].str.extract(_COLLECTION_PATTERN, expand=False)
        dataframe['collection_source'] = dataframe['collection_name'].map(self.collections).fillna('Custom')
        return dataframe

    def _compute_job_metrics(self, dataframe):
        """Compute job duration and waiting time metrics."""
        dataframe['job_duration_seconds'] = (dataframe['job_finished'] - dataframe['job_started']).dt.total_seconds()
        dataframe['job_waiting_time_seconds'] = (dataframe['job_started'] - dataframe['job_created']).dt.total_seconds()
        return dataframe

    @staticmethod
    def _parse_and_check_json_array(x):
        """Parse JSON array (string, list, or dict) and return True if it contains items.

        List/dict must be checked before ``pd.isnull``: ``pd.isnull([])`` returns an
        ndarray and ``if pd.isnull(x)`` raises ValueError on empty lists.
        """
        if isinstance(x, (list, dict)):
            parsed = x
        elif x is None or (not isinstance(x, str) and pd.isnull(x)):
            return False
        else:
            try:
                parsed = json.loads(x) if isinstance(x, str) else x
            except (json.JSONDecodeError, TypeError, ValueError):
                return False
        if isinstance(parsed, list):
            return len(parsed) > 0
        return bool(parsed)

    def _parse_warnings_deprecations(self, dataframe):
        """Parse warnings and deprecations from event_data.res (module-level annotations)."""
        if 'warnings' not in dataframe.columns:
            dataframe['warnings'] = pd.Series([None] * len(dataframe), dtype=object, index=dataframe.index)
        if 'deprecations' not in dataframe.columns:
            dataframe['deprecations'] = pd.Series([None] * len(dataframe), dtype=object, index=dataframe.index)

        dataframe['is_warning'] = dataframe['warnings'].apply(self._parse_and_check_json_array).astype(bool)
        dataframe['is_deprecation'] = dataframe['deprecations'].apply(self._parse_and_check_json_array).astype(bool)
        return dataframe

    def _filter_and_select_columns(self, dataframe):
        """Filter to rows with required fields and keep only needed columns."""
        dataframe = dataframe[
            dataframe['module_name'].notna()
            & dataframe['host_id'].notna()
            & dataframe['playbook'].notna()
            & dataframe['job_id'].notna()
            & (dataframe['module_name'].str.strip() != '')
            & (dataframe['playbook'].str.strip() != '')
        ]

        if 'ansible_version' not in dataframe.columns:
            dataframe['ansible_version'] = None
        if 'event_data_length' not in dataframe.columns:
            dataframe['event_data_length'] = 0

        columns_to_keep = [
            'job_id',
            'module_name',
            'playbook',
            'collection_name',
            'collection_source',
            'role',
            'job_failed',
            'job_started',
            'job_duration_seconds',
            'job_waiting_time_seconds',
            'event',
            'ignore_errors',
            'is_warning',
            'is_deprecation',
            'ansible_version',
            'event_data_length',
        ]
        return dataframe[columns_to_keep]

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    # Runner event types counted via precomputed int8 flag columns (sum in groupby).
    # is_ignore_errors is a combined counter across all failed-variant events;
    # see class docstring for why it isn't split per event type.
    _RUNNER_EVENT_FLAG_COLS = (
        'is_runner_on_ok',
        'is_runner_on_failed',
        'is_runner_on_unreachable',
        'is_runner_on_async_ok',
        'is_runner_on_async_failed',
        'is_runner_item_on_ok',
        'is_runner_item_on_failed',
        'is_runner_retry',
        'is_ignore_errors',
    )

    # Event types for which ignore_errors is meaningful (task-failure variants).
    _FAILED_EVENTS = frozenset(['runner_on_failed', 'runner_on_async_failed', 'runner_item_on_failed'])

    @staticmethod
    def _add_aggregation_columns(dataframe):
        """Precompute columns so groupby can use vectorized sum/nunique instead of lambdas.

        Runner event counters become int8 flags (0/1). Job success/failure counts use
        masked job_id columns so ``nunique`` ignores NaN rows for the other outcome.
        """
        event = dataframe['event']
        ignore_errors = dataframe['ignore_errors'].fillna(False).astype(bool)
        job_failed = dataframe['job_failed'].fillna(False).astype(bool)

        flags = {
            'is_runner_on_ok': event == 'runner_on_ok',
            'is_runner_on_failed': event == 'runner_on_failed',
            'is_runner_on_unreachable': event == 'runner_on_unreachable',
            'is_runner_on_async_ok': event == 'runner_on_async_ok',
            'is_runner_on_async_failed': event == 'runner_on_async_failed',
            'is_runner_item_on_ok': event == 'runner_item_on_ok',
            'is_runner_item_on_failed': event == 'runner_item_on_failed',
            'is_runner_retry': event == 'runner_retry',
            # Best-effort: undercounts ignored runner_on_async_failed events,
            # since ansible-core exposes no ignore_errors signal for them (see docstring).
            'is_ignore_errors': event.isin(EventModulesAnonymizedRollup._FAILED_EVENTS) & ignore_errors,
        }
        # int8 sum is cheaper than bool sum across many groupby aggregations
        flags = {name: series.astype('int8') for name, series in flags.items()}

        job_id = dataframe['job_id']
        return dataframe.assign(
            **flags,
            job_id_successful=job_id.where(~job_failed),
            job_id_failed=job_id.where(job_failed),
        )

    def _get_common_aggregation(self, dataframe):
        """Return aggregation spec for groupby calls over the event-level dataframe.

        Per-job metrics (counts, durations) are computed over unique job_ids within
        each group to avoid inflating counts when a job contributes many events.
        Event-level metrics sum precomputed flag columns — one increment per event.
        """

        def _unique_job_sum(col_name):
            col = dataframe[col_name]

            def agg(x):
                return col.loc[x[~x.duplicated()].index].sum()

            return agg

        def _unique_job_na_count(col_name):
            col = dataframe[col_name]

            def agg(x):
                return col.loc[x[~x.duplicated()].index].isna().sum()

            return agg

        aggregation = {
            'jobs_total': ('job_id', 'nunique'),
            # nunique skips NaN, so masked job_id columns replace per-group lambdas
            'jobs_successful_total': ('job_id_successful', 'nunique'),
            'jobs_failed_total': ('job_id_failed', 'nunique'),
            'jobs_duration_total_seconds': ('job_id', _unique_job_sum('job_duration_seconds')),
            'jobs_waiting_time_total_seconds': ('job_id', _unique_job_sum('job_waiting_time_seconds')),
            'jobs_never_started_total': ('job_id', _unique_job_na_count('job_started')),
            'jobs_successful_duration_total_seconds': ('job_id', _unique_job_sum('jobs_successful_duration_total_seconds')),
            'jobs_failed_duration_total_seconds': ('job_id', _unique_job_sum('jobs_failed_duration_total_seconds')),
            # Module-level annotations (from event_data.res, distinct from top-level warning events)
            'warnings_total': ('is_warning', 'sum'),
            'deprecations_total': ('is_deprecation', 'sum'),
            'collected_events_total': ('event', 'size'),
            'event_data_size_total': ('event_data_length', 'sum'),
            'ansible_versions': ('ansible_version', lambda x: set(x.dropna())),
        }

        # Map is_runner_on_ok -> runner_on_ok_total, etc.
        for flag_col in self._RUNNER_EVENT_FLAG_COLS:
            counter_name = flag_col[len('is_') :] + '_total'
            aggregation[counter_name] = (flag_col, 'sum')

        return aggregation

    def _compute_all_stats(self, dataframe):
        """Compute module_stats, collection_stats, and role_stats from the event dataframe."""
        dataframe = self._add_aggregation_columns(dataframe)
        common_aggregation = self._get_common_aggregation(dataframe)

        module_stats = dataframe.groupby(['module_name', 'collection_name', 'collection_source'], as_index=False, observed=True).agg(
            **common_aggregation
        )

        collection_stats = dataframe.groupby(['collection_name', 'collection_source'], as_index=False, observed=True).agg(**common_aggregation)

        dataframe['role_collection_name'] = dataframe['role'].astype(str).apply(lambda x: extract_collection_name(x) if x and x != 'nan' else None)
        role_collection_source_str = dataframe['role_collection_name'].astype(str).map(self.collections)
        dataframe['role_collection_source'] = role_collection_source_str.fillna('Custom')

        # Only group events that have an explicit role.  dropna=False is needed
        # so that one-dot roles (e.g. local.cleanup_role) whose collection_name
        # is None are kept; without it the default dropna=True would silently
        # discard them because role_collection_name is None.
        role_df = dataframe[dataframe['role'].notna()]
        role_stats = role_df.groupby(['role', 'role_collection_name', 'role_collection_source'], as_index=False, observed=True, dropna=False).agg(
            **common_aggregation
        )
        role_stats = role_stats.rename(columns={'role_collection_name': 'collection_name', 'role_collection_source': 'collection_source'})

        return module_stats, collection_stats, role_stats

    def _compute_unique_metadata(self, dataframe):
        """Compute unique_modules and modules_per_playbook."""
        unique_modules = sorted(dataframe['module_name'].dropna().unique())

        modules_per_playbook = {}
        for playbook in dataframe['playbook'].dropna().unique():
            modules_in_playbook = sorted(dataframe[dataframe['playbook'] == playbook]['module_name'].dropna().unique())
            modules_per_playbook[playbook] = modules_in_playbook

        return unique_modules, modules_per_playbook

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _convert_set_or_list_to_sorted_list(value):
        """Convert set or list to sorted list, return empty list for other types."""
        if isinstance(value, set):
            return sorted(list(value))
        if isinstance(value, list):
            return value
        return []

    def _convert_list_columns_to_json_format(self, dataframe, column_name):
        """Convert a list/set column in dataframe to JSON-compatible sorted list format."""
        if dataframe.empty or column_name not in dataframe.columns:
            return
        dataframe[column_name] = dataframe[column_name].apply(self._convert_set_or_list_to_sorted_list)

    def _convert_categorical_columns_to_string(self, dataframe):
        """Convert categorical columns to string type for JSON serialization."""
        if dataframe.empty:
            return
        for col in ['module_name', 'collection_name', 'collection_source', 'role']:
            if col in dataframe.columns and dataframe[col].dtype.name == 'category':
                dataframe[col] = dataframe[col].astype(str)

    def _convert_dataframe_to_json_records(self, dataframe):
        """Convert dataframe to JSON records format, return empty list if dataframe is empty."""
        if dataframe.empty:
            return []
        return dataframe.to_dict(orient='records')

    def _convert_stats_to_json(self, module_stats, collection_stats, role_stats):
        """Convert stats dataframes to JSON format."""
        for df in [module_stats, collection_stats, role_stats]:
            self._convert_list_columns_to_json_format(df, 'ansible_versions')
            self._convert_categorical_columns_to_string(df)

        module_stats_json = self._convert_dataframe_to_json_records(module_stats)
        collection_stats_json = self._convert_dataframe_to_json_records(collection_stats)
        role_stats_json = self._convert_dataframe_to_json_records(role_stats)

        return module_stats_json, collection_stats_json, role_stats_json

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def prepare(self, dataframe):
        """Prepare dataframe for aggregation by filtering, transforming, and computing statistics."""
        if dataframe is None:
            dataframe = pd.DataFrame()

        dataframe = self._convert_id_columns_to_strings(dataframe)
        dataframe = self._ensure_event_data_length(dataframe)

        collected_events_total, warnings_total, deprecations_total = self._count_initial_statistics(dataframe)

        # Empty / columnless frames must return before _filter_runner_events
        # (accessing dataframe['event'] raises KeyError when the column is absent).
        if dataframe.empty or 'event' not in dataframe.columns:
            return sanitize_json(
                {
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                    'module_stats': [],
                    'collection_stats': [],
                    'role_stats': [],
                    'unique_modules': [],
                    'modules_per_playbook': {},
                }
            )

        # Module/collection/role path uses runner events only.
        dataframe = self._filter_runner_events(dataframe)
        dataframe = self._prepare_basic_columns(dataframe)
        dataframe = self._extract_collection_info(dataframe)
        dataframe = self._compute_job_metrics(dataframe)
        dataframe = self._parse_warnings_deprecations(dataframe)
        dataframe = self._filter_and_select_columns(dataframe)

        if dataframe.empty:
            return sanitize_json(
                {
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                    'module_stats': [],
                    'collection_stats': [],
                    'role_stats': [],
                    'unique_modules': [],
                    'modules_per_playbook': {},
                }
            )

        dataframe = dataframe.assign(
            jobs_successful_duration_total_seconds=lambda x: x['job_duration_seconds'].where(~x['job_failed'], 0),
            jobs_failed_duration_total_seconds=lambda x: x['job_duration_seconds'].where(x['job_failed'], 0),
        )

        module_stats, collection_stats, role_stats = self._compute_all_stats(dataframe)
        module_stats_json, collection_stats_json, role_stats_json = self._convert_stats_to_json(module_stats, collection_stats, role_stats)
        unique_modules, modules_per_playbook = self._compute_unique_metadata(dataframe)

        result = {
            'collected_events_total': collected_events_total,
            'warnings_total': warnings_total,
            'deprecations_total': deprecations_total,
            'module_stats': module_stats_json,
            'collection_stats': collection_stats_json,
            'role_stats': role_stats_json,
            'unique_modules': unique_modules,
            'modules_per_playbook': modules_per_playbook,
        }

        return sanitize_json(result)

    def base(self, data):
        """
        Produce the final JSON report from aggregated event statistics.

        Top-level output:
            modules_used_to_automate_total   - distinct module count
            modules_used_per_playbook_total  - distinct modules per playbook
            module_stats                     - per-module stats (see _NUMERIC_COLS)
            collection_stats                 - per-collection stats
            role_stats                       - per-role stats
            collected_events_total           - all raw events before filtering
            warnings_total                   - top-level warning events
            deprecations_total               - top-level deprecated events

        data is a dict with already-aggregated JSON structures from prepare() and merge().
        """
        if data is None:
            return {
                'json': {
                    'collected_events_total': 0,
                    'warnings_total': 0,
                    'deprecations_total': 0,
                },
            }

        collected_events_total = data.get('collected_events_total', 0)
        warnings_total = data.get('warnings_total', 0)
        deprecations_total = data.get('deprecations_total', 0)
        module_stats = data.get('module_stats', [])
        collection_stats = data.get('collection_stats', [])
        role_stats = data.get('role_stats', [])
        unique_modules = data.get('unique_modules', [])
        modules_per_playbook = data.get('modules_per_playbook', {})

        if not module_stats and not collection_stats and not role_stats:
            return {
                'json': {
                    'collected_events_total': collected_events_total,
                    'warnings_total': warnings_total,
                    'deprecations_total': deprecations_total,
                },
            }

        # Drop host_ids and rename module_name/collection_name for Segment compatibility.
        for stats_list in [module_stats, collection_stats, role_stats]:
            for item in stats_list:
                _normalize_stats_item(item)

        modules_used_to_automate_total = len(unique_modules)
        modules_used_per_playbook_total = {
            playbook: len(module_list) if isinstance(module_list, list) else module_list for playbook, module_list in modules_per_playbook.items()
        }

        json_data = {
            'modules_used_to_automate_total': modules_used_to_automate_total,
            'modules_used_per_playbook_total': modules_used_per_playbook_total,
            'module_stats': module_stats,
            'collection_stats': collection_stats,
            'role_stats': role_stats,
            'collected_events_total': collected_events_total,
            'warnings_total': warnings_total,
            'deprecations_total': deprecations_total,
        }

        return {'json': json_data}
