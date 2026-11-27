"""Base class for all anonymized rollup processors."""

import pandas as pd


class BaseAnonymizedRollup:
    """Base class for all anonymized rollup processors.

    Subclasses implement ``prepare``, ``merge``, and ``base`` to define how
    raw collector data is aggregated into a JSON structure suitable for
    inclusion in the anonymized daily report.
    """

    # Columns summed when merging per-group stats entries.
    STATS_NUMERIC_COLUMNS = ()
    # Columns unioned into sorted lists when merging per-group stats entries.
    STATS_LIST_COLUMNS = ()

    def __init__(self, rollup_name: str):
        """Initialize the rollup with a name identifier.

        Args:
            rollup_name: Unique name for this rollup (used as a directory key when saving).
        """
        self.rollup_name = rollup_name
        self.collector_names = []

    # Merges two data (it dont have to be dataframes if overriden)
    # this is used in batch processing
    # where we need to merge partial rollup with current batch aggregation
    def merge(self, dataframe_all, dataframe_new):
        """Combine accumulated rollup data with a new batch.

        The default implementation concatenates two DataFrames.  Subclasses may
        override this method for non-DataFrame data structures or custom merge logic.

        Args:
            dataframe_all: The accumulated data so far (may be None on the first call).
            dataframe_new: The freshly prepared batch to merge in.

        Returns:
            The merged data (a concatenated DataFrame by default).
        """
        if dataframe_all is None:
            return dataframe_new

        return pd.concat([dataframe_all, dataframe_new], ignore_index=True)

    def _merge_stats_json(self, stats_all, stats_new, groupby_col, numeric_cols=None, list_cols=None):
        """Merge two stats JSON lists by summing numeric columns and unioning lists.

        Args:
            stats_all: Accumulated stats entries.
            stats_new: Newly prepared stats entries.
            groupby_col: Column whose value identifies matching entries.
            numeric_cols: Numeric columns to sum; defaults to ``STATS_NUMERIC_COLUMNS``.
            list_cols: List columns to union; defaults to ``STATS_LIST_COLUMNS``.
        """
        if not stats_all:
            return stats_new or []
        if not stats_new:
            return stats_all or []

        numeric_cols = self.STATS_NUMERIC_COLUMNS if numeric_cols is None else numeric_cols
        list_cols = self.STATS_LIST_COLUMNS if list_cols is None else list_cols

        # Create lookup dictionaries keyed by grouping column
        all_dict = {item.get(groupby_col): item.copy() for item in stats_all}
        new_dict = {item.get(groupby_col): item.copy() for item in stats_new}

        merged_list = []
        all_keys = set(all_dict.keys()) | set(new_dict.keys())

        for key in all_keys:
            item_all = all_dict.get(key, {})
            item_new = new_dict.get(key, {})
            merged_item = self._create_merged_item(item_all, item_new, numeric_cols, list_cols)
            if merged_item:
                merged_list.append(merged_item)

        return merged_list

    def _create_merged_item(self, item_all, item_new, numeric_cols, list_cols):
        """Create a merged item from two items, handling None/empty cases."""
        if not item_all and not item_new:
            return None

        merged_item = item_all.copy() if item_all else item_new.copy()

        if item_all and item_new:
            self._merge_numeric_columns(merged_item, item_all, item_new, numeric_cols)
            self._merge_extreme_columns(merged_item, item_all, item_new)
            self._merge_list_columns(merged_item, item_all, item_new, list_cols)

        self._recompute_totals(merged_item)

        return merged_item

    def _merge_numeric_columns(self, merged_item, item_all, item_new, numeric_cols):
        """Sum numeric columns from both items."""
        for col in numeric_cols:
            val_all = item_all.get(col) if item_all.get(col) is not None else 0
            val_new = item_new.get(col) if item_new.get(col) is not None else 0
            merged_item[col] = val_all + val_new

    def _merge_list_columns(self, merged_item, item_all, item_new, list_cols):
        """Union list columns from both items."""
        for col in list_cols:
            list_all = item_all.get(col) if item_all.get(col) is not None else []
            list_new = item_new.get(col) if item_new.get(col) is not None else []
            set_all = set(list_all) if isinstance(list_all, list) else set()
            set_new = set(list_new) if isinstance(list_new, list) else set()
            merged_item[col] = sorted(set_all.union(set_new))

    def _merge_extreme_columns(self, merged_item, item_all, item_new):
        """Hook for subclasses that merge max/min columns."""

    def _recompute_totals(self, merged_item):
        """Hook for subclasses that recompute totals from list columns."""

    def _convert_id_columns_to_strings(self, dataframe):
        """Convert ID columns to strings at the beginning of prepare().

        Converts numeric ID columns (id, job_id, host_id, job_remote_id) to strings
        to ensure consistent JSON serialization.
        """
        if dataframe.empty:
            return dataframe

        id_columns = ['id', 'job_id', 'host_id', 'job_remote_id']
        for col in id_columns:
            if col in dataframe.columns:
                # Convert numeric IDs to strings, preserving NaN values
                dataframe[col] = dataframe[col].apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) else x)

        return dataframe

    # takes raw data and computes aggregation
    # this works in batches, for example we are collecting every hour
    # this hourly data arrive into prepare, then it gets merged with partial rollup (initaly empty)
    def prepare(self, dataframe):
        """Transform a raw batch DataFrame into the intermediate rollup structure.

        Called once per hourly (or other interval) batch.  The result is later
        passed to ``merge`` to accumulate across batches.

        Args:
            dataframe: Raw pandas DataFrame from the collector.

        Returns:
            Transformed data (DataFrame or JSON-serialisable structure).
        """
        return dataframe

    # Base receive the full daily rollup and computes some final statistics for the day
    def base(self, dataframe):
        """Compute final daily statistics from the fully-merged rollup data.

        Args:
            dataframe: The accumulated rollup data produced by successive calls to
                ``merge`` across all batches for the day.

        Returns:
            An empty DataFrame by default; subclasses return a dict with a
            ``'json'`` key containing the final JSON payload.
        """
        return pd.DataFrame()


class SnapshotAnonymizedRollup(BaseAnonymizedRollup):
    """Base class for snapshot collectors that keep only the latest batch.

    Subclasses implement ``prepare`` and ``_empty_json``.
    """

    def merge(self, data_all, data_new):
        """For snapshot collectors, always pick new data (no merging needed)."""
        return data_new

    def _empty_json(self):
        """Return a fresh empty payload used when no data was collected."""
        raise NotImplementedError

    def base(self, data):
        """Return the data as-is, preparing it first if it is a DataFrame."""
        if data is None:
            return {'json': self._empty_json()}
        if isinstance(data, pd.DataFrame):
            data = self.prepare(data)
        return {'json': data}
