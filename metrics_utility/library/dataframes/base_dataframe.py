"""Base dataframe class and I/O helpers for billing library dataframes."""

import pandas as pd


def from_csv(f):
    """Read a CSV file (path or file-like) into a pandas DataFrame.

    Args:
        f: File path string or file-like object.

    Returns:
        pandas DataFrame.
    """
    return pd.read_csv(f)


def from_json(f):
    """Read a JSON file or string into a pandas DataFrame.

    Args:
        f: File path, URL, or JSON string.

    Returns:
        pandas DataFrame.
    """
    return pd.read_json(f)


def from_parquet(f):
    """Read a Parquet file into a pandas DataFrame.

    Args:
        f: File path or file-like object.

    Returns:
        pandas DataFrame.
    """
    return pd.read_parquet(f)


# these return the file content if f=None
def to_csv(df, f=None):
    """Serialise *df* to CSV, writing to *f* or returning a string.

    Args:
        df: pandas DataFrame to serialise.
        f: Optional file path or file-like object.

    Returns:
        CSV string when *f* is None; otherwise None (writes to *f*).
    """
    return df.to_csv(f, index=False)


def to_json(df, f=None):
    """Serialise *df* to JSON, writing to *f* or returning a string.

    Args:
        df: pandas DataFrame to serialise.
        f: Optional file path or file-like object.

    Returns:
        JSON string when *f* is None; otherwise None (writes to *f*).
    """
    return df.to_json(f)


def to_parquet(df, f=None):
    """Serialise *df* to Parquet format, writing to *f*.

    Args:
        df: pandas DataFrame to serialise.
        f: Optional file path or file-like object.
    """
    return df.to_parquet(f)


# read_sql / to_sql need a sqlalchemy connection handle,
# we already have a read_sql_query in the form of copy_table
# so we might do to_sql manually with Django's connection too

# (raw csv) -> from_csv -> (pd.DataFrame) -> add_raw (= prepare -> group -> add_rollup) -> postprocess
# (parquet rollup) -> ... -> (pd.DataFrame) -> add_rollup -> regroup -> postprocess


class BaseDataframe:
    """Base class for library-layer billing dataframes.

    Manages an incremental rollup built from successive batches.  Subclasses
    override :meth:`prepare`, :meth:`group`, :meth:`merge`, and :meth:`regroup`
    to implement data-type-specific aggregation logic.
    """

    def __init__(self):
        """Initialise with an empty rollup."""
        self.rollup = None

    def add_rollup(self, new_group):
        """Merge *new_group* into the accumulated rollup.

        Args:
            new_group: A pre-grouped DataFrame to merge into the current rollup.
        """
        if self.rollup is None:
            self.rollup = new_group
        else:
            self.rollup = self.merge(self.rollup, new_group)

    # a batch is either a dataframe (straight from `from_csv`), or a tuple of (dataframe(s), config dict)
    def add_raw(self, batch):
        """Prepare, group, and merge a raw data batch into the rollup.

        Args:
            batch: A raw DataFrame or tuple ``(dataframe(s), config_dict)``.
        """
        df = self.prepare(batch)
        if df is None:
            return

        group = self.group(df)
        self.add_rollup(group)

    def merge(self, old, new):
        """Merge *old* and *new* DataFrames into one (default: concat).

        Overridden by subclasses that need custom merge logic (e.g. outer-join
        and column summarisation).

        Args:
            old: Accumulated rollup DataFrame.
            new: Freshly grouped batch DataFrame.

        Returns:
            Merged DataFrame.
        """
        # merges old + new, returns the result
        # both are expected to be pre-grouped dataframes, if applicable
        # default to concat, overridden for complex merges
        return pd.concat([old, new], ignore_index=True)

    def prepare(self, df):
        """Prepare a raw batch for grouping. Override to add pre-processing.

        Args:
            df: Raw input DataFrame or batch tuple.

        Returns:
            Prepared DataFrame, or None to skip the batch.
        """
        return df

    def group(self, df):
        """Aggregate a prepared DataFrame. Override for aggregation logic.

        Args:
            df: Prepared DataFrame.

        Returns:
            Grouped (aggregated) DataFrame.
        """
        return df

    def regroup(self, df):
        """Re-aggregate a DataFrame after deduplication. Override if needed.

        Args:
            df: DataFrame to re-aggregate.

        Returns:
            Re-grouped DataFrame.
        """
        return df

    def empty(self):
        """Return an empty DataFrame with the expected schema.

        Returns:
            Empty pandas DataFrame.
        """
        # overriden where types are known
        return pd.DataFrame()

    def postprocess(self, df):
        """Apply final post-processing to the completed rollup.

        Args:
            df: Completed rollup DataFrame.

        Returns:
            Post-processed DataFrame (default: reset index).
        """
        return df.reset_index()

    def from_tarballs(self, batches):
        """Process an iterable of raw batches and build the accumulated rollup.

        Iterates over *batches*, calling :meth:`add_raw` for each, then
        applies :meth:`postprocess`.  On completion, ``self.rollup`` holds the
        final DataFrame (or None if no data).

        Args:
            batches: Iterable of raw batches (DataFrames or tuples).
        """
        # all-rows dataframe, no aggregation
        self.rollup = None

        for batch in batches:
            self.add_raw(batch)

        if self.rollup is None or self.rollup.empty:
            self.rollup = None
        else:
            self.rollup = self.postprocess(self.rollup)

    def to_csv(self, f=None):
        return to_csv(self.rollup, f)

    def to_json(self, f=None):
        return to_json(self.rollup, f)

    def to_parquet(self, f=None):
        return to_parquet(self.rollup, f)
