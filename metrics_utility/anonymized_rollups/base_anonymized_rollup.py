"""Base class for all anonymized rollup processors."""

import pandas as pd


class BaseAnonymizedRollup:
    """Base class for all anonymized rollup processors.

    Subclasses implement ``prepare``, ``merge``, and ``base`` to define how
    raw collector data is aggregated into a JSON structure suitable for
    inclusion in the anonymized daily report.
    """

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
