"""Base class for traditional (non-engine) billing dataframes and merge helpers."""

import json

from functools import reduce
from itertools import chain

import pandas as pd

from metrics_utility.library.dataframes.base_dataframe import BaseDataframe


# a dataframe class with logic for merges based on lists of indexes and merge operations
# used by DataframeMainJobevent, DataframeMainHost and DataframeJobHostSummary
class BaseTraditional(BaseDataframe):
    """Dataframe base class with outer-join merge logic and hostname deduplication.

    Used by :class:`DataframeJobHostSummary`, ``DataframeMainHost``, and
    ``DataframeMainJobevent``.  Subclasses define :meth:`unique_index_columns`,
    :meth:`data_columns`, :meth:`cast_types`, and :meth:`operations`.
    """

    def cast_dataframe(self, df):
        """Cast index and column types after an outer merge.

        Args:
            df: DataFrame with a single or composite index.

        Returns:
            Type-cast DataFrame.
        """
        types = self.cast_types()
        levels = []
        if len(self.unique_index_columns()) == 1:
            # Special behavior if the index is not composite, but only 1 column
            # Casting index field to object
            df.index = df.index.astype(object)
        else:
            # Composite index branch
            # Casting index field to object
            for index, _level in enumerate(df.index.levels):
                casted_level = df.index.levels[index].astype(object)
                levels.append(casted_level)

            df.index = df.index.set_levels(levels)

        return df.astype(types)

    def dedup(self, dataframe, hostname_mapping=None, **kwargs):
        """Deduplicate hosts by mapping hostnames to canonical values.

        Args:
            dataframe: DataFrame to deduplicate.
            hostname_mapping: Dict mapping original hostnames to canonical hostnames.
                If None or empty, the original DataFrame is returned unchanged.
            **kwargs: Ignored extra keyword arguments.

        Returns:
            Deduplicated DataFrame, or :meth:`empty` if *dataframe* is empty.
        """
        if dataframe is None or dataframe.empty:
            return self.empty()

        if not hostname_mapping:
            return dataframe

        # map hostnames to canonical value
        df = dataframe.copy()

        df['host_name'] = df['host_name'].map(hostname_mapping).fillna(df['host_name'])

        # multiple rows can now have the same hostname, regroup
        df_grouped = self.regroup(df)

        # cast types to match the table
        df_grouped = self.cast_dataframe(df_grouped)
        return df_grouped.reset_index()

    def summarize_merged_dataframes(self, df, columns, operations={}):
        """Reduce ``_x``/``_y`` suffix columns produced by an outer merge.

        Args:
            df: DataFrame after ``pd.merge(..., how='outer')``.
            columns: List of base column names to reconcile.
            operations: Dict mapping column names to merge strategies
                (``'min'``, ``'max'``, ``'combine_set'``, ``'combine_json'``,
                ``'combine_json_values'``; default is summation).

        Returns:
            The DataFrame with reconciled columns (in-place modifications).
        """
        for col in columns:
            if operations.get(col) == 'min':
                df[col] = df[[f'{col}_x', f'{col}_y']].min(axis=1)
            elif operations.get(col) == 'max':
                df[col] = df[[f'{col}_x', f'{col}_y']].max(axis=1)
            elif operations.get(col) == 'combine_set':
                df[col] = df.apply(lambda row, c=col: combine_set(row.get(f'{c}_x'), row.get(f'{c}_y')), axis=1)
            elif operations.get(col) == 'combine_json':
                df[col] = df.apply(lambda row, c=col: combine_json(row.get(f'{c}_x'), row.get(f'{c}_y')), axis=1)
            elif operations.get(col) == 'combine_json_values':
                df[col] = df.apply(lambda row, c=col: combine_json_values(row.get(f'{c}_x'), row.get(f'{c}_y')), axis=1)
            else:
                df[col] = df[[f'{col}_x', f'{col}_y']].sum(axis=1)
            del df[f'{col}_x']
            del df[f'{col}_y']
        return df

    def empty(self):
        return pd.DataFrame(columns=self.unique_index_columns() + self.data_columns())

    # Multipart collection, merge the dataframes and sum counts
    # used by BaseDataframe.add_rollup
    def merge(self, rollup, new_group):
        if rollup is None:
            return new_group

        rollup = pd.merge(rollup.loc[:,], new_group.loc[:,], on=self.unique_index_columns(), how='outer')
        rollup = self.summarize_merged_dataframes(rollup, self.data_columns(), operations=self.operations())
        rollup = self.cast_dataframe(rollup)
        return rollup

    @staticmethod
    def cast_types():
        pass

    @staticmethod
    def data_columns():
        pass

    @staticmethod
    def operations():
        pass

    @staticmethod
    def unique_index_columns():
        pass


def combine_json(json1, json2):
    """Merge two dicts, with values from *json2* overwriting those in *json1*.

    Args:
        json1: First dict (non-dict inputs are treated as empty).
        json2: Second dict (non-dict inputs are treated as empty).

    Returns:
        Merged dict.
    """
    merged = {}
    if isinstance(json1, dict):
        merged.update(json1)
    if isinstance(json2, dict):
        merged.update(json2)
    return merged


# For set columns: take the union of the two sets
def combine_set(set1, set2):
    """
    Combine two collections (set or list) into a single set of unique items.
    If an input is a list, it is first converted to a set.
    If an input is not a list or a set, it is treated as empty.
    """
    # Convert to set if input is a list; otherwise, if not a set, default to an empty set.
    if isinstance(set1, list):
        set1 = set(set1)
    elif not isinstance(set1, set):
        set1 = set()

    if isinstance(set2, list):
        set2 = set(set2)
    elif not isinstance(set2, set):
        set2 = set()

    # Return the union of both sets.
    return set1.union(set2)


def combine_json_values(val1, val2):
    """Combine two value-dicts by building a set of non-null/non-empty values per key.

    Args:
        val1: First dict (values may be scalars or sets).
        val2: Second dict (values may be scalars or sets).

    Returns:
        Dict mapping each key to a set of distinct non-empty values.
    """
    merged = {}
    for d in [val1, val2]:
        if isinstance(d, dict):
            for key, value in d.items():
                if value is not None and value != '':
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(value)
                    else:
                        merged.setdefault(key, set()).add(value)

    return merged


def merge_sets(x):
    """Return the union of an iterable of sets.

    Args:
        x: Iterable of sets.

    Returns:
        A single merged set.
    """
    return set().union(*x)


def merge_setdicts(x):
    """Reduce an iterable of value-dicts into one dict using :func:`combine_json_values`.

    Args:
        x: Iterable of dicts where each value may be a set or scalar.

    Returns:
        Merged dict mapping each key to a set of its distinct non-empty values.
    """
    return reduce(combine_json_values, x, {})


def parse_json_array(x):
    """Parse a JSON string as a list, returning an empty list on failure.

    Args:
        x: JSON string, or null/NaN value.

    Returns:
        Parsed list, or an empty list if *x* is null/NaN or not a JSON array.
    """
    if pd.isnull(x):
        return []
    try:
        parsed = json.loads(x)
        # Check if the parsed JSON object is a list (array)
        if isinstance(parsed, list):
            return parsed
        else:
            return []
    except json.JSONDecodeError:
        return []


def parse_json(val):
    """Parse a JSON string into a dict, or pass through a dict unchanged.

    Args:
        val: JSON-encoded string or existing dict.

    Returns:
        Parsed dict, or an empty dict if parsing fails or *val* is neither.
    """
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}  # Return empty dict if parsing fails.
    elif isinstance(val, dict):
        return val
    return {}


def merge_json_sets(json_values):
    """Merge a sequence of JSON dict values into a mapping of key → set of non-null values.

    Args:
        json_values: Iterable of JSON strings or dicts.

    Returns:
        Dict mapping each key to a set of its distinct non-empty values.
    """
    merged = {}
    for val in json_values:
        d = parse_json(val)
        if isinstance(d, dict):
            for key, value in d.items():
                # Ignore null (None) or empty string values.
                # We also want to ignore NA value used when facts are not available
                if value is not None and value not in {'', 'NA'}:
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(value)
                    else:
                        merged.setdefault(key, set()).add(value)
    return merged


def merge_arrays(values):
    """Flatten and deduplicate a sequence of lists into a single list of unique items.

    Args:
        values: Iterable of lists (None entries are ignored).

    Returns:
        List containing all unique non-None items from all input lists.
    """
    # Filter out None values
    valid_events = [e for e in values if e is not None]
    # Flatten the list of lists and extract unique events
    unique = set(chain.from_iterable(valid_events))
    return list(unique)
