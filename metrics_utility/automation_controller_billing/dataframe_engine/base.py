"""Base dataframe engine utilities and base class for billing dataframe processors."""

import datetime

from functools import reduce

import pandas as pd

from dateutil.relativedelta import relativedelta


def granularity_cast(date, granularity):
    """Truncate *date* to the start of its month or year according to *granularity*.

    Args:
        date: A :class:`datetime.date` or :class:`datetime.datetime` instance.
        granularity: One of ``'daily'``, ``'monthly'``, or ``'yearly'``.

    Returns:
        The (potentially truncated) date with day/month reset where appropriate.
    """
    if granularity == 'monthly':
        return date.replace(day=1)
    elif granularity == 'yearly':
        return date.replace(month=1, day=1)
    else:
        return date


def list_dates(start_date, end_date, granularity):
    """Generate a list of period-start dates between *start_date* and *end_date* (inclusive).

    Args:
        start_date: Beginning of the range (date or datetime).
        end_date: End of the range (date or datetime).
        granularity: Step size — one of ``'daily'``, ``'monthly'``, or ``'yearly'``.

    Returns:
        List of dates at each granularity boundary from *start_date* through *end_date*.
    """
    # Given start date and end date, return list of dates in the given granularity
    # e.g. for daily it is a list of days withing the interval, for monthly it is a
    # list of months withing the interval, etc.
    start_date = granularity_cast(start_date, granularity)
    end_date = granularity_cast(end_date, granularity)

    dates_arr = []
    while start_date < end_date:
        dates_arr.append(start_date)

        if granularity == 'monthly':
            start_date += relativedelta(months=+1)
        elif granularity == 'yearly':
            start_date += relativedelta(years=+1)
        else:
            start_date += datetime.timedelta(days=1)

    dates_arr.append(end_date)

    return dates_arr


def combine_json(json1, json2):
    """Merge two dicts, with values from *json2* overwriting those in *json1*.

    Args:
        json1: First dict (may be None or non-dict, treated as empty).
        json2: Second dict (may be None or non-dict, treated as empty).

    Returns:
        A new merged dict.
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


def merge_sets(x):
    """Return the union of an iterable of sets.

    Args:
        x: Iterable of sets.

    Returns:
        A single set containing all unique elements.
    """
    return set().union(*x)


def merge_setdicts(x):
    """Reduce an iterable of value-dicts into one dict using :func:`combine_json_values`.

    Args:
        x: Iterable of dicts where each value may be a set or scalar.

    Returns:
        A merged dict mapping each key to a set of its distinct non-empty values.
    """
    return reduce(combine_json_values, x, {})


def combine_json_values(val1, val2):
    """Combine two value-dicts by building a set of non-null/non-empty values per key.

    Args:
        val1: First dict (values may be scalars or sets).
        val2: Second dict (values may be scalars or sets).

    Returns:
        Dict mapping each key to a set of its distinct non-empty values from both dicts.
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


class Base:
    """Abstract base class for billing dataframe engines.

    Subclasses implement :meth:`build_dataframe` to extract and aggregate raw
    tarball data into a pandas DataFrame suitable for report generation.
    Common helpers (``merge``, ``dedup``, ``dates``, ``cast_dataframe``, etc.)
    are provided here.
    """

    def __init__(self, extractor, month, extra_params):
        """Initialise the dataframe engine.

        Args:
            extractor: An extractor instance (e.g. ``ExtractorDirectory``) used
                to iterate over tarball batches.
            month: A :class:`datetime.date` representing the start of the
                reporting month.
            extra_params: Dict of configuration parameters (report_type,
                deduplicator, ship_path, etc.).
        """
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

    def build_dataframe(self):
        """Build and return the aggregated DataFrame for the reporting period.

        Must be implemented by subclasses.
        """

    def dates(self):
        """Return the list of daily dates to iterate over for the reporting window.

        Uses ``since_date``/``until_date`` from ``extra_params`` when set;
        otherwise defaults to the full calendar month defined by ``self.month``.

        Returns:
            List of :class:`datetime.date` objects, one per day in the window.
        """
        if self.extra_params.get('since_date') is not None:
            beginning_of_the_month = self.extra_params.get('since_date')
            end_of_the_month = self.extra_params.get('until_date')
        else:
            beginning_of_the_month = self.month.replace(day=1)
            end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)

        dates_list = list_dates(start_date=beginning_of_the_month, end_date=end_of_the_month, granularity='daily')
        return dates_list

    def cast_dataframe(self, df, types):
        """Cast DataFrame columns and index to the specified types.

        Args:
            df: pandas DataFrame to cast (may have a single or composite index).
            types: Dict mapping column names to target dtypes.

        Returns:
            The DataFrame with index and column types applied.
        """
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

    def summarize_merged_dataframes(self, df, columns, operations=None):
        """Reduce paired ``_x``/``_y`` suffix columns produced by an outer merge.

        For each column in *columns*, the ``_x`` and ``_y`` variants are combined
        using the operation specified in *operations* (defaulting to element-wise
        sum), and the originals are deleted.

        Args:
            df: DataFrame after ``pd.merge(..., how='outer')``.
            columns: List of base column names to reconcile.
            operations: Dict mapping column names to merge strategies
                (``'min'``, ``'max'``, ``'combine_set'``, ``'combine_json'``,
                ``'combine_json_values'``; default is summation).

        Returns:
            The DataFrame with reconciled columns (in-place).
        """
        if operations is None:
            operations = {}
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
        """Return an empty DataFrame with the expected column structure.

        Returns:
            An empty pandas DataFrame with index and data columns defined by
            the subclass.
        """
        return pd.DataFrame(columns=self.unique_index_columns() + self.data_columns())

    # Multipart collection, merge the dataframes and sum counts
    def merge(self, rollup, new_group):
        """Merge a new batch DataFrame into the accumulated rollup.

        On the first call (*rollup* is None) the new group is returned as-is.
        Subsequent calls outer-join and summarise using the subclass operations.

        Args:
            rollup: Accumulated rollup DataFrame (or None on the first call).
            new_group: Freshly aggregated batch DataFrame.

        Returns:
            The merged and type-cast rollup DataFrame.
        """
        if rollup is None:
            return new_group

        rollup = pd.merge(rollup.loc[:,], new_group.loc[:,], on=self.unique_index_columns(), how='outer')
        rollup = self.summarize_merged_dataframes(rollup, self.data_columns(), operations=self.operations())
        return self.cast_dataframe(rollup, self.cast_types())

    def dedup(self, dataframe, hostname_mapping=None):
        """Apply hostname deduplication to a rollup DataFrame.

        When *hostname_mapping* is provided, ``host_name`` values are replaced
        with their canonical equivalents and the rows are re-aggregated.

        Args:
            dataframe: Rollup DataFrame to deduplicate.
            hostname_mapping: Optional dict mapping original hostnames to their
                canonical hostname.

        Returns:
            Deduplicated (and possibly re-aggregated) DataFrame, or
            :meth:`empty` if the input is empty.
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
        df_grouped = self.cast_dataframe(df_grouped, self.cast_types())
        return df_grouped.reset_index()

    @staticmethod
    def unique_index_columns():
        pass

    @staticmethod
    def data_columns():
        pass

    @staticmethod
    def cast_types():
        pass

    @staticmethod
    def operations():
        pass
