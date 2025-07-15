import datetime
import logging

from functools import reduce

import pandas as pd

from dateutil.relativedelta import relativedelta

from metrics_utility.automation_controller_billing.helpers import merge_arrays, merge_json_sets


logger = logging.getLogger(__name__)


def granularity_cast(date, granularity):
    if granularity == 'monthly':
        return date.replace(day=1)
    elif granularity == 'yearly':
        return date.replace(month=1, day=1)
    else:
        return date


def list_dates(start_date, end_date, granularity):
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


# For JSON/dict columns: update one dict with the other (later values overwrite earlier ones)
def combine_json(json1, json2):
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
    return set().union(*x)


def merge_setdicts(x):
    return reduce(combine_json_values, x, {})


# Helper function to combine two JSON values.
# For each key, it builds a set of non-null, non-empty values from both inputs.
def combine_json_values(val1, val2):
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
    def __init__(self, extractor, month, extra_params):
        self.logger = logger

        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

    def build_dataframe(self):
        pass

    def dates(self):
        if self.extra_params.get('since_date') is not None:
            beginning_of_the_month = self.extra_params.get('since_date')
            end_of_the_month = self.extra_params.get('until_date')
        else:
            beginning_of_the_month = self.month.replace(day=1)
            end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)

        dates_list = list_dates(start_date=beginning_of_the_month, end_date=end_of_the_month, granularity='daily')
        return dates_list

    def cast_dataframe(self, df, types):
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

    def summarize_merged_dataframes(self, df, columns, operations={}):
        df_copy = df.copy()

        for col in columns:
            col_x = f'{col}_x'
            col_y = f'{col}_y'

            operation_type = operations.get(col, 'sum')

            # Handle column existence before any operation to avoid Type and Key errors
            if col_x in df_copy.columns and col_y in df_copy.columns:
                if operation_type == 'min':
                    df_copy[col] = df_copy[[col_x, col_y]].min(axis=1)
                elif operation_type == 'max':
                    df_copy[col] = df_copy[[col_x, col_y]].max(axis=1)
                elif operation_type == 'combine_set':
                    if col == 'events':
                        def safe_merge_arrays(row):
                            items = [row.get(col_x), row.get(col_y)]
                            valid_items = [item for item in items if not pd.isnull(item) and item is not None]
                            return merge_arrays(valid_items) if valid_items else []
                        df_copy[col] = df_copy.apply(safe_merge_arrays, axis=1)
                    elif col == 'managed_node_types_set':
                        def safe_merge_sets(row):
                            items = [row.get(col_x), row.get(col_y)]
                            valid_items = [item for item in items if not pd.isnull(item) and item is not None]
                            return merge_sets(valid_items) if valid_items else set()
                        df_copy[col] = df_copy.apply(safe_merge_sets, axis=1)
                    else:
                        pass  # Fallback for other combine_set types if any

                elif operation_type == 'combine_json_values':
                    if col == 'facts' or col == 'canonical_facts':
                        def safe_merge_json_sets(row):
                            items = [row.get(col_x), row.get(col_y)]
                            valid_items = [item for item in items if not pd.isnull(item) and item is not None]
                            return merge_json_sets(valid_items) if valid_items else {}
                        df_copy[col] = df_copy.apply(safe_merge_json_sets, axis=1)
                    else:
                        pass  # Fallback for other combine_json_values types if any

                elif operation_type == 'first_non_null':
                    df_copy[col] = df_copy[col_x].fillna(df_copy[col_y])
                else:
                    df_copy[col_x] = pd.to_numeric(df_copy[col_x], errors='coerce').fillna(0)
                    df_copy[col_y] = pd.to_numeric(df_copy[col_y], errors='coerce').fillna(0)
                    df_copy[col] = df_copy[col_x] + df_copy[col_y]

                # Conditionally delete _x and _y columns
                if col_x in df_copy.columns:
                    del df_copy[col_x]
                if col_y in df_copy.columns:
                    del df_copy[col_y]
            # Handle cases where only one of _x or _y exists, or neither
            elif col_x in df_copy.columns:
                df_copy[col] = df_copy[col_x]
                del df_copy[col_x]
            elif col_y in df_copy.columns:
                df_copy[col] = df_copy[col_y]
                del df_copy[col_y]
            else:
                # If neither _x nor _y exists and 'col' wasn't created, initialize it.
                if col not in df_copy.columns:
                    df_copy[col] = None

        return df_copy

    def empty(self):
        return pd.DataFrame(columns=self.unique_index_columns() + self.data_columns())

    # Multipart collection, merge the dataframes and sum counts
    def merge(self, rollup, new_group):
        if rollup is None:
            return new_group

        rollup = pd.merge(rollup.loc[:,], new_group.loc[:,], on=self.unique_index_columns(), how='outer')
        rollup = self.summarize_merged_dataframes(rollup, self.data_columns(), operations=self.operations())
        return self.cast_dataframe(rollup, self.cast_types())

    def dedup(self, dataframe, hostname_mapping=None):
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
