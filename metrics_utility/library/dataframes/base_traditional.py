import json

from functools import reduce
from itertools import chain

import pandas as pd

from metrics_utility.library.dataframes.base_dataframe import BaseDataframe


# a dataframe class with logic for merges based on lists of indexes and merge operations
# used by DataframeMainJobevent, DataframeMainHost and DataframeJobHostSummary
class BaseTraditional(BaseDataframe):
    def cast_dataframe(self, df):
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
        for col in columns:
            if operations.get(col) == 'min':
                df[col] = df[[f'{col}_x', f'{col}_y']].min(axis=1)
            elif operations.get(col) == 'max':
                df[col] = df[[f'{col}_x', f'{col}_y']].max(axis=1)
            elif operations.get(col) == 'combine_set':
                df[col] = df.apply(lambda row: combine_set(row.get(f'{col}_x'), row.get(f'{col}_y')), axis=1)
            elif operations.get(col) == 'combine_json':
                df[col] = df.apply(lambda row: combine_json(row.get(f'{col}_x'), row.get(f'{col}_y')), axis=1)
            elif operations.get(col) == 'combine_json_values':
                df[col] = df.apply(lambda row: combine_json_values(row.get(f'{col}_x'), row.get(f'{col}_y')), axis=1)
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


def merge_sets(x):
    return set().union(*x)


def merge_setdicts(x):
    return reduce(combine_json_values, x, {})


def parse_json_array(x):
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


# Helper function to parse a JSON string or return the dict if it's already a dict.
def parse_json(val):
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}  # Return empty dict if parsing fails.
    elif isinstance(val, dict):
        return val
    return {}


# Function to merge a list of JSON values into a dict mapping each key to a set of non-null/non-empty values.
def merge_json_sets(json_values):
    merged = {}
    for val in json_values:
        d = parse_json(val)
        if isinstance(d, dict):
            for key, value in d.items():
                # Ignore null (None) or empty string values.
                # We also want to ignore NA value used when facts are not available
                if value is not None and value != '' and value != 'NA':
                    if isinstance(value, set):
                        merged.setdefault(key, set()).update(value)
                    else:
                        merged.setdefault(key, set()).add(value)
    return merged


# Function to merge array type columns getting a unique set back
def merge_arrays(values):
    # Filter out None values
    valid_events = [e for e in values if e is not None]
    # Flatten the list of lists and extract unique events
    unique = set(chain.from_iterable(valid_events))
    return list(unique)
