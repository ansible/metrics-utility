import datetime
import logging

from dateutil.relativedelta import relativedelta

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
    LOG_PREFIX = '[AAPBillingReport] '

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
            for index, level in enumerate(df.index.levels):
                casted_level = df.index.levels[index].astype(object)
                levels.append(casted_level)

            df.index = df.index.set_levels(levels)

        return df.astype(types)

    def summarize_merged_dataframes(self, df, columns, operations={}):
        for col in columns:
            if operations.get(col) == 'min':
                df[col] = df[[f'{col}_x', f'{col}_y']].min(axis=1)
            elif operations.get(col) == 'max':
                df[col] = df[[f'{col}_x', f'{col}_y']].max(axis=1)
            elif operations.get(col) == 'set_merge':
                df[col] = df.apply(lambda row: combine_set(row.get(f'{col}_x'), row.get(f'{col}_y')), axis=1)
            elif operations.get(col) == 'dict_merge':
                df[col] = df.apply(lambda row: combine_json(row.get(f'{col}_x'), row.get(f'{col}_y')), axis=1)
            elif operations.get(col) == 'dict_set_merge':
                df[col] = df.apply(lambda row: combine_json_values(row.get(f'{col}_x'), row.get(f'{col}_y')), axis=1)
            else:
                df[col] = df[[f'{col}_x', f'{col}_y']].sum(axis=1)
            del df[f'{col}_x']
            del df[f'{col}_y']
        return df

    @staticmethod
    def get_logger():
        return logging.getLogger(__name__)

    @staticmethod
    def unique_index_columns():
        pass

    @staticmethod
    def data_columns():
        pass

    @staticmethod
    def cast_types():
        pass
