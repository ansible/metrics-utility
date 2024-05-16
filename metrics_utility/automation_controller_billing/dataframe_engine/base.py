import logging
import datetime
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def granularity_cast(date, granularity):
    if granularity == "monthly":
        return date.replace(day=1)
    elif granularity == "yearly":
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

        if granularity == "monthly":
            start_date += relativedelta(months=+1)
        elif granularity == "yearly":
            start_date += relativedelta(years=+1)
        else:
            start_date += datetime.timedelta(days=1)

    dates_arr.append(end_date)

    return dates_arr

class Base:
    LOG_PREFIX = "[AAPBillingReport] "

    def __init__(self, extractor, month, extra_params):
        self.logger = logger

        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

    def build_dataframe(self):
        pass

    def cast_dataframe(self, df, types):
        levels = []
        for index, level in enumerate(df.index.levels):
            casted_level = df.index.levels[index].astype(object)
            levels.append(casted_level)

        df.index = df.index.set_levels(levels)

        return df.astype(types)

    def summarize_merged_dataframes(self, df, columns):
        for col in columns:
            df[col] = df[[f"{col}_x", f"{col}_y"]].sum(axis=1)
            del df[f"{col}_x"]
            del df[f"{col}_y"]
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
