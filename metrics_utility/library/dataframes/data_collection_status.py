"""Dataframe for parsing and post-processing data_collection_status CSV files."""

import pandas as pd

from metrics_utility.library.dataframes.base_dataframe import BaseDataframe


class DataframeDataCollectionStatus(BaseDataframe):
    """Dataframe that reads data_collection_status CSV and coerces timestamp columns."""

    TARBALL_NAMES = ['data_collection_status.csv']

    def postprocess(self, df):
        """Convert ISO 8601 timestamp columns to naive datetimes.

        Args:
            df: Raw data_collection_status DataFrame.

        Returns:
            DataFrame with ``collection_start_timestamp``, ``since``, and
            ``until`` converted to timezone-naive datetimes.
        """
        df['collection_start_timestamp'] = pd.to_datetime(df['collection_start_timestamp'], format='ISO8601').dt.tz_localize(None)
        df['since'] = pd.to_datetime(df['since'], format='ISO8601').dt.tz_localize(None)
        df['until'] = pd.to_datetime(df['until'], format='ISO8601').dt.tz_localize(None)

        # not super, no need to reset index
        return df
