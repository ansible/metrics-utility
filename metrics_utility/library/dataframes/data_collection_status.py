import pandas as pd

from metrics_utility.library.dataframes.base_dataframe import BaseDataframe


class DataframeDataCollectionStatus(BaseDataframe):
    TARBALL_NAMES = ['data_collection_status.csv']

    def postprocess(self, df):
        df['collection_start_timestamp'] = pd.to_datetime(df['collection_start_timestamp'], format='ISO8601').dt.tz_localize(None)
        df['since'] = pd.to_datetime(df['since'], format='ISO8601').dt.tz_localize(None)
        df['until'] = pd.to_datetime(df['until'], format='ISO8601').dt.tz_localize(None)

        # not super, no need to reset index
        return df
