import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.base import Base


# dataframe for data_collection_status
class DataframeCollectionStatus(Base):
    def build_dataframe(self):
        # all-rows dataframe, no aggregation
        dataframe = None

        for date in self.dates():
            # collections=None because data_collection_status is in every tarball, and none of them are named after it
            for data in self.extractor.iter_batches(date=date, collections=None, optional=['data_collection_status']):
                batch = data['data_collection_status']
                if batch.empty:
                    continue

                # Merge batches
                if dataframe is None:
                    dataframe = batch
                else:
                    dataframe = pd.concat([dataframe, batch], ignore_index=True)

        if dataframe is None or dataframe.empty:
            return None

        dataframe['collection_start_timestamp'] = pd.to_datetime(dataframe['collection_start_timestamp'], format='ISO8601').dt.tz_localize(None)
        dataframe['since'] = pd.to_datetime(dataframe['since'], format='ISO8601').dt.tz_localize(None)
        dataframe['until'] = pd.to_datetime(dataframe['until'], format='ISO8601').dt.tz_localize(None)

        return dataframe
