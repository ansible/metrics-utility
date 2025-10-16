import glob
import tarfile

import pandas as pd

from pandas import DataFrame


# loads data from tarballs located in base_path/data/year/month/day/*{collector_name}*.tar.gz
# inside tarball is file named {collector_name}.csv
# this goes to dataframe, then filter_function is applied to the dataframe
# all result dataframes are concatenated into one dataframe
def load_data(collection_name: str, base_path: str, year: int, month: int, day: int, filter_function: callable) -> DataFrame:
    # list all tarballs in base_path/data/year/month/day/*{collector_name}*.tar.gz
    tarballs = glob.glob(f'{base_path}/data/{year}/{month:02d}/{day:02d}/*{collection_name}*.tar.gz')

    # load each tarball into a dataframe
    dataframes = []
    for tarball in tarballs:
        with tarfile.open(tarball, 'r') as tar:
            for member in tar.getmembers():
                if member.name.endswith(f'{collection_name}.csv'):
                    df = pd.read_csv(tar.extractfile(member))
                    # filter df is function exist
                    if filter_function:
                        df = filter_function(df)
                    dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()
    return pd.concat(dataframes, ignore_index=True)
