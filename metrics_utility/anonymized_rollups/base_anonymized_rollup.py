import io
import json
import os
import tarfile

import pandas as pd


class BaseAnonymizedRollup:
    def __init__(self, rollup_name: str):
        self.rollup_name = rollup_name
        self.collector_names = []

    def merge(self, dataframe_all, dataframe_new):
        return pd.concat([dataframe_all, dataframe_new], ignore_index=True)

    def rollup(self, dataframe_all, dataframe_new):
        # not implemented in base class, return empty dataframe
        return pd.DataFrame()

    def prepare(self, dataframe):
        return dataframe

    def base(self, dataframe):
        return pd.DataFrame()

    def save_rollup(
        self, rollup_data: dict, base_path: str, year: int, month: int, day: int, save_csv: bool = False, save_parquet: bool = True
    ) -> None:
        # rollup data is dictionary
        # the dictionary can have those values:
        # scalar, list, pandas.Series, pandas.DataFrame
        # each dictionary key will be stored as separate file, with file name as key
        # file will be dataframe or json for rest of the values

        # file will be stored inside base_path/rollups/rollup_name/year/month/day

        # save_csv is for testing purposes only - so we can check content of files easily
        # save_parquet controls whether to save parquet files

        # make sure year is 4 digits, month is 2 digits, day is 2 digits
        year = str(year).zfill(4)
        month = str(month).zfill(2)
        day = str(day).zfill(2)
        rollup_path = os.path.join(base_path, 'rollups', str(year), str(month), str(day), self.rollup_name)

        os.makedirs(rollup_path, exist_ok=True)

        # Collect files in memory for tar archive
        tar_files = {}

        for key, value in rollup_data.items():
            filename = key + '_' + str(year) + '_' + str(month) + '_' + str(day)

            if isinstance(value, pd.DataFrame):
                if save_csv:
                    # Save CSV to tarball instead of filesystem
                    csv_buffer = io.StringIO()
                    value.to_csv(csv_buffer, index=False)
                    tar_files[f'{key}.csv'] = csv_buffer.getvalue().encode('utf-8')

                # to parquet (outside tarball)
                if save_parquet:
                    value.to_parquet(os.path.join(rollup_path, f'{filename}.parquet'), index=False)

            elif isinstance(value, pd.Series):
                # Convert Series to DataFrame to preserve index with proper column names
                df = value.reset_index()

                if save_csv:
                    # Save CSV to tarball instead of filesystem
                    csv_buffer = io.StringIO()
                    df.to_csv(csv_buffer, index=False)
                    tar_files[f'{key}.csv'] = csv_buffer.getvalue().encode('utf-8')

                if save_parquet:
                    df.to_parquet(os.path.join(rollup_path, f'{filename}.parquet'), index=False)

            elif isinstance(value, list):
                # Store JSON data in memory for tar
                tar_files[f'{filename}.json'] = json.dumps(value, indent=2).encode('utf-8')

            elif isinstance(value, dict):
                # Store JSON data in memory for tar
                tar_files[f'{filename}.json'] = json.dumps(value, indent=2).encode('utf-8')
            # the rest
            else:
                print(f'Key {key} is a unknown type')

        # Create tarball only if there are files to add
        if tar_files:
            tar_path = os.path.join(rollup_path, f'data_rollups_{year}_{month}_{day}.tar.gz')
            with tarfile.open(tar_path, 'w:gz') as tar:
                for filename, data in tar_files.items():
                    # Create TarInfo object
                    tarinfo = tarfile.TarInfo(name=f'./{filename}')
                    tarinfo.size = len(data)

                    # Add to tar from memory
                    tar.addfile(tarinfo, io.BytesIO(data))
