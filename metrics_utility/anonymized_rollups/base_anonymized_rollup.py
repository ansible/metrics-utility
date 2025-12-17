import io
import json
import os
import tarfile

from datetime import datetime

import pandas as pd

from metrics_utility.anonymized_rollups.helpers import sanitize_json


class BaseAnonymizedRollup:
    def __init__(self, rollup_name: str):
        self.rollup_name = rollup_name
        self.collector_names = []

    def merge(self, dataframe_all, dataframe_new):
        if dataframe_all is None:
            return dataframe_new

        return pd.concat([dataframe_all, dataframe_new], ignore_index=True)

    def rollup(self, dataframe_all, dataframe_new):
        # not implemented in base class, return empty dataframe
        return pd.DataFrame()

    def prepare(self, dataframe):
        return dataframe

    def base(self, dataframe):
        return pd.DataFrame()

    def save_rollup(self, rollup_data: dict, base_path: str, since: datetime, until: datetime, packed: bool = True) -> None:
        # rollup data is dictionary
        # the dictionary can have those values:
        # scalar, list, pandas.Series, pandas.DataFrame
        # each dictionary key will be stored as separate file, with file name as key
        # file will be dataframe or json for rest of the values

        # file will be stored inside base_path/rollups/rollup_name/year/month/day

        # make sure year is 4 digits, month is 2 digits, day is 2 digits

        year = since.year
        month = since.month
        day = since.day

        year = str(year).zfill(4)
        month = str(month).zfill(2)
        day = str(day).zfill(2)
        rollup_path = os.path.join(base_path, 'rollups', str(year), str(month), str(day), self.rollup_name)

        os.makedirs(rollup_path, exist_ok=True)

        # Collect files in memory for tar archive
        tar_files = {}

        for key, value in rollup_data.items():
            # filename is key + since + until, month and day are 2 digits
            filename = key + '_' + since.strftime('%Y-%m-%d') + '_' + until.strftime('%Y-%m-%d')

            if isinstance(value, pd.DataFrame):
                # Save CSV to tarball instead of filesystem
                csv_buffer = io.StringIO()
                value.to_csv(csv_buffer, index=False)
                tar_files[f'{key}.csv'] = csv_buffer.getvalue().encode('utf-8')

            elif isinstance(value, pd.Series):
                # Convert Series to DataFrame to preserve index with proper column names
                df = value.reset_index()

                # Save CSV to tarball instead of filesystem
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                tar_files[f'{key}.csv'] = csv_buffer.getvalue().encode('utf-8')

            elif isinstance(value, list):
                # Sanitize and store JSON data in memory for tar
                sanitized_value = sanitize_json(value)
                tar_files[f'{filename}.json'] = json.dumps(sanitized_value, indent=2).encode('utf-8')

            elif isinstance(value, dict):
                # Sanitize and store JSON data in memory for tar
                sanitized_value = sanitize_json(value)
                tar_files[f'{filename}.json'] = json.dumps(sanitized_value, indent=2).encode('utf-8')
            elif isinstance(value, (int, float, str, bool)) or value is None:
                # Handle scalar values (int, float, str, bool, None) by wrapping in a dict
                sanitized_value = sanitize_json({key: value})
                tar_files[f'{filename}.json'] = json.dumps(sanitized_value, indent=2).encode('utf-8')
            # the rest
            else:
                print(f'Key {key} is a unknown type')

        # Create tarball or save files directly based on packed parameter
        if tar_files:
            if packed:
                # Create tarball
                tar_path = os.path.join(rollup_path, f'data_rollups_{year}_{month}_{day}.tar.gz')
                with tarfile.open(tar_path, 'w:gz') as tar:
                    for filename, data in tar_files.items():
                        # Create TarInfo object
                        tarinfo = tarfile.TarInfo(name=f'./{filename}')
                        tarinfo.size = len(data)

                        # Add to tar from memory
                        tar.addfile(tarinfo, io.BytesIO(data))
            else:
                # Save files directly to filesystem (no tarball)
                for filename, data in tar_files.items():
                    file_path = os.path.join(rollup_path, filename)
                    with open(file_path, 'wb') as f:
                        f.write(data)
