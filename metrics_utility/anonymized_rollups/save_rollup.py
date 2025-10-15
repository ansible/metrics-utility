import io
import json
import os
import tarfile

import pandas as pd


def save_rollup(rollup_data: dict, rollup_name: str, base_path, year: int, month: int, day: int, save_csv: bool = False) -> None:
    # rollup data is dictionary
    # the dictionary can have those values:
    # scalar, list, pandas.Series, pandas.DataFrame
    # each dictionary key will be stored as separate file, with file name as key
    # file will be dataframe or json for rest of the values

    # file will be stored inside base_path/rollups/rollup_name/year/month/day

    # save_csv is for testing purposes only - so we can check content of files easily

    rollup_path = os.path.join(base_path, 'rollups', str(year), str(month), str(day), rollup_name)

    os.makedirs(rollup_path, exist_ok=True)

    print('--------------------------------')
    print(f'rollup_name: {rollup_name}')
    print('--------------------------------')

    # Collect JSON data in memory for tar archive
    json_files = {}

    for key, value in rollup_data.items():
        print(f'Saving {key} to {rollup_path}')

        filename = key + '_' + str(year) + '_' + str(month) + '_' + str(day)

        if isinstance(value, pd.DataFrame):
            print(f'Key {key} is a DataFrame')

            if save_csv:
                value.to_csv(os.path.join(rollup_path, f'{key}.csv'), index=False)

            # to parquet
            value.to_parquet(os.path.join(rollup_path, f'{filename}.parquet'), index=False)
        elif isinstance(value, pd.Series):
            print(f'Key {key} is a Series')
            # Convert Series to DataFrame to preserve index with proper column names
            df = value.reset_index()

            if save_csv:
                df.to_csv(os.path.join(rollup_path, f'{key}.csv'), index=False)

            df.to_parquet(os.path.join(rollup_path, f'{filename}.parquet'), index=False)

        elif isinstance(value, list):
            print(f'Key {key} is a list')
            # Store JSON data in memory for tar
            json_files[f'{filename}.json'] = value

        elif isinstance(value, dict):
            print(f'Key {key} is a dict')
            # Store JSON data in memory for tar
            json_files[f'{filename}.json'] = value
        # the rest
        else:
            print(f'Key {key} is a unknown type')

    # Create tarball from in-memory JSON data (excluding parquet files)
    # tarball name is data_rollups_<year>_<month>_<day>.tar.gz
    tar_path = os.path.join(rollup_path, f'data_rollups_{year}_{month}_{day}.tar.gz')
    with tarfile.open(tar_path, 'w:gz') as tar:
        for filename, data in json_files.items():
            # Convert data to JSON bytes
            json_bytes = json.dumps(data, indent=2).encode('utf-8')

            # Create TarInfo object
            tarinfo = tarfile.TarInfo(name=f'./{filename}')
            tarinfo.size = len(json_bytes)

            # Add to tar from memory
            tar.addfile(tarinfo, io.BytesIO(json_bytes))
