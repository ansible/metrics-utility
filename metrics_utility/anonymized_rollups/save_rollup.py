import json
import os

import pandas as pd


def save_rollup(rollup_data: dict, rollup_name: str, base_path, year: int, month: int, day: int) -> None:
    # rollup data is dictionary
    # the dictionary can have those values:
    # scalar, list, pandas.Series, pandas.DataFrame
    # each dictionary key will be stored as separate file, with file name as key
    # file will be dataframe or json for rest of the values

    # file will be stored inside base_path/rollups/rollup_name/year/month/day

    rollup_path = os.path.join(base_path, 'rollups', str(year), str(month), str(day), rollup_name)

    os.makedirs(rollup_path, exist_ok=True)

    print('--------------------------------')
    print(f'rollup_name: {rollup_name}')
    print('--------------------------------')

    for key, value in rollup_data.items():
        print(f'Saving {key} to {rollup_path}')

        filename = key

        if isinstance(value, pd.DataFrame):
            print(f'Key {key} is a DataFrame')

            # uncomment for testing purporses
            # value.to_csv(os.path.join(rollup_path, f'{key}.csv'), index=False)
            # to parquet

            value.to_parquet(os.path.join(rollup_path, f'{filename}.parquet'), index=False)
        elif isinstance(value, pd.Series):
            print(f'Key {key} is a Series')
            # Convert Series to DataFrame to preserve index with proper column names
            df = value.reset_index()

            # uncomment for testing purporses
            # df.to_csv(os.path.join(rollup_path, f'{key}.csv'), index=False)

            df.to_parquet(os.path.join(rollup_path, f'{filename}.parquet'), index=False)

        elif isinstance(value, list):
            print(f'Key {key} is a list')
            with open(os.path.join(rollup_path, f'{filename}.json'), 'w') as f:
                json.dump(value, f)

        elif isinstance(value, dict):
            print(f'Key {key} is a dict')
            with open(os.path.join(rollup_path, f'{filename}.json'), 'w') as f:
                json.dump(value, f)
        # the rest
        else:
            print('Key {key} is a unknown type')
