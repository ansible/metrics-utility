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

    rollup_path = os.path.join(base_path, 'rollups', rollup_name, str(year), str(month), str(day))

    os.makedirs(rollup_path, exist_ok=True)

    for key, value in rollup_data.items():
        if isinstance(value, pd.DataFrame):
            value.to_csv(os.path.join(rollup_path, f'{key}.csv'), index=False)
        elif isinstance(value, pd.Series):
            value.to_csv(os.path.join(rollup_path, f'{key}.csv'), index=False)
        elif isinstance(value, list):
            with open(os.path.join(rollup_path, f'{key}.json'), 'w') as f:
                json.dump(value, f)
