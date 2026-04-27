"""Base class for all anonymized rollup processors."""

import io
import json
import os
import tarfile

from datetime import datetime

import pandas as pd

from metrics_utility.anonymized_rollups.helpers import sanitize_json


class BaseAnonymizedRollup:
    """Base class for all anonymized rollup processors.

    Subclasses implement ``prepare``, ``merge``, and ``base`` to define how
    raw collector data is aggregated into a JSON structure suitable for
    inclusion in the anonymized daily report.
    """

    def __init__(self, rollup_name: str):
        """Initialize the rollup with a name identifier.

        Args:
            rollup_name: Unique name for this rollup (used as a directory key when saving).
        """
        self.rollup_name = rollup_name
        self.collector_names = []

    # Merges two data (it dont have to be dataframes if overriden)
    # this is used in batch processing
    # where we need to merge partial rollup with current batch aggregation
    def merge(self, dataframe_all, dataframe_new):
        """Combine accumulated rollup data with a new batch.

        The default implementation concatenates two DataFrames.  Subclasses may
        override this method for non-DataFrame data structures or custom merge logic.

        Args:
            dataframe_all: The accumulated data so far (may be None on the first call).
            dataframe_new: The freshly prepared batch to merge in.

        Returns:
            The merged data (a concatenated DataFrame by default).
        """
        if dataframe_all is None:
            return dataframe_new

        return pd.concat([dataframe_all, dataframe_new], ignore_index=True)

    def _convert_id_columns_to_strings(self, dataframe):
        """Convert ID columns to strings at the beginning of prepare().

        Converts numeric ID columns (id, job_id, host_id, job_remote_id) to strings
        to ensure consistent JSON serialization.
        """
        if dataframe.empty:
            return dataframe

        id_columns = ['id', 'job_id', 'host_id', 'job_remote_id']
        for col in id_columns:
            if col in dataframe.columns:
                # Convert numeric IDs to strings, preserving NaN values
                dataframe[col] = dataframe[col].apply(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) else x)

        return dataframe

    # takes raw data and computes aggregation
    # this works in batches, for example we are collecting every hour
    # this hourly data arrive into prepare, then it gets merged with partial rollup (initaly empty)
    def prepare(self, dataframe):
        """Transform a raw batch DataFrame into the intermediate rollup structure.

        Called once per hourly (or other interval) batch.  The result is later
        passed to ``merge`` to accumulate across batches.

        Args:
            dataframe: Raw pandas DataFrame from the collector.

        Returns:
            Transformed data (DataFrame or JSON-serialisable structure).
        """
        return dataframe

    # Base receive the full daily rollup and computes some final statistics for the day
    def base(self, _dataframe):
        """Compute final daily statistics from the fully-merged rollup data.

        Args:
            _dataframe: The accumulated rollup data produced by successive calls to
                ``merge`` across all batches for the day.

        Returns:
            An empty DataFrame by default; subclasses return a dict with a
            ``'json'`` key containing the final JSON payload.
        """
        return pd.DataFrame()

    def save_rollup(self, rollup_data: dict, base_path: str, since: datetime, until: datetime, packed: bool = True) -> None:
        """Persist rollup data to the filesystem, optionally as a tar.gz archive.

        Rollup data is written to
        ``<base_path>/rollups/<year>/<month>/<day>/<rollup_name>/``.
        DataFrames are stored as CSV files; scalars, lists and dicts as JSON.

        Args:
            rollup_data: Dict mapping key names to data (DataFrame, Series,
                list, dict, or scalar).
            base_path: Root directory under which the rollup tree is created.
            since: Start timestamp of the rollup window (determines date path).
            until: End timestamp of the rollup window (used in file names).
            packed: When True (default) all files are bundled into a .tar.gz
                archive; when False they are written individually to the directory.
        """
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

            elif isinstance(value, (list, dict)):
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
