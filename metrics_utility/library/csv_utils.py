"""CSV utilities for converting DataFrames to split CSV files."""

import os

from .csv_file_splitter import CsvFileSplitter


def dataframe_to_csv_files(df, table_name, output_dir, max_file_size=200 * 1048576):
    """
    Convert pandas DataFrame to split CSV files.

    Uses CsvFileSplitter to handle large datasets by creating multiple files
    when size exceeds max_file_size.

    Args:
        df: pandas DataFrame to write
        table_name: Base name for CSV files (e.g., 'main_executionenvironment')
        output_dir: Directory to write files to
        max_file_size: Maximum size per file in bytes (default: 200MB)

    Returns:
        List of file paths created. For small datasets returns one file,
        for large datasets returns multiple files with _split0, _split1 suffixes.
    """
    import pandas as pd

    file_path = os.path.join(output_dir, f'{table_name}_table.csv')

    if df is None:
        # just an empty file
        with open(file_path, 'w'):
            pass

        return [file_path]

    if isinstance(df, pd.DataFrame) and df.empty:
        # empty file with headers only
        df.to_csv(file_path, index=False)

        return [file_path]

    file = CsvFileSplitter(filespec=file_path, max_file_size=max_file_size)
    df.to_csv(file, index=False)

    return file.file_list(keep_empty=True)
