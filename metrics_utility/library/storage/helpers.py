import csv
import json

import pandas as pd


# Load functions - accept either a filename (str/Path) or a file-like object


def load_csv(source):
    """
    Load a CSV file and return a list of dictionaries.

    Args:
        source: Either a filename (str/Path) or a file-like object

    Returns:
        List of dictionaries, one per row
    """
    # pandas read_csv handles both filenames and file-like objects
    df = pd.read_csv(source, encoding='utf-8')
    return df.to_dict('records')


def load_json(source):
    """
    Load a JSON file and return the parsed data (list or dict).

    Args:
        source: Either a filename (str/Path) or a file-like object

    Returns:
        Parsed JSON data (list or dict)
    """
    if isinstance(source, str):
        with open(source, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        # source is a file-like object
        return json.load(source)


def load_parquet(source):
    """
    Load a Parquet file and return a pandas DataFrame.

    Args:
        source: Either a filename (str/Path) or a file-like object

    Returns:
        pandas DataFrame
    """
    return pd.read_parquet(source)


# Save functions - follow storage put convention with filename= and fileobj= parameters


def save_csv(data, *, filename=None, fileobj=None):
    """
    Save data as CSV to a file.

    Args:
        data: Either a list of dictionaries or a pandas DataFrame
        filename: Path to save the file (mutually exclusive with fileobj)
        fileobj: File-like object to write to (mutually exclusive with filename)

    Note:
        Exactly one of filename or fileobj must be provided.
    """
    if (filename is None) == (fileobj is None):
        raise ValueError('Exactly one of filename or fileobj must be provided')

    if isinstance(data, pd.DataFrame):
        # Use pandas to_csv for DataFrames
        if filename:
            data.to_csv(filename, index=False, encoding='utf-8')
        else:
            data.to_csv(fileobj, index=False, encoding='utf-8')
    elif isinstance(data, list):
        # Use csv.DictWriter for list of dicts
        if not data:
            # Handle empty list
            if filename:
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    pass
            return

        fieldnames = list(data[0].keys())

        if filename:
            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
        else:
            writer = csv.DictWriter(fileobj, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    else:
        raise TypeError(f'data must be a DataFrame or list of dicts, got {type(data).__name__}')


def save_json(data, *, filename=None, fileobj=None):
    """
    Save data as JSON to a file.

    Args:
        data: A list or dictionary to save
        filename: Path to save the file (mutually exclusive with fileobj)
        fileobj: File-like object to write to (mutually exclusive with filename)

    Note:
        Exactly one of filename or fileobj must be provided.
    """
    if (filename is None) == (fileobj is None):
        raise ValueError('Exactly one of filename or fileobj must be provided')

    if filename:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    else:
        json.dump(data, fileobj)


def save_parquet(df, *, filename=None, fileobj=None):
    """
    Save a DataFrame as Parquet to a file.

    Args:
        df: pandas DataFrame to save
        filename: Path to save the file (mutually exclusive with fileobj)
        fileobj: File-like object to write to (mutually exclusive with filename)

    Note:
        Exactly one of filename or fileobj must be provided.
    """
    if (filename is None) == (fileobj is None):
        raise ValueError('Exactly one of filename or fileobj must be provided')

    if filename:
        df.to_parquet(filename)
    else:
        df.to_parquet(fileobj)
