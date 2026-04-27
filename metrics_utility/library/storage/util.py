"""Utility helpers for the storage layer: temp-file JSON serialisation and date filtering."""

import datetime
import json
import re
import tempfile

from contextlib import contextmanager


# dict_to_json_file - create a temporary file with the input dict stringified to json


@contextmanager
def dict_to_json_file(data):
    """Write *data* to a temporary JSON file and yield its path.

    Args:
        data: JSON-serialisable dict or list.

    Yields:
        Absolute path to the temporary ``.json`` file (deleted on context exit).
    """
    with tempfile.NamedTemporaryFile(mode='x', encoding='utf-8', newline='\n', suffix='.json', delete_on_close=False) as file:
        json.dump(data, file)
        file.close()
        yield file.name


# date_filter - return True if filename contains a date between since (included) and until (excluded)
# only supports '%Y-%m-%d-%H%M%S%z'


def date_filter(filename, since=None, until=None):
    """Return True if *filename* contains a date timestamp within [since, until).

    Expects timestamps in the format ``%Y-%m-%d-%H%M%S%z`` embedded anywhere in
    the filename.

    Args:
        filename: File path or name string to parse.
        since: Optional inclusive lower bound datetime.
        until: Optional exclusive upper bound datetime.

    Returns:
        True if the embedded timestamp falls within the requested range (or the
        date falls within when either bound is None); False if no timestamp is found.
    """
    m = re.search(r'\b\d{4}-\d{2}-\d{2}-\d{2}\d{2}\d{2}([-+]\d+)?', filename)
    if not m:
        return False

    dt = datetime.datetime.strptime(m[0], '%Y-%m-%d-%H%M%S%z')
    if since and dt < since:
        return False
    if until and dt >= until:
        return False

    return True
