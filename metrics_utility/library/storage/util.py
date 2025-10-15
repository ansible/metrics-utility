import datetime
import json
import re
import tempfile

from contextlib import contextmanager


# dict_to_json_file - create a temporary file with the input dict stringified to json
# Example:

# with dict_to_json_file({'foo': 'bar'}) as filename:
#     storage.put('collected.json', filename=filename)


@contextmanager
def dict_to_json_file(data):
    with tempfile.NamedTemporaryFile(mode='x', encoding='utf-8', newline='\n', suffix='.json', delete_on_close=False) as file:
        json.dump(data, file)
        file.close()
        yield file.name


# date_filter - return True if filename contains a date between since (included) and until (excluded)
# only supports '%Y-%m-%d-%H%M%S%z'


def date_filter(filename, since=None, until=None):
    m = re.search(r'\b\d{4}-\d{2}-\d{2}-\d{2}\d{2}\d{2}([-+]\d+)?', filename)
    if not m:
        return False

    dt = datetime.datetime.strptime(m[0], '%Y-%m-%d-%H%M%S%z')
    if since and dt < since:
        return False
    if until and dt >= until:
        return False

    return True
