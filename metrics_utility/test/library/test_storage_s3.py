import datetime
import os
import tempfile

import pytest

from metrics_utility.library.storage import StorageS3


# "localhost" (github actions, pytest WITH compose) or "minio" (pytest IN compose)
endpoint = os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', 'http://localhost:9000')
s3_settings = {
    'access_key': 'myuseraccesskey',
    'bucket': 'metricsutilitys3',
    'endpoint': endpoint,
    'region': 'us-east-1',
    'secret_key': 'myusersecretkey',
}
s3_object_name = f'temporary object x{os.getpid()}y'


def test_put_get_filename():
    storage = StorageS3(**s3_settings)

    with tempfile.NamedTemporaryFile(mode='x', encoding='utf-8', newline='\n', suffix='.txt', delete_on_close=False) as file:
        file.write(f'Hello {s3_object_name}!')
        file.close()
        storage.put(s3_object_name, filename=file.name)

    with storage.get(s3_object_name) as filename:
        with open(filename, mode='r', encoding='utf-8') as file:
            assert file.read() == f'Hello {s3_object_name}!'


def test_put_get_dict():
    storage = StorageS3(**s3_settings)

    obj = {'foo': 5, 'bar': 'baz'}
    storage.put(s3_object_name, dict=obj)

    with storage.get(s3_object_name) as filename:
        with open(filename, mode='r', encoding='utf-8') as file:
            assert file.read() == '{"foo": 5, "bar": "baz"}'


def test_get_error():
    storage = StorageS3(**s3_settings)

    with pytest.raises(Exception) as exc:
        with storage.get('not ' + s3_object_name) as _filename:
            assert False
    assert exc is not None


def test_exists():
    storage = StorageS3(**s3_settings)
    assert storage.exists(s3_object_name) is True
    assert storage.exists('not ' + s3_object_name) is False


def test_glob():
    storage = StorageS3(**s3_settings)
    pid = f'x{os.getpid()}y'
    now = datetime.datetime.now(datetime.timezone.utc)

    assert storage.glob(s3_object_name) == [s3_object_name]
    assert storage.glob('not ' + s3_object_name) == []
    assert storage.glob(f'*{pid}*') == [s3_object_name]

    assert storage.glob(f'*{pid}*', since=now) == []
    assert storage.glob(f'*{pid}*', until=now) == []

    filename = f'foo-{pid}-{now.strftime("%Y-%m-%d-%H%M%S%z")}-bar'
    storage.put(filename, filename='/dev/null')

    before = now - datetime.timedelta(minutes=5)
    after = now + datetime.timedelta(minutes=5)

    assert storage.glob(f'*{pid}*', since=before, until=after) == [filename]
    assert storage.glob(f'*{pid}*', since=before) == [filename]
    assert storage.glob(f'*{pid}*', until=after) == [filename]
    assert storage.glob(f'*{pid}*', since=after) == []
    assert storage.glob(f'*{pid}*', until=before) == []


def test_remove():
    storage = StorageS3(**s3_settings)
    storage.remove(s3_object_name)
    assert storage.exists(s3_object_name) is False
