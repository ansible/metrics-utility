import datetime
import os
import tempfile

import pytest

from metrics_utility.library.storage import StorageDirectory


base_path = '/tmp/test_storage_directory'
target_filename = f'temporary object {os.getpid()}'
joined = os.path.join(base_path, target_filename)


def test_put_get_filename():
    storage = StorageDirectory(base_path=base_path)

    with tempfile.NamedTemporaryFile(mode='x', encoding='utf-8', newline='\n', suffix='.txt', delete_on_close=False) as file:
        file.write(f'Hello {target_filename}!')
        file.close()
        storage.put(target_filename, filename=file.name)

    with open(joined, mode='r', encoding='utf-8') as file:
        assert file.read() == f'Hello {target_filename}!'

    with storage.get(target_filename) as filename:
        with open(filename, mode='r', encoding='utf-8') as file:
            assert file.read() == f'Hello {target_filename}!'


def test_put_get_dict():
    storage = StorageDirectory(base_path=base_path)

    obj = {'foo': 5, 'bar': 'baz'}
    storage.put(target_filename, dict=obj)

    with open(joined, mode='r', encoding='utf-8') as file:
        assert file.read() == '{"foo": 5, "bar": "baz"}'

    with storage.get(target_filename) as filename:
        with open(filename, mode='r', encoding='utf-8') as file:
            assert file.read() == '{"foo": 5, "bar": "baz"}'


def test_base_path_slash():
    base_path_without = base_path
    base_path_with = base_path + '/'

    assert base_path_without[-1] != '/'
    assert base_path_with[-1] == '/'

    storage_without = StorageDirectory(base_path=base_path_without)
    storage_with = StorageDirectory(base_path=base_path_with)

    filename1 = f'temporary one {os.getpid()}'
    filename2 = f'temporary two {os.getpid()}'

    storage_without.put(filename1, dict={'foo': 123})
    storage_with.put(filename2, dict={'foo': 456})

    with storage_with.get(filename1) as fn1:
        with open(fn1, mode='r', encoding='utf-8') as file:
            assert file.read() == '{"foo": 123}'
    with storage_without.get(filename2) as fn2:
        with open(fn2, mode='r', encoding='utf-8') as file:
            assert file.read() == '{"foo": 456}'

    storage_with.remove(filename1)
    storage_with.remove(filename2)


def test_get_error():
    storage = StorageDirectory(base_path=base_path)

    with pytest.raises(Exception) as exc:
        with storage.get('not ' + target_filename) as _filename:
            assert False
    assert exc is not None


def test_exists():
    storage = StorageDirectory(base_path=base_path)
    assert storage.exists(target_filename) is True
    assert storage.exists('not ' + target_filename) is False


def test_glob():
    storage = StorageDirectory(base_path=base_path)
    pid = os.getpid()
    now = datetime.datetime.now(datetime.timezone.utc)

    assert storage.glob(target_filename) == [target_filename]
    assert storage.glob('not ' + target_filename) == []
    assert storage.glob(f'*{pid}*') == [target_filename]

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
    storage = StorageDirectory(base_path=base_path)
    storage.remove(target_filename)
    assert storage.exists(target_filename) is False
