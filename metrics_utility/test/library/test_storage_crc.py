import pytest

from metrics_utility.library.storage.crc import StorageCRC


def test_missing_client_id_raises_value_error():
    with pytest.raises(ValueError, match='client_id not set'):
        StorageCRC(client_secret='secret')


def test_missing_client_secret_raises_value_error():
    with pytest.raises(ValueError, match='client_secret not set'):
        StorageCRC(client_id='my-id')
