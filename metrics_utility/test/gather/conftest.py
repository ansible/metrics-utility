import pytest


@pytest.fixture
def ship_path(tmp_path):
    return str(tmp_path)
