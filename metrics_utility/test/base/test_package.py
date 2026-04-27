from unittest.mock import MagicMock

from metrics_utility.base.package import Package


def test_get_max_data_size_classmethod():
    assert Package.get_max_data_size() == Package.MAX_DATA_SIZE
    assert Package.get_max_data_size() == 200 * 1048576


def test_has_free_space_within_limit():
    pkg = MagicMock(spec=Package)
    pkg.total_data_size = 100
    pkg.get_max_data_size.return_value = 200
    assert Package.has_free_space(pkg, 99) is True
    assert Package.has_free_space(pkg, 100) is True  # exactly at limit


def test_has_free_space_exceeds_limit():
    pkg = MagicMock(spec=Package)
    pkg.total_data_size = 100
    pkg.get_max_data_size.return_value = 200
    assert Package.has_free_space(pkg, 101) is False
